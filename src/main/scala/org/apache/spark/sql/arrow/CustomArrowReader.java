package org.apache.spark.sql.arrow;

import sun.misc.Unsafe;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;


/**
 * Pure custom Arrow IPC file reader using only mmap + Unsafe.
 * Enhanced for multiple record batches with streaming/lazy iteration.
 * No Apache Arrow dependencies.
 */
public class CustomArrowReader implements AutoCloseable, Iterable<CustomArrowReader.RecordBatch> {

    // ─── Unsafe Setup ──────────────────────────────────────────────────────

    private static final Unsafe UNSAFE;
    static {
        try {
            java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Unsafe", e);
        }
    }


    // ─── Arrow Type Constants ──────────────────────────────────────────────

    private static final byte TYPE_NONE = 0;
    private static final byte TYPE_NULL = 1;
    private static final byte TYPE_INT = 2;
    private static final byte TYPE_FLOATINGPOINT = 3;
    private static final byte TYPE_BINARY = 4;
    private static final byte TYPE_UTF8 = 5;
    private static final byte TYPE_BOOL = 6;
    private static final byte TYPE_LIST = 7;
    private static final byte TYPE_STRUCT = 8;
    private static final byte TYPE_UNION = 9;
    private static final byte TYPE_FIXED_SIZE_BINARY = 10;
    private static final byte TYPE_FIXED_SIZE_LIST = 11;
    private static final byte TYPE_MAP = 12;
    private static final byte TYPE_DURATION = 13;
    private static final byte TYPE_LARGE_BINARY = 14;
    private static final byte TYPE_LARGE_UTF8 = 15;
    private static final byte TYPE_LARGE_LIST = 16;

    // ─── File State ────────────────────────────────────────────────────────

    private final long mmapBase;      // Base address of mmap'd file
    private final long fileSize;      // Total file size
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MappedByteBuffer mappedBuffer;

    // Parsed metadata
    private Schema schema;
    private List<Block> recordBlocks;
    private int currentBatchIndex = 0;

    // ─── Constructor ───────────────────────────────────────────────────────

    public CustomArrowReader(String filePath) throws Exception {
        this.raf = new RandomAccessFile(filePath, "r");
        this.channel = raf.getChannel();
        this.fileSize = raf.length();

        // Memory-map the entire file
        this.mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        this.mmapBase = ((DirectBuffer) mappedBuffer).address();

        // Verify magic at start
        if (!checkMagic(mmapBase)) {
            throw new IllegalArgumentException("Not an Arrow IPC file (bad start magic)");
        }

        // Verify magic at end
        if (!checkMagic(mmapBase + fileSize - 6)) {
            throw new IllegalArgumentException("Not an Arrow IPC file (bad end magic)");
        }

        // Parse footer to get schema and batch locations
        parseFooter();
    }

    private boolean checkMagic(long addr) {
        return UNSAFE.getByte(addr) == 'A'
                && UNSAFE.getByte(addr + 1) == 'R'
                && UNSAFE.getByte(addr + 2) == 'R'
                && UNSAFE.getByte(addr + 3) == 'O'
                && UNSAFE.getByte(addr + 4) == 'W'
                && UNSAFE.getByte(addr + 5) == '1';
    }

    // ─── FlatBuffer Reading Primitives ─────────────────────────────────────

    private byte getByte(long addr) { return UNSAFE.getByte(addr); }
    private short getShort(long addr) { return UNSAFE.getShort(addr); }
    private int getInt(long addr) { return UNSAFE.getInt(addr); }
    private long getLong(long addr) { return UNSAFE.getLong(addr); }
    private float getFloat(long addr) { return UNSAFE.getFloat(addr); }
    private double getDouble(long addr) { return UNSAFE.getDouble(addr); }
    private int getUByte(long addr) { return UNSAFE.getByte(addr) & 0xFF; }
    private int getUShort(long addr) { return UNSAFE.getShort(addr) & 0xFFFF; }
    private long getUInt(long addr) { return UNSAFE.getInt(addr) & 0xFFFFFFFFL; }

    /**
     * FlatBuffer soffset_t: signed offset from table start to vtable.
     */
    private int getVTableOffset(long tableStart) {
        return getInt(tableStart);
    }

    private long getVTableAddress(long tableStart) {
        return tableStart - getVTableOffset(tableStart);
    }

    /**
     * Read field offset from vtable.
     * VTable: [vtable_size, table_size, field0_offset, field1_offset, ...]
     */
    private int getFieldOffset(long tableStart, int fieldIndex) {
        long vtable = getVTableAddress(tableStart);
        int vtableSize = getUShort(vtable);
        if (fieldIndex + 2 >= vtableSize / 2) return 0;
        return getUShort(vtable + 4 + fieldIndex * 2);
    }

    private long getTableFieldAddress(long tableStart, int fieldIndex) {
        int offset = getFieldOffset(tableStart, fieldIndex);
        if (offset == 0) return 0;
        return tableStart + offset;
    }

    private long getStructFieldAddress(long tableStart, int fieldIndex) {
        int offset = getFieldOffset(tableStart, fieldIndex);
        if (offset == 0) return 0;
        return tableStart + offset;
    }

    /**
     * Read a string from FlatBuffer.
     * String: [length: int32, bytes...]
     */
    private String getString(long stringOffsetAddr) {
        int offset = getInt(stringOffsetAddr);
        if (offset == 0) return null;
        long stringAddr = stringOffsetAddr + offset;
        int length = getInt(stringAddr);
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = getByte(stringAddr + 4 + i);
        }
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Get vector (array) address and count.
     * Vector: [count: int32, elem0, elem1, ...]
     * Returns [address_of_first_element, count]
     */
    private long[] getVector(long vectorOffsetAddr) {
        int offset = getInt(vectorOffsetAddr);
        if (offset == 0) return null;
        long vectorAddr = vectorOffsetAddr + offset;
        int count = getInt(vectorAddr);
        return new long[]{vectorAddr + 4, count};
    }

    // ─── Footer Parsing ────────────────────────────────────────────────────

    private void parseFooter() {
        long footerSizeAddr = mmapBase + fileSize - 10;
        int footerSize = getInt(footerSizeAddr);
        long footerAddr = mmapBase + fileSize - 10 - footerSize;

        System.out.println("[CustomArrowReader] Footer size: " + footerSize);
        System.out.println("[CustomArrowReader] Footer offset: " + (footerAddr - mmapBase));

        // Footer FlatBuffer schema:
        // table Footer {
        //   version: MetadataVersion;      // field 0
        //   schema: Schema;                // field 1
        //   dictionaries: [Block];         // field 2
        //   recordBatches: [Block];        // field 3
        // }

        // Field 0: version
        long versionAddr = getStructFieldAddress(footerAddr, 0);
        if (versionAddr != 0) {
            System.out.println("[CustomArrowReader] Arrow version: " + getShort(versionAddr));
        }

        // Field 1: schema
        long schemaOffsetAddr = getTableFieldAddress(footerAddr, 1);
        if (schemaOffsetAddr != 0) {
            int schemaOffset = getInt(schemaOffsetAddr);
            long schemaAddr = schemaOffsetAddr + schemaOffset;
            this.schema = parseSchema(schemaAddr);
        }

        // Field 2: dictionaries (skip for now)

        // Field 3: recordBatches
        long batchesOffsetAddr = getTableFieldAddress(footerAddr, 3);
        if (batchesOffsetAddr != 0) {
            long[] batchVector = getVector(batchesOffsetAddr);
            if (batchVector != null) {
                long batchesAddr = batchVector[0];
                int batchCount = (int) batchVector[1];
                this.recordBlocks = new ArrayList<>(batchCount);

                System.out.println("[CustomArrowReader] Found " + batchCount + " record batches");

                for (int i = 0; i < batchCount; i++) {
                    // Block struct: {offset: int64, metaDataLength: int32, bodyLength: int64}
                    long blockAddr = batchesAddr + i * 24;
                    long offset = getLong(blockAddr);
                    int metaDataLength = getInt(blockAddr + 8);
                    long bodyLength = getLong(blockAddr + 16);
                    recordBlocks.add(new Block(offset, metaDataLength, bodyLength));
                    System.out.println("  Batch " + i + ": offset=" + offset +
                            ", metaLen=" + metaDataLength + ", bodyLen=" + bodyLength);
                }
            }
        }
    }

    // ─── Schema Parsing ────────────────────────────────────────────────────

    private Schema parseSchema(long schemaAddr) {
        Schema schema = new Schema();

        // Schema FlatBuffer:
        // table Schema {
        //   fields: [Field];           // field 0
        //   custom_metadata: [KeyValue]; // field 1
        // }

        long fieldsOffsetAddr = getTableFieldAddress(schemaAddr, 0);
        if (fieldsOffsetAddr != 0) {
            long[] fieldsVector = getVector(fieldsOffsetAddr);
            if (fieldsVector != null) {
                long fieldsAddr = fieldsVector[0];
                int fieldCount = (int) fieldsVector[1];

                for (int i = 0; i < fieldCount; i++) {
                    int fieldOffset = getInt(fieldsAddr + i * 4);
                    long fieldAddr = fieldsAddr + i * 4 + fieldOffset;
                    Field field = parseField(fieldAddr);
                    schema.fields.add(field);
                }
            }
        }

        return schema;
    }

    private Field parseField(long fieldAddr) {
        // Field FlatBuffer:
        // table Field {
        //   name: string;              // field 0
        //   nullable: bool;            // field 1
        //   type_type: Type;           // field 2
        //   type: Table;               // field 3
        //   children: [Field];         // field 4
        // }

        Field field = new Field();

        long nameOffsetAddr = getTableFieldAddress(fieldAddr, 0);
        if (nameOffsetAddr != 0) {
            field.name = getString(nameOffsetAddr);
        }

        long nullableAddr = getStructFieldAddress(fieldAddr, 1);
        if (nullableAddr != 0) {
            field.nullable = getByte(nullableAddr) != 0;
        }

        long typeTypeAddr = getStructFieldAddress(fieldAddr, 2);
        if (typeTypeAddr != 0) {
            field.typeType = getByte(typeTypeAddr);
        }

        long typeOffsetAddr = getTableFieldAddress(fieldAddr, 3);
        if (typeOffsetAddr != 0) {
            int typeOffset = getInt(typeOffsetAddr);
            long typeAddr = typeOffsetAddr + typeOffset;
            field.typeInfo = parseType(field.typeType, typeAddr);
        }

        return field;
    }

    private TypeInfo parseType(byte typeType, long typeAddr) {
        TypeInfo info = new TypeInfo();
        info.typeCode = typeType;

        switch (typeType) {
            case TYPE_INT:
                // Int: {bitWidth: int32, is_signed: bool}
                info.bitWidth = getInt(getStructFieldAddress(typeAddr, 0));
                info.isSigned = getByte(getStructFieldAddress(typeAddr, 1)) != 0;
                break;

            case TYPE_FLOATINGPOINT:
                // FloatingPoint: {precision: Precision}
                byte precision = getByte(getStructFieldAddress(typeAddr, 0));
                info.bitWidth = (precision == 0) ? 16 : (precision == 1) ? 32 : 64;
                break;

            case TYPE_BOOL:
                info.bitWidth = 1;
                break;

            case TYPE_UTF8:
            case TYPE_BINARY:
                info.bitWidth = -1;
                break;
        }

        return info;
    }

    // ─── Record Batch Reading ──────────────────────────────────────────────

    /**
     * Read a specific record batch by index.
     * This parses the batch header and creates ColumnData objects pointing into mmap'd memory.
     */
    public RecordBatch readBatch(int batchIndex) {
        if (batchIndex < 0 || batchIndex >= recordBlocks.size()) {
            throw new IndexOutOfBoundsException(
                    "Batch index " + batchIndex + " out of range [0, " + recordBlocks.size() + ")");
        }

        Block block = recordBlocks.get(batchIndex);

        // Message starts at block.offset (relative to file start)
        long messageAddr = mmapBase + block.offset;

        // Message FlatBuffer:
        // table Message {
        //   version: MetadataVersion;  // field 0
        //   bodyLength: int64;         // field 2
        //   header_type: MessageHeader; // field 3
        //   header: Table;             // field 4
        // }

        // Field 4: header (union, offset to RecordBatch table)
        long headerOffsetAddr = getTableFieldAddress(messageAddr, 4);
        int headerOffset = getInt(headerOffsetAddr);
        long recordBatchAddr = headerOffsetAddr + headerOffset;

        // RecordBatch FlatBuffer:
        // table RecordBatch {
        //   length: int64;             // field 0
        //   nodes: [FieldNode];        // field 1
        //   buffers: [Buffer];         // field 2
        // }

        long lengthAddr = getStructFieldAddress(recordBatchAddr, 0);
        long numRows = getLong(lengthAddr);

        // Parse nodes (FieldNode per column)
        long nodesOffsetAddr = getTableFieldAddress(recordBatchAddr, 1);
        long[] nodesVector = getVector(nodesOffsetAddr);
        long nodesAddr = nodesVector[0];
        int nodeCount = (int) nodesVector[1];

        FieldNode[] nodes = new FieldNode[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            long nodeAddr = nodesAddr + i * 16;
            nodes[i] = new FieldNode(getLong(nodeAddr), getLong(nodeAddr + 8));
        }

        // Parse buffers
        long buffersOffsetAddr = getTableFieldAddress(recordBatchAddr, 2);
        long[] buffersVector = getVector(buffersOffsetAddr);
        long buffersAddr = buffersVector[0];
        int bufferCount = (int) buffersVector[1];

        Buffer[] buffers = new Buffer[bufferCount];
        for (int i = 0; i < bufferCount; i++) {
            long bufAddr = buffersAddr + i * 16;
            buffers[i] = new Buffer(getLong(bufAddr), getLong(bufAddr + 8));
        }

        // Body starts after the message header
        long bodyAddr = mmapBase + block.offset + block.metaDataLength;

        // Create columns
        List<ColumnData> columns = new ArrayList<>();
        int bufferIdx = 0;

        for (int i = 0; i < schema.fields.size(); i++) {
            Field field = schema.fields.get(i);
            FieldNode node = nodes[i];

            ColumnData col = new ColumnData();
            col.name = field.name;
            col.type = field.typeType;
            col.length = node.length;
            col.nullCount = node.nullCount;
            col.nullable = field.nullable;

            int numBuffers = getBufferCount(field.typeType);

            // Validity bitmap (first buffer, if nullable)
            if (numBuffers >= 1 && bufferIdx < bufferCount) {
                Buffer validityBuf = buffers[bufferIdx++];
                col.validityAddr = bodyAddr + validityBuf.offset;
                col.validityLength = validityBuf.length;
            }

            // Type-specific data buffers
            switch (field.typeType) {
                case TYPE_INT:
                case TYPE_FLOATINGPOINT:
                    if (bufferIdx < bufferCount) {
                        Buffer dataBuf = buffers[bufferIdx++];
                        col.dataAddr = bodyAddr + dataBuf.offset;
                        col.dataLength = dataBuf.length;
                        col.elementSize = field.typeInfo.bitWidth / 8;
                    }
                    break;

                case TYPE_BOOL:
                    if (bufferIdx < bufferCount) {
                        Buffer dataBuf = buffers[bufferIdx++];
                        col.dataAddr = bodyAddr + dataBuf.offset;
                        col.dataLength = dataBuf.length;
                        col.elementSize = 1;
                    }
                    break;

                case TYPE_UTF8:
                case TYPE_BINARY:
                    if (bufferIdx < bufferCount) {
                        Buffer offsetsBuf = buffers[bufferIdx++];
                        col.offsetsAddr = bodyAddr + offsetsBuf.offset;
                        col.offsetsLength = offsetsBuf.length;
                    }
                    if (bufferIdx < bufferCount) {
                        Buffer dataBuf = buffers[bufferIdx++];
                        col.dataAddr = bodyAddr + dataBuf.offset;
                        col.dataLength = dataBuf.length;
                    }
                    col.elementSize = -1;
                    break;
            }

            columns.add(col);
        }

        return new RecordBatch(batchIndex, numRows, columns, block);
    }

    private int getBufferCount(byte typeType) {
        switch (typeType) {
            case TYPE_INT:
            case TYPE_FLOATINGPOINT:
            case TYPE_BOOL:
                return 2; // validity + data
            case TYPE_UTF8:
            case TYPE_BINARY:
                return 3; // validity + offsets + data
            default:
                return 2;
        }
    }

    // ─── Streaming / Iterator Support ──────────────────────────────────────

    /**
     * Reset batch iterator to start from first batch.
     */
    public void reset() {
        currentBatchIndex = 0;
    }

    /**
     * Check if there are more batches to read.
     */
    public boolean hasNext() {
        return currentBatchIndex < recordBlocks.size();
    }

    /**
     * Read next batch in sequence (for streaming).
     */
    public RecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more batches");
        }
        return readBatch(currentBatchIndex++);
    }

    /**
     * Get total number of batches.
     */
    public int getBatchCount() {
        return recordBlocks.size();
    }

    /**
     * Get schema.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Iterate over all batches (for-each support).
     */
    @Override
    public Iterator<RecordBatch> iterator() {
        return new Iterator<RecordBatch>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < recordBlocks.size();
            }

            @Override
            public RecordBatch next() {
                return readBatch(index++);
            }
        };
    }

    // ─── Data Access Methods ───────────────────────────────────────────────

    public long getInt64(ColumnData col, int rowIndex) {
        if (col.type != TYPE_INT || col.elementSize != 8) {
            throw new IllegalArgumentException("Not an int64 column");
        }
        return UNSAFE.getLong(col.dataAddr + rowIndex * 8);
    }

    public int getInt32(ColumnData col, int rowIndex) {
        if (col.type != TYPE_INT || col.elementSize != 4) {
            throw new IllegalArgumentException("Not an int32 column");
        }
        return UNSAFE.getInt(col.dataAddr + rowIndex * 4);
    }

    public double getDouble(ColumnData col, int rowIndex) {
        if (col.type != TYPE_FLOATINGPOINT || col.elementSize != 8) {
            throw new IllegalArgumentException("Not a float64 column");
        }
        return UNSAFE.getDouble(col.dataAddr + rowIndex * 8);
    }

    public float getFloat(ColumnData col, int rowIndex) {
        if (col.type != TYPE_FLOATINGPOINT || col.elementSize != 4) {
            throw new IllegalArgumentException("Not a float32 column");
        }
        return UNSAFE.getFloat(col.dataAddr + rowIndex * 4);
    }

    public boolean getBool(ColumnData col, int rowIndex) {
        if (col.type != TYPE_BOOL) {
            throw new IllegalArgumentException("Not a bool column");
        }
        long byteOffset = rowIndex / 8;
        int bitOffset = rowIndex % 8;
        byte b = UNSAFE.getByte(col.dataAddr + byteOffset);
        return ((b >> bitOffset) & 1) != 0;
    }

    public byte[] getString(ColumnData col, int rowIndex) {
        if (col.type != TYPE_UTF8) {
            throw new IllegalArgumentException("Not a utf8 column");
        }
        int startOffset = UNSAFE.getInt(col.offsetsAddr + rowIndex * 4);
        int endOffset = UNSAFE.getInt(col.offsetsAddr + (rowIndex + 1) * 4);
        int length = endOffset - startOffset;

        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = UNSAFE.getByte(col.dataAddr + startOffset + i);
        }
        return result;
    }

    public boolean isNull(ColumnData col, int rowIndex) {
        if (col.nullCount == 0 || !col.nullable) return false;
        long byteOffset = rowIndex / 8;
        int bitOffset = rowIndex % 8;
        byte b = UNSAFE.getByte(col.validityAddr + byteOffset);
        return ((b >> bitOffset) & 1) == 0;
    }

    // ─── Close ─────────────────────────────────────────────────────────────

    @Override
    public void close() throws Exception {
        // Unmap is not directly exposed, but we can close the channel
        // The OS will clean up the mmap when the process exits
        channel.close();
        raf.close();
    }

    // ─── Data Classes ──────────────────────────────────────────────────────

    public static class Schema {
        public final List<Field> fields = new ArrayList<>();
    }

    public static class Field {
        public String name;
        public boolean nullable = true;
        public byte typeType;
        public TypeInfo typeInfo;

        @Override
        public String toString() {
            String typeStr;
            switch (typeType) {
                case TYPE_INT: typeStr = "int" + typeInfo.bitWidth; break;
                case TYPE_FLOATINGPOINT: typeStr = "float" + typeInfo.bitWidth; break;
                case TYPE_BOOL: typeStr = "bool"; break;
                case TYPE_UTF8: typeStr = "utf8"; break;
                case TYPE_BINARY: typeStr = "binary"; break;
                default: typeStr = "type(" + typeType + ")";
            }
            return name + ": " + typeStr + (nullable ? " (nullable)" : "");
        }
    }

    public static class TypeInfo {
        public byte typeCode;
        public int bitWidth;
        public boolean isSigned;
    }

    public static class Block {
        public final long offset;
        public final int metaDataLength;
        public final long bodyLength;

        public Block(long offset, int metaDataLength, long bodyLength) {
            this.offset = offset;
            this.metaDataLength = metaDataLength;
            this.bodyLength = bodyLength;
        }
    }

    public static class FieldNode {
        public final long length;
        public final long nullCount;

        public FieldNode(long length, long nullCount) {
            this.length = length;
            this.nullCount = nullCount;
        }
    }

    public static class Buffer {
        public final long offset;
        public final long length;

        public Buffer(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }
    }

    /**
     * Represents a complete record batch with all columns.
     */
    public static class RecordBatch {
        public final int index;
        public final long numRows;
        public final List<ColumnData> columns;
        public final Block block;

        public RecordBatch(int index, long numRows, List<ColumnData> columns, Block block) {
            this.index = index;
            this.numRows = numRows;
            this.columns = columns;
            this.block = block;
        }

        public ColumnData getColumn(String name) {
            for (ColumnData col : columns) {
                if (col.name.equals(name)) return col;
            }
            throw new IllegalArgumentException("Column not found: " + name);
        }

        public ColumnData getColumn(int index) {
            return columns.get(index);
        }
    }

    /**
     * Column data pointing into mmap'd memory.
     */
    public static class ColumnData {
        public String name;
        public byte type;
        public long length;
        public long nullCount;
        public boolean nullable;

        // Buffer addresses (native pointers into mmap'd file)
        public long validityAddr;
        public long validityLength;

        public long dataAddr;
        public long dataLength;

        public long offsetsAddr;
        public long offsetsLength;

        public int elementSize;
    }

    // ─── Main: Test with Multiple Batches ──────────────────────────────────

    public static void main(String[] args) throws Exception {
        String filePath = args.length > 0 ? args[0] : "data_multi_batch.arrow";

        try (CustomArrowReader reader = new CustomArrowReader(filePath)) {

            System.out.println("\n=== Schema ===");
            for (Field f : reader.getSchema().fields) {
                System.out.println("  " + f);
            }

            System.out.println("\n=== Total Batches: " + reader.getBatchCount() + " ===");

            // ─── Method 1: Random Access by Index ──────────────────────────

            System.out.println("\n--- Method 1: Random Access ---");
            RecordBatch batch2 = reader.readBatch(2);  // Read batch 2 directly
            System.out.println("Batch 2 rows: " + batch2.numRows);

            ColumnData idCol = batch2.getColumn("id");
            for (int i = 0; i < (int)batch2.numRows; i++) {
                System.out.println("  id[" + i + "] = " + reader.getInt64(idCol, i));
            }

            // ─── Method 2: Sequential Streaming ────────────────────────────

            System.out.println("\n--- Method 2: Sequential Streaming ---");
            reader.reset();
            while (reader.hasNext()) {
                RecordBatch batch = reader.next();
                System.out.println("\nBatch " + batch.index + " (" + batch.numRows + " rows):");

                ColumnData nameCol = batch.getColumn("name");
                ColumnData ageCol = batch.getColumn("age");
                ColumnData salaryCol = batch.getColumn("salary");
                ColumnData activeCol = batch.getColumn("active");

                for (int i = 0; i < (int)batch.numRows; i++) {
                    String name = new String(reader.getString(nameCol, i));
                    long age = reader.getInt64(ageCol, i);
                    double salary = reader.getDouble(salaryCol, i);
                    boolean active = reader.getBool(activeCol, i);

                    System.out.printf("  Row %d: %s, age=%d, salary=%.1f, active=%b%n",
                            i, name, age, salary, active);
                }
            }

            // ─── Method 3: For-Each Iterator ───────────────────────────────

            System.out.println("\n--- Method 3: For-Each Iterator ---");
            for (RecordBatch batch : reader) {
                System.out.println("Processing batch " + batch.index +
                        " with " + batch.numRows + " rows");
                // Process batch...
            }

            // ─── Method 4: Out-of-Core Aggregation ─────────────────────────

            System.out.println("\n--- Method 4: Out-of-Core Aggregation ---");
            double totalSalary = 0;
            long totalRows = 0;

            for (RecordBatch batch : reader) {
                ColumnData salaryCol2 = batch.getColumn("salary");
                for (int i = 0; i < (int)batch.numRows; i++) {
                    totalSalary += reader.getDouble(salaryCol2, i);
                }
                totalRows += batch.numRows;
            }

            System.out.println("Total rows processed: " + totalRows);
            System.out.println("Average salary: " + (totalSalary / totalRows));
        }
    }
}
