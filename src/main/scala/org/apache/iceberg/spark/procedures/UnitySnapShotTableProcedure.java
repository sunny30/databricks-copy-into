package org.apache.iceberg.spark.procedures;


import avro.shaded.com.google.common.base.Preconditions;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.actions.UnitySparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;


import java.util.HashMap;
import java.util.Map;

public class UnitySnapShotTableProcedure extends BaseProcedure {
    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                    ProcedureParameter.required("source_table", DataTypes.StringType),
                    ProcedureParameter.required("table", DataTypes.StringType),
                    ProcedureParameter.optional("location", DataTypes.StringType),
                    ProcedureParameter.optional("properties", STRING_MAP)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                            new StructField("imported_files_count", DataTypes.LongType, false, Metadata.empty())
                    });

    private UnitySnapShotTableProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<UnitySnapShotTableProcedure>() {
            @Override
            protected UnitySnapShotTableProcedure doBuild() {
                return new UnitySnapShotTableProcedure(tableCatalog());
            }
        };
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        String source = args.getString(0);
        Preconditions.checkArgument(
                source != null && !source.isEmpty(),
                "Cannot handle an empty identifier for argument source_table");
        String dest = args.getString(1);
        Preconditions.checkArgument(
                dest != null && !dest.isEmpty(), "Cannot handle an empty identifier for argument table");
        String snapshotLocation = args.isNullAt(2) ? null : args.getString(2);

        Map<String, String> properties =new HashMap<>();
        if (!args.isNullAt(3)) {
            args.getMap(3)
                    .foreach(
                            DataTypes.StringType,
                            DataTypes.StringType,
                            (k, v) -> {
                                properties.put(k.toString(), v.toString());
                                return BoxedUnit.UNIT;
                            });
        }

        Preconditions.checkArgument(
                !source.equals(dest),
                "Cannot create a snapshot with the same name as the source of the snapshot.");
        SnapshotTable action = UnitySparkActions.get(tableCatalog().name()).snapshotTable(source).as(dest);

        if (snapshotLocation != null) {
            action.tableLocation(snapshotLocation);
        }

        SnapshotTable.Result result = action.tableProperties(properties).execute();
        return new InternalRow[] {newInternalRow(result.importedDataFilesCount())};
    }

    @Override
    public String description() {
        return "SnapshotTableProcedure";
    }
}

