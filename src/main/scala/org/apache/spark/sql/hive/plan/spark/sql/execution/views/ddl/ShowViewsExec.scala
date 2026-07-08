package org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{DescribeColumn, DescribeRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, ResolveDefaultColumns, StringUtils, quoteIfNeeded}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsMetadataColumns, SupportsRead, Table, TableCatalog, TableSchemaChangeCatalog}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.{DescribeTableExec, LeafV2CommandExec, V2CommandExec, V2SessionCatalog}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

/**
 * Physical plan node for showing views.
 */
case class ShowViewsExec(
                           output: Seq[Attribute],
                           catalog: TableCatalog,
                           namespace: Seq[String],
                           pattern: Option[String]) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    val tables = catalog.listTables(namespace.toArray).map(ti => catalog.loadTable(ti, null)).
      filter(tbl => tbl match {
        case V2Table(v1Table) if v1Table.tableType == CatalogTableType.VIEW => true
        case _ => false
      }).map(tbl => {
        val ti = tbl.asInstanceOf[V2Table].v1Table.identifier
        Identifier.of(ti.database.toArray, ti.table)
      })
    tables.map { table =>
      if (pattern.map(StringUtils.filterPattern(Seq(table.name()), _).nonEmpty).getOrElse(true)) {
        rows += toCatalystRow(table.namespace().quoted, table.name(), isTempView(table))
      }
    }

    rows.toSeq
  }

  private def isTempView(ident: Identifier): Boolean = {
    catalog match {
      case s: V2SessionCatalog => s.isTempView(ident)
      case _ => false
    }
  }
}



case class RenameCatalogViewExec(catalog: TableCatalog,
  oldIdent: Identifier,
  newIdent: Identifier,
  invalidateCache: () => Option[StorageLevel],
  cacheTable: (SparkSession, LogicalPlan, Option[String], StorageLevel) => Unit) extends LeafV2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
   // newIdent.asTableIdentifier.

    val qualifiedNewIdent = Identifier.of(oldIdent.namespace, newIdent.name)

    catalog.renameTable(oldIdent, qualifiedNewIdent)
    Seq.empty
  }
}

case class SecureDescribeTableExec(describeTableExec: DescribeRelation) extends LeafV2CommandExec{
  override def output: Seq[Attribute] = describeTableExec.output

  override protected def run(): Seq[InternalRow] = {
    val table = describeTableExec.relation.asInstanceOf[ResolvedTable].table
    val (c,d,t) = table match {
      case v:V2Table => (v.v1Table.identifier.catalog.getOrElse("default"), v.v1Table.identifier.database.getOrElse("default"), v.v1Table.identifier.table)
      case dt: DeltaTableV2 => (dt.v1Table.identifier.catalog.getOrElse("default"), dt.v1Table.identifier.database.getOrElse("default"), dt.v1Table.identifier.table)
      case st: SparkTable =>
        val multipartName = st.table().name().split("\\.")
        if(multipartName.size == 3){
          (multipartName(0), multipartName(1), multipartName(2))
        }else if(multipartName.size == 2){
          ("default", multipartName(0), multipartName(1))
        }else{
          ("default", "default", multipartName(0))
        }
    }

    val plugin = SparkSession.active.sessionState.catalogManager.catalog(c)
    val secureCatalogTable =
      plugin match {
        case catalog: TableSchemaChangeCatalog =>
          catalog.loadSecureTable(d, t)
        case _ => plugin.asInstanceOf[TableCatalog].loadTable(Identifier.of(Array(d), t))
      }
    val secureV2Table = secureCatalogTable match {
      case v2Table: V2Table => v2Table
      case catalogTable: CatalogTable =>V2Table(catalogTable)
    }
   // describeTableExec.copy(table = secureV2Table).run()
   val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows,secureV2Table)
    addPartitioning(rows,secureV2Table)

    if (describeTableExec.isExtended) {
      addMetadataColumns(rows,secureV2Table)
      addTableDetails(rows,secureV2Table)
    }
    rows.toSeq

  }


  private def addSchema(rows: ArrayBuffer[InternalRow], table:V2Table): Unit = {
    rows ++= table.schema.map { column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow], table:V2Table): Unit = {
    if (table.partitioning.nonEmpty) {
      val partitionColumnsOnly = table.partitioning.forall(t => t.isInstanceOf[IdentityTransform])
      if (partitionColumnsOnly) {
        rows += toCatalystRow("# Partition Information", "", "")
        rows += toCatalystRow(s"# ${output(0).name}", output(1).name, output(2).name)
        rows ++= table.partitioning
          .map(_.asInstanceOf[IdentityTransform].ref.fieldNames())
          .map { fieldNames =>
            val nestedField = table.schema.findNestedField(fieldNames)
//            assert(nestedField.isDefined,
//              s"Not found the partition column ${fieldNames.map(quoteIfNeeded).mkString(".")} " +
//                s"in the table schema ${table.v1Table.schema.catalogString}.")
            nestedField.get
          }.map { case (path, field) =>
            toCatalystRow(
              (path :+ field.name).map(quoteIfNeeded(_)).mkString("."),
              field.dataType.simpleString,
              field.getComment().orNull)
          }
      } else {
        rows += emptyRow()
        rows += toCatalystRow("# Partitioning", "", "")
        rows ++= table.partitioning.zipWithIndex.map {
          case (transform, index) => toCatalystRow(s"Part $index", transform.describe(), "")
        }
      }
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow], table:V2Table): Unit = table match {
    case hasMeta: SupportsMetadataColumns if hasMeta.metadataColumns.nonEmpty =>
      rows += emptyRow()
      rows += toCatalystRow("# Metadata Columns", "", "")
      rows ++= hasMeta.metadataColumns.map { column =>
        toCatalystRow(
          column.name,
          column.dataType.simpleString,
          Option(column.comment()).getOrElse(""))
      }
    case _ =>
  }


  private def addTableDetails(rows: ArrayBuffer[InternalRow], table:V2Table): Unit = {
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Table Information", "", "")
    rows += toCatalystRow("Name", table.name(), "")

    val tableType = table.v1Table.tableType.name
    rows += toCatalystRow("Type", tableType, "")
    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(propKey => {
        if (table.properties.containsKey(propKey)) {
          rows += toCatalystRow(propKey.capitalize, table.properties.get(propKey), "")
        }
      })
    val properties =
      conf.redactOptions(table.properties.asScala.toMap).toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1).map {
          case (key, value) => key + "=" + value
        }.mkString("[", ",", "]")
    rows += toCatalystRow("Table Properties", properties, "")

    // If any columns have default values, append them to the result.
    ResolveDefaultColumns.getDescribeMetadata(table.schema).foreach { row =>
      rows += toCatalystRow(row._1, row._2, row._3)
    }
  }
}

case class SecureDescribeColumnExec(
                                     override val output: Seq[Attribute],
                                     column: Attribute,
                                     isExtended: Boolean,
                                     table: Table) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    val (c, d, t) = table match {
      case v: V2Table => (v.v1Table.identifier.catalog.getOrElse("default"), v.v1Table.identifier.database.getOrElse("default"), v.v1Table.identifier.table)
      case dt: DeltaTableV2 => (dt.v1Table.identifier.catalog.getOrElse("default"), dt.v1Table.identifier.database.getOrElse("default"), dt.v1Table.identifier.table)
      case st: SparkTable =>
        val multipartName = st.table().name().split("\\.")
        if (multipartName.size == 3) {
          (multipartName(0), multipartName(1), multipartName(2))
        } else if (multipartName.size == 2) {
          ("default", multipartName(0), multipartName(1))
        } else {
          ("default", "default", multipartName(0))
        }
    }

    val plugin = SparkSession.active.sessionState.catalogManager.catalog(c)
    val secureCatalogTable = plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(d, t)
    val secureColumns = secureCatalogTable.schema.map(f=>f.name)
    val doesExist = secureColumns.exists(col => column.name.equalsIgnoreCase(col))
    if(!doesExist){
      throw new IllegalArgumentException("user does not have access to this column")
    }
    val comment = if (column.metadata.contains("comment")) {
      column.metadata.getString("comment")
    } else {
      "NULL"
    }

    rows += toCatalystRow("col_name", column.name)
    rows += toCatalystRow("data_type",
      CharVarcharUtils.getRawType(column.metadata).getOrElse(column.dataType).catalogString)
    rows += toCatalystRow("comment", comment)

    if (isExtended) {
      val colStats = table match {
        case read: SupportsRead =>
          read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
            case s: SupportsReportStatistics =>
              val stats = s.estimateStatistics()
              Some(stats.columnStats().get(FieldReference.column(column.name)))
            case _ => None
          }
        case _ => None
      }

      if (colStats.nonEmpty) {
        if (colStats.get.min().isPresent) {
          rows += toCatalystRow("min", colStats.get.min().toString)
        } else {
          rows += toCatalystRow("min", "NULL")
        }

        if (colStats.get.max().isPresent) {
          rows += toCatalystRow("max", colStats.get.max().toString)
        } else {
          rows += toCatalystRow("max", "NULL")
        }

        if (colStats.get.nullCount().isPresent) {
          rows += toCatalystRow("num_nulls", colStats.get.nullCount().getAsLong.toString)
        } else {
          rows += toCatalystRow("num_nulls", "NULL")
        }

        if (colStats.get.distinctCount().isPresent) {
          rows += toCatalystRow("distinct_count", colStats.get.distinctCount().getAsLong.toString)
        } else {
          rows += toCatalystRow("distinct_count", "NULL")
        }

        if (colStats.get.avgLen().isPresent) {
          rows += toCatalystRow("avg_col_len", colStats.get.avgLen().getAsLong.toString)
        } else {
          rows += toCatalystRow("avg_col_len", "NULL")
        }

        if (colStats.get.maxLen().isPresent) {
          rows += toCatalystRow("max_col_len", colStats.get.maxLen().getAsLong.toString)
        } else {
          rows += toCatalystRow("max_col_len", "NULL")
        }
      }
    }

    rows.toSeq
  }
}

