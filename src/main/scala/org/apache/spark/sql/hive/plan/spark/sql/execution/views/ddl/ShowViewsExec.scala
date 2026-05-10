package org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{DescribeRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{ResolveDefaultColumns, StringUtils, quoteIfNeeded}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsMetadataColumns, TableCatalog, TableSchemaChangeCatalog}
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.{DescribeTableExec, LeafV2CommandExec, V2CommandExec, V2SessionCatalog}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
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
    val secureCatalogTable = plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(d, t)
    val secureV2Table = V2Table(secureCatalogTable)
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

