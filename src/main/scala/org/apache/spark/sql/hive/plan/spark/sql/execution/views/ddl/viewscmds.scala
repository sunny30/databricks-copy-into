package org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, UnresolvedLeafNode, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.DescribeCommandSchema
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowViews, UnaryCommand}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UNRESOLVED_RELATION}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{Identifier, TableSchemaChangeCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class ShowCatalogViews(
                      namespace: LogicalPlan,
                      pattern: Option[String],
                      override val output: Seq[Attribute] = ShowViews.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowCatalogViews =
    copy(namespace = newChild)
}

object ShowCatalogViews {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("viewName", StringType, nullable = false)(),
    AttributeReference("isTemporary", BooleanType, nullable = false)())
}


case class RenameCatalogView(
                        child: LogicalPlan,
                        newName: Seq[String],
                        isView: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RenameCatalogView =
    copy(child = newChild)
}

case class CatalogDescribeViewCmd(
                                       catalogName: String,
                                       databaseName:String,
                                       tableName: String,
                                       isExtended: Boolean)
  extends LeafRunnableCommand {

  override val output: Seq[AttributeReference] = DescribeCommandSchema.describeTableAttributes()


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableIdent = Identifier.of(Seq(databaseName).toArray, tableName)
    val table = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(databaseName, tableName)
    val result = new ArrayBuffer[Row]
    val metadata= table match {
      case v2Table:V2Table =>
        v2Table.v1Table

      case dt:DeltaTableV2 =>
        dt.catalogTable.get.copy(
          schema = dt.deltaLog.snapshot.schema,
          partitionColumnNames = dt.deltaLog.snapshot.metadata.partitionColumns,
          properties = dt.catalogTable.get.properties ++ dt.deltaLog.snapshot.getProperties
        )

      case ct: CatalogTable => ct
    }
    describeSchema(metadata.schema, result, header = false)
    if (isExtended) {
      describeFormattedTableInfo(metadata, result)
      //describeFormattedTableDescription(metadata, result)
    }
    result
  }

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row], header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // The following information has been already shown in the previous outputs
    val excludedTableInfo = Seq(
      "Partition Columns",
      "Schema"
    )
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    table.toLinkedHashMap.filterKeys(!excludedTableInfo.contains(_)).foreach {
      s => append(buffer, s._1, s._2, "")
    }
  }

//  private def describeFormattedTableDescription(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
//    val properties: String = table.toLinkedHashMap()
//    append(buffer, "Description", convertToPropertiesMap(properties).getOrElse("description",""), "")
//  }

  protected def append(buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }

  /**
   * this method takes a property string of format [key1=val1,key2=val2] and
   * converts it into an ordered Map of key-value pairs
   * @param propertyStr - property string
   * @return - ordered Map of key-value pairs
   */
  private def convertToPropertiesMap(propertyStr: String): mutable.LinkedHashMap[String, String] = {
    val propertiesMap = mutable.LinkedHashMap[String, String]()
    if (propertyStr.nonEmpty) {
      val strippedProperties = propertyStr.stripPrefix("[").stripSuffix("]")
      strippedProperties.split(", ").foreach { pair =>
        val kv = pair.split("=")
        if (kv.length == 1) {
          propertiesMap += (kv(0).replaceAll("\\s", "") -> "")
        } else {
          propertiesMap += (kv(0).replaceAll("\\s", "") -> kv(1))
        }
      }
    }
    propertiesMap
  }

}


case class ViewUnresolvedRelation(u:UnresolvedRelation)
  extends UnresolvedLeafNode with NamedRelation {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /** Returns a `.` separated name for this relation. */
  def tableName: String = u.multipartIdentifier.quoted

  override def name: String = tableName

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_RELATION)
}


