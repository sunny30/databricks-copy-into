package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TableIdentifierHelper
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.catalog.V1Table.addV2TableProperties
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * An implementation of catalog v2 `Table` to expose v1 table metadata.
 */
case class V2Table(v1Table: CatalogTable) extends Table {

  def catalogTable: CatalogTable = v1Table

  lazy val options: Map[String, String] = {
    v1Table.storage.locationUri match {
      case Some(uri) =>
        v1Table.storage.properties + ("path" -> uri.toString)
      case _ =>
        v1Table.storage.properties
    }
  }

  override lazy val properties: util.Map[String, String] = addV2TableProperties(v1Table).asJava

  override lazy val schema: StructType = v1Table.schema

  override lazy val partitioning: Array[Transform] = {
    import CatalogV2Implicits._
    val partitions = new mutable.ArrayBuffer[Transform]()

    v1Table.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(LogicalExpressions.reference(Seq(col)))
    }

    v1Table.bucketSpec.foreach { spec =>
      partitions += spec.asTransform
    }

    partitions.toArray
  }

  override def name: String = v1Table.identifier.quoted

  override def capabilities: util.Set[TableCapability] =
    util.EnumSet.noneOf(classOf[TableCapability])

  override def toString: String = s"V1Table($name)"

  def getTableCaseInsensitiveStringMap: CaseInsensitiveStringMap={
    val options = V2Table.addV2TableProperties(v1Table) ++ Map("path" -> v1Table.storage.locationUri.get.toString)
    new CaseInsensitiveStringMap(options.asJava)
  }

  def getV2CustomTable: Table = {
    val options = V2Table.addV2TableProperties(v1Table) ++ Map("path" -> v1Table.storage.locationUri.getOrElse(v1Table.location).toString)
    V2CustomTable(name, SparkSession.active, new CaseInsensitiveStringMap(options.asJava), v1Table)
  }

  def getCatalogName: String = {
    v1Table.identifier.catalog.getOrElse("spark_catalog")
  }
}

object V2Table {
  def addV2TableProperties(v1Table: CatalogTable): Map[String, String] = {
    val external = v1Table.tableType == CatalogTableType.EXTERNAL
    val managed = v1Table.tableType == CatalogTableType.MANAGED

    v1Table.properties ++
      v1Table.storage.properties.map { case (key, value) =>
        TableCatalog.OPTION_PREFIX + key -> value
      } ++
      v1Table.provider.map(TableCatalog.PROP_PROVIDER -> _) ++
      v1Table.comment.map(TableCatalog.PROP_COMMENT -> _) ++
      v1Table.storage.locationUri.map(TableCatalog.PROP_LOCATION -> _.toString) ++
      (if (managed) Some(TableCatalog.PROP_IS_MANAGED_LOCATION -> "true") else None) ++
      (if (external) Some(TableCatalog.PROP_EXTERNAL -> "true") else None) ++
      Some(TableCatalog.PROP_OWNER -> v1Table.owner)
  }
}

/**
 * A V2 table with V1 fallback support. This is used to fallback to V1 table when the V2 one
 * doesn't implement specific capabilities but V1 already has.
 */
trait V2TableWithV1Fallback extends Table {
  def v1Table: CatalogTable
}
