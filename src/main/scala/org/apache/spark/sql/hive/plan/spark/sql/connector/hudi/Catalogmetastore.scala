package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.concurrent.TrieMap

trait CatalogMetastore {
  def listDatabases(): Array[String]
  def databaseExists(db: String): Boolean
  def getDatabaseProperties(db: String): Map[String, String]
  def createDatabase(db: String, location: String, props: Map[String, String]): Unit
  def alterDatabase(db: String, props: Map[String, String]): Unit
  def dropDatabase(db: String, cascade: Boolean): Boolean

  def listTables(db: String): Array[String]
  def tableExists(db: String, table: String): Boolean
  def getTableMetadata(db: String, table: String): HudiTableMetadata
  def createTable(db: String, table: String, location: String, schema: StructType, props: Map[String, String]): Unit
  def alterTableSchema(db: String, table: String, schema: StructType): Unit
  def dropTable(db: String, table: String): Boolean
}

case class HudiTableMetadata(
                              db: String,
                              name: String,
                              location: String,
                              schema: StructType,
                              properties: Map[String, String]
                            )

/** In-memory backend — swap for HMS or Glue in production. */
class InMemoryCatalogMetastore(opts: CaseInsensitiveStringMap) extends CatalogMetastore {

  private val databases = TrieMap.empty[String, Map[String, String]]
  private val tables    = TrieMap.empty[String, HudiTableMetadata]

  private def key(db: String, tbl: String) = s"$db.$tbl"

  override def listDatabases(): Array[String]               = databases.keys.toArray
  override def databaseExists(db: String): Boolean          = databases.contains(db)
  override def getDatabaseProperties(db: String): Map[String, String] = databases.getOrElse(db, Map.empty)
  override def createDatabase(db: String, loc: String, props: Map[String, String]): Unit = databases(db) = props + ("location" -> loc)
  override def alterDatabase(db: String, props: Map[String, String]): Unit = databases(db) = props
  override def dropDatabase(db: String, cascade: Boolean): Boolean = {
    if (cascade) tables.keys.filter(_.startsWith(s"$db.")).foreach(tables.remove)
    databases.remove(db).isDefined
  }
  override def listTables(db: String): Array[String] =
    tables.keys.filter(_.startsWith(s"$db.")).map(_.stripPrefix(s"$db.")).toArray
  override def tableExists(db: String, tbl: String): Boolean = tables.contains(key(db, tbl))
  override def getTableMetadata(db: String, tbl: String): HudiTableMetadata =
    tables.getOrElse(key(db, tbl), throw new NoSuchElementException(s"Table not found: $db.$tbl"))
  override def createTable(db: String, tbl: String, loc: String, schema: StructType, props: Map[String, String]): Unit =
    tables(key(db, tbl)) = HudiTableMetadata(db, tbl, loc, schema, props)
  override def alterTableSchema(db: String, tbl: String, schema: StructType): Unit = {
    val e = getTableMetadata(db, tbl)
    tables(key(db, tbl)) = e.copy(schema = schema)
  }
  override def dropTable(db: String, tbl: String): Boolean = tables.remove(key(db, tbl)).isDefined
}