package org.apache.spark.sql.connector.catalog;

import org.apache.iceberg.types.Types
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable}
import org.apache.spark.sql.types.StructType;

trait TableSchemaChangeCatalog extends CatalogPlugin {

  def alterTable(
                  ident: Identifier,
                  schema: StructType): Unit = {}


  def getTableLocation(db: String,
                       table: String
                      ): String


  def alterTableStats(
                       db: String,
                       table: String,
                       stats: Option[CatalogStatistics]
                     ):Unit

  def getTableStats(
                   db: String,
                   table: String
                   ): Option[CatalogStatistics]

  def registerTableInMetastore(
                              table:CatalogTable
                              ):Unit

  def loadSecureTable(db:String, table:String): CatalogTable


}
