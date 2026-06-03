package org.apache.spark.sql.hive.catalog.cls

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
trait ExternalSecureCatalog extends ExternalCatalog{

  def getSecureTable(db: String, table: String): CatalogTable

  def alterUnsafeCatalogTable(ct: CatalogTable):Unit

}
