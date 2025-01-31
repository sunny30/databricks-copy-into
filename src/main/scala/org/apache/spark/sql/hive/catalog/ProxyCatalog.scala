package org.apache.spark.sql.hive.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ProxyCatalog(params:Map[String,String]=Map.empty, catalogName:String, proxyDBName:Option[String]) {


  private val reserveCatalog = "cat"

  private val reserveDb = "reservedb"
  def listDatabase():Seq[String] = {
    if(catalogName.equalsIgnoreCase(reserveCatalog)){
      Seq(reserveDb)
    }else{
      Seq.empty[String]
    }
  }

  //this is to check reserve db under reserve catalog
  def databaseExists(dbName:String):Boolean = {
    if(catalogName.equalsIgnoreCase(reserveCatalog) &&
    dbName.equalsIgnoreCase(reserveDb)){
      true
    }else{
      false
    }
  }

  def getDatabase(db: String): CatalogDatabase={
    CatalogDatabase(
      name = reserveDb,
      description = "reserve-database for systems",
      locationUri = CatalogUtils.stringToURI("NA"),
      properties = Map.empty[String,String]
    )
  }

  def listTables(db: String):Seq[String] = {

    if(SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")){
      Seq("resevetbl")
    }else{
      Seq("resevetbl")
    }
  }

  def tableExists(db: String, table: String): Boolean={
    if(catalogName.equalsIgnoreCase(reserveCatalog) &&
    db.equalsIgnoreCase(reserveDb) &&
    listTables(db).contains(table)){
      true
    }else{
      false
    }
  }
  def getTable(db: String, table: String): CatalogTable={
      if(tableExists(db,table)){
        CatalogTable(
          identifier = TableIdentifier(table, Some(db), Some(reserveCatalog)),
          tableType = CatalogTableType.MANAGED,
          schema = StructType.apply(Array(StructField("id", StringType))),
          owner = "Unity",
          storage = CatalogStorageFormat(
            locationUri = Some(CatalogUtils.stringToURI("NA")),
            inputFormat = None,
            outputFormat = None,
            serde = None,
            compressed = true,
            properties = Map.empty[String, String]
          ),

          properties = Map.empty[String, String],
          //stats = readHiveStats(properties),
          comment = Some("Reserved table")
          )
      }else{
        null
      }
  }



}
