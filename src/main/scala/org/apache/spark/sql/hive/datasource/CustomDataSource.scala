package org.apache.spark.sql.hive.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class CustomDataSource extends DataSourceRegister with SchemaRelationProvider with CreatableRelationProvider with RelationProvider with Logging{

  override def shortName(): String = "custom"


  override def createRelation(sqlContext:SQLContext, parameters: Map[String, String], schema: StructType):BaseRelation = {
    val relation = CustomRelation(sqlContext, parameters)
    relation.setSchema(schema)
    relation
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    logInfo("Inside createRelation of CreatableRelationProvider. Save Mode = " + mode)

    val relation = CustomRelation(sqlContext, parameters)
    relation
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    logInfo("Inside createRelation of RelationProvider")
    val relation = CustomRelation(sqlContext, parameters)
    relation
  }


}
