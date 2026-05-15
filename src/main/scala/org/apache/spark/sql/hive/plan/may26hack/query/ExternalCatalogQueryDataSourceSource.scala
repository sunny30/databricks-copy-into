package org.apache.spark.sql.hive.plan.may26hack.query

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.datasource.CustomRelation
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class ExternalCatalogQueryDataSourceSource extends DataSourceRegister with SchemaRelationProvider with CreatableRelationProvider with RelationProvider with Logging{

  override def shortName(): String = "hubquery"


  override def createRelation(sqlContext:SQLContext, parameters: Map[String, String], schema: StructType):BaseRelation = {
    val relation = ExternalCatalogQueryRelation(sqlContext,parameters)
    relation.setSchema(schema)
    relation
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    logInfo("Inside createRelation of CreatableRelationProvider. Save Mode = " + mode)

    val relation = ExternalCatalogQueryRelation(sqlContext, parameters)
    relation
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    logInfo("Inside createRelation of RelationProvider")
    val relation = ExternalCatalogQueryRelation(sqlContext, parameters)
    relation
  }


}
