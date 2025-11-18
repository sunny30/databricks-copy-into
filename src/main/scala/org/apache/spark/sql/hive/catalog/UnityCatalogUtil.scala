package org.apache.spark.sql.hive.catalog

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.DefinedByConstructorParams
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag


class UnityCatalogUtil(spark:SparkSession) {

  def listColumns(catalogName:String, dbName:String, tableName:String):Dataset[Column]={

    val tableCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName))
//    val v2table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName)) match {
//      case v2Table: V2Table => v2Table.v1Table
//      case _ => throw new IllegalArgumentException("only v2 is allowed")
//    }
    if(table.partitioning() != null) {
      val partitionColumnNames = table.partitioning.toSeq.convertTransforms
      val bucketColumnNames = Nil

      val columns = schemaToColumns(table.schema(), partitionColumnNames._1.contains, bucketColumnNames.contains)
      makeDataset(columns, spark)
    }else{
      val columns =  schemaToColumns(table.schema())
      makeDataset(columns, spark)
    }
  }


  private def schemaToColumns(
                               schema: StructType,
                               isPartCol: String => Boolean = _ => false,
                               isBucketCol: String => Boolean = _ => false): Seq[Column] = {
    schema.map { field =>
      new Column(
        name = field.name,
        description = field.getComment().orNull,
        dataType = field.dataType.simpleString,
        nullable = field.nullable,
        isPartition = isPartCol(field.name),
        isBucket = isBucketCol(field.name))
    }
  }

  def makeDataset[T <: DefinedByConstructorParams : TypeTag](
                                                              data: Seq[T],
                                                              sparkSession: SparkSession): Dataset[T] = {
    val enc = ExpressionEncoder[T]()
    val toRow = enc.createSerializer()
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(DataTypeUtils.toAttributes(enc.schema), encoded)
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    new Dataset[T](queryExecution, enc)
  }

}
