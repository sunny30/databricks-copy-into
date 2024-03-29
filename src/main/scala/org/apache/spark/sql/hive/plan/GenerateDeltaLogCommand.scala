package org.apache.spark.sql.hive.plan

import org.apache.derby.catalog.UUID
import org.apache.hadoop.fs.Path
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.hive.datashare.ConverterUtil

import scala.collection.mutable.ListBuffer

case class GenerateDeltaLogCommand(table: Option[CatalogTable],
                                   location: Option[String],
                                   format: String
                                  ) extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val path = if (table.isDefined) {
      table.get.storage.locationUri.get.toString
    } else {
      location.get
    }
    ConverterUtil(None, table, format).generateDeltaLog(sparkSession, path, format)
    scala.collection.immutable.Seq.empty[Row]
  }


}


case class CopyIntoFromLocationCommand(databaseName: String,
                                       newTableName: String,
                                       fromLocation: String,
                                       format: String,
                                       optionsMap: Option[Map[String, String]] = None,
                                       storageMap: Option[Map[String, String]] = None) extends LeafRunnableCommand {


  override val output: Seq[Attribute] = Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val df = sparkSession.read.format(format).load(fromLocation)
    val qualifiedTable = databaseName + "." + newTableName
    df.write.insertInto(qualifiedTable)
    scala.collection.immutable.Seq.empty[Row]
  }


}


case class CopyIntoFromSelectClauseCommand(databaseName: String,
                                           newTableName: String,
                                           fromLocation: String,
                                           format: String,
                                           selectClause: String,
                                           optionsMap: Option[Map[String, String]] = None,
                                           storageMap: Option[Map[String, String]] = None) extends LeafRunnableCommand {


  override val output: Seq[Attribute] = Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import sparkSession.implicits._
    val df = sparkSession.read.format(format).load(fromLocation)
    val ttlViewName = String.format("%s_%s", "ttlView", "1")
    df.createTempView(ttlViewName)
    //    val colClauses = selectClause.split(",").map(cl => expr(cl)).toSeq
    //    val resultDf = df.select(colClauses)
    val resultDf = sparkSession.sql(selectClause + "from " + ttlViewName)
    val qualifiedTable = databaseName + "." + newTableName
    resultDf.write.insertInto(qualifiedTable)
    scala.collection.immutable.Seq.empty[Row]
  }
}

  case class CopyIntoFromFilesCommand(databaseName: String,
                                      newTableName: String,
                                      fromLocation: String,
                                      format: String,
                                      files: Seq[String],
                                      formatOptions: Option[Map[String, String]] = None,
                                      copyOptionsMap: Option[Map[String, String]] = None) extends LeafRunnableCommand {


    override val output: Seq[Attribute] = Nil
    override def run(sparkSession: SparkSession): Seq[Row] = {
      import sparkSession.implicits._
      var mergedOptionsMap: Map[String, String] = Map.empty[String, String]
      if (!formatOptions.isEmpty) {
        mergedOptionsMap = formatOptions.get
      }
      if (!copyOptionsMap.isEmpty) {
        mergedOptionsMap = mergedOptionsMap.++(copyOptionsMap.get)
      }
      var df:sql.DataFrame = None.get
      // pass comma separated list of files to load into spark dataframe
      if (!files.isEmpty) {
        val qualifiedFiles = files.map(e => String.format("%s/%s", fromLocation, e))
        df = sparkSession.read.options(mergedOptionsMap).format(format).load(paths = qualifiedFiles: _*)
      } else {
         df = sparkSession.read.options(mergedOptionsMap).format(format).load(fromLocation)
      }
      val qualifiedTable = databaseName + "." + newTableName
      df.write.options(mergedOptionsMap).insertInto(qualifiedTable)
      scala.collection.immutable.Seq.empty[Row]
    }

}
