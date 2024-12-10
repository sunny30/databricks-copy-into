package org.apache.spark.sql.hive.experiment.sql

object SQLDetailsUtil {

  abstract class PlanDetails() {
    def getRelationalDetails: Seq[QualifiedColumn]
  }


  case class RelationDetails(catalogName:String, dbName: String, tableName: String) extends PlanDetails {
    override def getRelationalDetails: Seq[QualifiedColumn] = {
      Seq(QualifiedColumn(catalogName, dbName, tableName, "all*c"))
    }
  }

  case class QualifiedColumn(catalogName:String, dbName: String, tableName: String, columnName: String)

  case class InterimPlanDetails(optype: String, attributes: Seq[QualifiedColumn],
                                expressions: Seq[String], lineageInfo: Option[Map[String, String]] = None)
    extends PlanDetails {
    override def getRelationalDetails: Seq[QualifiedColumn] = {
      attributes
    }
  }

  case class QualifiedColumns(catalogName:String , dbName: String, tableName: String, columnNames: Seq[String])


  def getQualifiedColumns(inputResult: Seq[QualifiedColumn]): Seq[QualifiedColumns] = {
    inputResult.
      map(r => ((r.catalogName, r.dbName, r.tableName), r.columnName)).
      groupBy(f => f._1).map(mp => {
        val catalogName = mp._1._1
        val dbName = mp._1._2
        val tableName = mp._1._3
        val columns = mp._2.map(x => x._2).distinct
        val normalizedColumns = if(columns.length>1){
          columns.filter(r=> !r.equalsIgnoreCase("all*c"))
        }else{
          columns
        }
        QualifiedColumns(catalogName, dbName, tableName, normalizedColumns)
      }
      ).toSeq
  }
}

