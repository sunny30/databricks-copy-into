import org.apache.spark.sql.SparkSession

object LiveApp {

  def testTableFunction(sparkSession: SparkSession):Unit={
    sparkSession.sql("select * from remote_query('select * from live.db.tbl', 'live')").show()
  }

}
