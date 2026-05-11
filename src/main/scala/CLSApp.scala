import org.apache.spark.sql.SparkSession

object CLSApp {


  def viewCLSApp(spark: SparkSession):Unit = {

    spark.sql("""create database if not exists cat.cls_db2""")
    spark.sql("create table cat.cls_db2.ppt(id int, name1 string) using parquet PARTITIONED BY (name1)")
    spark.sql("insert into cat.cls_db2.ppt values(1,'sh'), (2, 'su')")
    spark.sql("create view cat.cls_db2.v1(id , cls_name) as select *  from cat.cls_db2.ppt")
    spark.sql("select * from cat.cls_db2.v1").show()


  }

}
