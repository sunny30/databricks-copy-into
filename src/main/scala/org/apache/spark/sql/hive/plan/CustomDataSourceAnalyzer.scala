package org.apache.spark.sql.hive.plan

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedAttribute, UnresolvedRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, DataSource, FileFormat, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class CustomDataSourceAnalyzer(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  def getFileFormat(formatName: String): FileFormat = {
    formatName.toLowerCase match {
      case "csv" => new CSVFileFormat
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "orc" => new OrcFileFormat
      case "avro" => new AvroFileFormat
      case _ => new CSVFileFormat
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case DataSourceV2Relation(table: V1Table, _, _, _, _) =>

      val provider = table.v1Table.provider.getOrElse("custom")
      val dataSource = DataSource(
        session,
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.v1Table.partitionColumnNames,
        bucketSpec = table.v1Table.bucketSpec,
        className = table.v1Table.provider.get,
        options = table.v1Table.storage.properties,
        catalogTable = Some(table.v1Table))

      if (provider.equalsIgnoreCase("hive") || provider.equalsIgnoreCase("csv")
        || provider.equalsIgnoreCase("parquet")
       || provider.equalsIgnoreCase("orc")
       || provider.equalsIgnoreCase("avro")) {
        val schemaColName = table.v1Table.dataSchema.map(f => f.name)
        val partSchemaColNames = table.v1Table.partitionSchema.map(f => f.name)
        val defaultTableSize = SparkSession.active.sessionState.conf.defaultSizeInBytes
        val fileCatalog = new CatalogFileIndex(
          SparkSession.active,
          table.v1Table,
          table.v1Table.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))

        //val source = DataSource.lookupDataSource("hive", SparkSession.active.sessionState.conf)
        //val fileFormat = source.getConstructor().newInstance().asInstanceOf[FileFormat]
        val ff = if (provider.equalsIgnoreCase("hive")) {
          getHiveTableFileFormat(table.v1Table)
        } else {
          getFileFormat(provider)
        }
        val relation = LogicalRelation(relation = HadoopFsRelation(
          location = fileCatalog,
          partitionSchema = table.v1Table.partitionSchema,
          dataSchema = table.v1Table.dataSchema,
          fileFormat = ff,
          options = table.v1Table.storage.properties,
          bucketSpec = None
        )(SparkSession.active), table = table.v1Table)

        relation

      } else {
        if (provider.equalsIgnoreCase("custom")) {
          LogicalRelation(dataSource.resolveRelation(false), table.v1Table)
        } else {
          LogicalRelation(dataSource.resolveRelation(true), table.v1Table)
        }
      }

    //in managed catalog we have to fix this.
    case x@Project(p, child@SubqueryAlias(identifier, child1: DataSourceV2Relation))
      if child1.catalog.isDefined =>
      x.setAnalyzed()
      //      child.setAnalyzed()
      //      child1.setAnalyzed()
      val table = child1.table.asInstanceOf[V1Table]
      val provider = child1.table.asInstanceOf[V1Table].v1Table.provider.getOrElse("custom")
      val dataSource = DataSource(
        session,
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.v1Table.partitionColumnNames,
        bucketSpec = table.v1Table.bucketSpec,
        className = table.v1Table.provider.get,
        options = table.v1Table.storage.properties,
        catalogTable = Some(table.v1Table))

      if (provider.equalsIgnoreCase("hive") || provider.equalsIgnoreCase("csv")
        || provider.equalsIgnoreCase("parquet")
        || provider.equalsIgnoreCase("orc")
        || provider.equalsIgnoreCase("avro")) {
        val schemaColName = table.v1Table.dataSchema.map(f => f.name)
        val partSchemaColNames = table.v1Table.partitionSchema.map(f => f.name)
        val dataCols = child1.output.filter(p => schemaColName.contains(p.name))
        val partCols = child1.output.filter(p => partSchemaColNames.contains(p.name))
        val defaultTableSize = SparkSession.active.sessionState.conf.defaultSizeInBytes
        val fileCatalog = new CatalogFileIndex(
          SparkSession.active,
          table.v1Table,
          table.v1Table.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))

        //val source = DataSource.lookupDataSource("hive", SparkSession.active.sessionState.conf)
        //val fileFormat = source.getConstructor().newInstance().asInstanceOf[FileFormat]
        val ff = if (provider.equalsIgnoreCase("hive")) {
          getHiveTableFileFormat(table.v1Table)
        } else {
          getFileFormat(provider)
        }
        val relation = LogicalRelation(relation = HadoopFsRelation(
          location = fileCatalog,
          partitionSchema = table.v1Table.partitionSchema,
          dataSchema = table.v1Table.dataSchema,
          fileFormat = ff,
          options = table.v1Table.storage.properties,
          bucketSpec = None
        )(SparkSession.active), table = table.v1Table)
        val newRelation = relation.copy(output = child1.output)
        val newChild = child.copy(identifier = identifier, child = newRelation)
        val op = x.copy(projectList = p, child = newChild)
        op.resolved
        op.setAnalyzed()
        op
      } else {
        val relation = if (provider.equalsIgnoreCase("custom")) {
          LogicalRelation(dataSource.resolveRelation(false), table.v1Table)
        } else {
          LogicalRelation(dataSource.resolveRelation(true), table.v1Table)
        }
        val newRelation = relation.copy(output = child1.output, catalogTable = Some(table.v1Table), relation = relation.relation, isStreaming = false)
        val newChild = child.copy(identifier = identifier, child = newRelation)
        val op = x.copy(projectList = p, child = newChild)
        op.resolved
        op.setAnalyzed()
        op
      }

    case u: UnresolvedTable =>
      if (u.multipartIdentifier.size == 3) {
        val catName = u.multipartIdentifier(0)
        val dbName = u.multipartIdentifier(1)
        val tableName = u.multipartIdentifier(2)
        val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog
        val tc = sessionCatalog.loadTable(Identifier.of(Seq(dbName).toArray, tableName))
        tc match {
          case d: DeltaTableV2 => (ResolvedTable.create(sessionCatalog, u.multipartIdentifier.asIdentifier, d))
          case _ => u
        }
      } else {
        u
      }

    // child.setAnalyzed()
    //  child
    case InsertIntoStatement(u: UnresolvedRelation, m: Map[String, Option[String]], a: Seq[String], q: LogicalPlan, f: Boolean, ip: Boolean, c: Boolean) =>
      val (catalogName, dbName, tableName) = if (u.multipartIdentifier.size == 2) {
        //extract catalog name from conf
        if (SparkSession.active.conf.contains("spark.insert.catalog")) {
          (SparkSession.active.conf.get("spark.insert.catalog"), u.multipartIdentifier.head, u.multipartIdentifier.last)
        } else {
          ("spark_catalog", u.multipartIdentifier.head, u.multipartIdentifier.last)
        }
      } else if (u.multipartIdentifier.size == 3) {
        (u.multipartIdentifier.head, u.multipartIdentifier(1), u.multipartIdentifier.last)
      } else {
        ("spark_catalog", u.multipartIdentifier.head, u.multipartIdentifier.last)
      }
      val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
      val catalogTable = sessionCatalog.loadTable(Identifier.of(Seq(dbName).toArray, tableName))
      val ct = catalogTable.asInstanceOf[V1Table].v1Table
      q.setAnalyzed()

      if(ct.provider.getOrElse("custom").equalsIgnoreCase("custom")){
        val table = ct
        val dataSource = DataSource(
          session,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.partitionColumnNames,
          bucketSpec = table.bucketSpec,
          className = table.provider.get,
          options = table.storage.properties,
          catalogTable = Some(table)
        )

        val relation = LogicalRelation(dataSource.resolveRelation(false), table)
        InsertIntoStatement(relation, m, a, q, f, ip, c)

      }else {
        InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(ct.storage.locationUri.get.toString),
          staticPartitions = Map.empty,
          ifPartitionNotExists = false,
          partitionColumns = ct.partitionColumnNames.map(UnresolvedAttribute.quoted),
          bucketSpec = None,
          fileFormat = getFileFormat(ct.provider.getOrElse("csv")),
          options = Map.empty,
          query = q,
          mode = SaveMode.Append,
          catalogTable = Some(ct),
          fileIndex = None,
          outputColumnNames = ct.schema.map(f => f.name)
        )
      }

    case InsertIntoStatement(d: DataSourceV2Relation, m: Map[String, Option[String]], a: Seq[String], q: LogicalPlan, f: Boolean, ip: Boolean, c: Boolean) => {
      d.table match {
        case dtb: DeltaTableV2 => plan
        case v:V1Table =>

          val ct = d.table.asInstanceOf[V1Table].v1Table
          if(ct.provider.getOrElse("custom").equalsIgnoreCase("custom")){
            val table = ct
            val dataSource = DataSource(
              session,
              // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
              // inferred at runtime. We should still support it.
              userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
              partitionColumns = table.partitionColumnNames,
              bucketSpec = table.bucketSpec,
              className = table.provider.get,
              options = table.storage.properties,
              catalogTable = Some(table)
            )

            val relation = LogicalRelation(dataSource.resolveRelation(false), table)
            InsertIntoStatement(relation, m, a, q, f, ip,c)


          }else {
            InsertIntoHadoopFsRelationCommand(
              outputPath = new Path(ct.storage.locationUri.get.toString),
              staticPartitions = Map.empty,
              ifPartitionNotExists = false,
              partitionColumns = ct.partitionColumnNames.map(UnresolvedAttribute.quoted),
              bucketSpec = None,
              fileFormat = getFileFormat(ct.provider.getOrElse("csv")),
              options = Map.empty,
              query = q,
              mode = SaveMode.Append,
              catalogTable = Some(ct),
              fileIndex = None,
              outputColumnNames = ct.schema.map(f => f.name)
            )
          }
      }
      }



    case p: LogicalPlan => p
  }

  def getHiveTableFileFormat(table: CatalogTable): FileFormat = {
    table.storage.properties("fileformat").toLowerCase match {
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "csv" => new CSVFileFormat
      case "avro" => new AvroFileFormat
      case "json" => new JsonFileFormat
      case "text" => new CSVFileFormat
      case "_" => throw new IllegalAccessException("invalid format")
    }
  }


}
