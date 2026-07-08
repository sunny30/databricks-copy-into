package org.apache.spark.sql.hive.plan

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.TableNameContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableSchemaChangeCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.listener.CrossThreadSqlHolder
import org.apache.spark.sql.hive.plan.spark.sql.connector.{V2CustomTable, V2Table}
import org.apache.spark.sql.types.StructType

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.jdk.CollectionConverters.asScalaBufferConverter

object CLSUtils {

  def isViewTagPresent(plan:LogicalPlan):Boolean ={
    plan.getTagValue(TreeNodeTag[String]("view-sql-plan")).isDefined
  }
  def isViewsPlan(plan:LogicalPlan):Boolean={
    plan.find(isViewTagPresent).isDefined
  }
  def tagSingleViewPlan(plan:LogicalPlan):Unit={
    val tagKey = "view-sql-plan"
    plan.setTagValue(TreeNodeTag[String](tagKey), "true")
  }

  def isTimeTravelTagPresentAtLogicalRelation(plan: LogicalPlan):Boolean ={
    plan.getTagValue(TreeNodeTag[String]("delta-time-travel-read")).isDefined
  }

  def tagViewPlan(plan: LogicalPlan):Unit={
    plan.foreach(tagSingleViewPlan)
  }

  def isExternalCatalogTable(catalogTable: CatalogTable):Boolean ={
    catalogTable.provider.getOrElse("hive").equalsIgnoreCase("custom")
  }

  def isExternalCatalogTable(table: Table): Boolean = {
    table match {
      case v2Table: V2Table =>
        isExternalCatalogTable(v2Table.v1Table)
      case _ => false
    }
  }

  def getSecureDataSource(plan: LogicalPlan): LogicalPlan = {
    if(CLSUtils.isViewsPlan(plan)){
      return plan
    }
    plan match {
      case ds@DataSourceV2Relation(table, output, catalog, identifier, options) if !CDCReader.isCDCRead(options) && !isExternalCatalogTable(table)  =>
        if(table!=null) {
          getSecurePlanFromDataSourceV2(ds, table)
        }else{
          ds
        }
      case lr@LogicalRelation(relation, output, catalogTable, isStreaming) if catalogTable.isDefined && !isExternalCatalogTable(catalogTable.get) =>
        getSecurePlanFromLogicalRelation(lr, catalogTable.get)
      case _ => plan

    }
  }

  //covers Iceberg and V2Table
  def getSecurePlanFromDataSourceV2(ds: DataSourceV2Relation, table: Table): LogicalPlan = {
    val (catalogName, dbName, tableName) = getCatalogTableDetails(table)
    if(catalogName.isEmpty && dbName.isEmpty && tableName.isEmpty){
      return ds
    }
    if(isExternalCatalog(catalogName)){
      return ds
    }

    val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
    getSecureLeafPlan(secureTable, ds)
  }

  def getSecurePlanFromLogicalRelation(ds: LogicalRelation, table: CatalogTable): LogicalPlan = {
    println("Inside getSecurePlanFromLogicalRelation")
    val (catalogName, dbName, tableName) = (table.identifier.catalog.getOrElse("default"), table.identifier.database.getOrElse("default"), table.identifier.table)

    if(isExternalCatalog(catalogName)){
      return ds
    }
    val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
    getSecureLeafPlan(secureTable, ds)
  }


  def getCatalogTableDetails(table: Table): (String, String, String) = {
    println("{}", table.toString)
    val ct = table match {
      case c: CatalogTable => c

      case v2CustomTable: V2CustomTable =>
        v2CustomTable.catalogTable

      case v2Table: V2Table => v2Table.v1Table
      // (ct.identifier.catalog.getOrElse("default"),ct.identifier.database.getOrElse("default"), ct.identifier.table)
      case deltaTableV2: DeltaTableV2 if deltaTableV2.catalogTable.isDefined => deltaTableV2.catalogTable.get
//      deltaTableV2.copy()
      case sparkTable: SparkTable =>
        val multiPartName = sparkTable.name().split("\\.").toArray
        if (multiPartName.length == 3) {
          val plugin = SparkSession.active.sessionState.catalogManager.catalog(multiPartName(0))
          plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(multiPartName(1), multiPartName(2))
        } else {
          null //bad code needs to be fixed.
        }
      case _ =>
        val multiPartName = table.name().split("\\.").toArray
        if (multiPartName.length == 3) {
          val plugin = SparkSession.active.sessionState.catalogManager.catalog(multiPartName(0))
          plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(multiPartName(1), multiPartName(2))
        } else {
          null //bad code needs to be fixed.
        }


    }

    if(ct == null){
      ("","","")
    }else {
      (ct.identifier.catalog.getOrElse("default"), ct.identifier.database.getOrElse("default"), ct.identifier.table)
    }

  }


  def getSecureTableFrom(catalogName: String, db: String, table: String): CatalogTable = {
    val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
    val ct = plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(db, table)
    ct
  }

  def getSecureViewPlan(view:View):LogicalPlan={
    val tid = view.desc.identifier
    val (catalogName, dbName, tableName) = (tid.catalog.getOrElse("default"), tid.database.getOrElse("default"), tid.table)
    val secureCatalogTable = getSecureTableFrom(catalogName,dbName,tableName)
    getSecureLeafPlan(secureCatalogTable, view)
  }

  def getSecureLeafPlan(catalogTable: CatalogTable, leafPlan: LogicalPlan): LogicalPlan = {

    val tagKey = if(catalogTable.tableType == CatalogTableType.VIEW){
      "col-view-sec"
    }else{
      "col-table-sec"
    }

    if (isTimeTravelTagPresentAtLogicalRelation(leafPlan)) {
      return leafPlan
    }

    val resolver  = SparkSession.active.sessionState.conf.resolver
    if (leafPlan.getTagValue(TreeNodeTag[String]("cls-sec")).isEmpty) {
      val secureFields = catalogTable.schema.fields.map(f => f.name).toSet
      println("***Secure fields name***"+secureFields.mkString(","))
      val secureAttributes = leafPlan.output.filter(at => secureFields.contains(at.name))
      leafPlan.setTagValue(TreeNodeTag[String]("cls-sec"), "cls-sec")
      val prj = Project(secureAttributes, leafPlan)
      prj.setTagValue(TreeNodeTag[String](tagKey), "true")

      val sameOutput =
        secureAttributes.size == leafPlan.output.size &&
          secureAttributes.zip(leafPlan.output).forall { case (secureAttr, outputAttr) =>
            resolver(secureAttr.name, outputAttr.name)
          }

     if(sameOutput || isTimeTravelTagPresentAtLogicalRelation(leafPlan)){
       return leafPlan
     }else {
       val analyzed = SparkSession.active.sessionState.analyzer.execute(prj)
       //analyzed.foreach(pl => pl.setTagValue(TreeNodeTag[String]("cls-sec"), "cls-sec"))

       analyzed
//       if (tagKey == "col-table-sec") {
//         analyzed
////         SecureRelationalTable(
////           desc = catalogTable,
////           member = analyzed, // Project([secure cols], DSv2/LogicalRelation)
////           secureOutput = analyzed.output
//         )
//       }else{
//         analyzed
//       }
     }

    } else {
      leafPlan
    }
  }

  def relationExists(multipartIdentifier: Seq[String]): Boolean = {
    val cat = SparkSession.active.sessionState.catalogManager.currentCatalog.name()
    val (catalogName:String, dbName:String, tableName:String) = if (multipartIdentifier.size == 3) {
      (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
    } else if (multipartIdentifier.size == 2) {
      (cat, multipartIdentifier(0), multipartIdentifier(1))
    } else {
      (cat, "default", multipartIdentifier(0))
    }
    val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
    val ident = Identifier.of(Seq(dbName).toArray, tableName)
    plugin.asInstanceOf[TableCatalog].tableExists(ident)

  }

  def getSecureTableFromMultiPart(multipartIdentifier: Seq[String]): Option[CatalogTable] ={
    val catalogName = SparkSession.active.sessionState.catalogManager.currentCatalog.name()
    val res = if (multipartIdentifier.size == 3) {
      (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
    } else if (multipartIdentifier.size == 2) {
      (catalogName, multipartIdentifier(0), multipartIdentifier(1))
    } else {
      (catalogName, "default", multipartIdentifier(0))
    }
    try {
      val ct = getSecureTableFrom(res._1, res._2, res._3)
      if(ct == null)
        return None
      Some(ct)
    } catch {
      case e: Exception => None
    }
  }

  def getSecureColumns(multipartIdentifier: Seq[String]):Option[Seq[String]]={

    val catalogName = SparkSession.active.sessionState.catalogManager.currentCatalog.name()
    val res = if (multipartIdentifier.size == 3) {
      (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
    } else if (multipartIdentifier.size == 2) {
      (catalogName, multipartIdentifier(0), multipartIdentifier(1))
    } else {
      (catalogName, "default", multipartIdentifier(0))
    }
    try {
      if(isExternalCatalog(res._1)){
        return None
      }
      val ct = getSecureTableFrom(res._1, res._2, res._3)
      Some(ct.schema.fields.map(f => f.name).toSeq)
    }catch {
      case e:Exception => None
    }
  }

  def isPlanAlreadyHaveSecureProjection(plan:LogicalPlan):Boolean = {
    plan.find(pl => isSecureTableProjection(pl)).isDefined
  }

  def getProjectedTable(plan:LogicalPlan,ctx: TableNameContext):LogicalPlan={
    println("Inside getProjectedTable")
    if(ctx.identifierReference()!=null && !isPlanAlreadyHaveSecureProjection(plan)){
      val multiParts = ctx.identifierReference().multipartIdentifier().parts.asScala.map(_.getText).toSeq
      val secureColumns = getSecureColumns(multiParts)
      val ct = getSecureTableFromMultiPart(multiParts)
      val tag_key = ct match {
        case Some(table) => if (table.tableType == CatalogTableType.VIEW){
          "col-view-sec"
        }else{
          "col-table-sec"
        }
        case None => ""
      }
      secureColumns match {
        case Some(cols) =>
          if(tag_key.nonEmpty && tag_key.equalsIgnoreCase("col-table-sec")) {
            val secureAttributes = cols.map(name => UnresolvedAttribute.apply(name))
            val prj = Project(secureAttributes, plan)

            prj.setTagValue(TreeNodeTag[String](tag_key), "true")
            prj
          }else{
            plan
          }
        case _ => plan
      }
    }else{
      plan
    }

  }

  def isSecureTableProjection(plan:LogicalPlan):Boolean ={
    plan.getTagValue(TreeNodeTag[String]("col-table-sec")).isDefined
  }

  def removeSecureProjection(plan:LogicalPlan):LogicalPlan ={
    plan.transformUpWithSubqueries {
      case project: Project if isSecureTableProjection(project)=>
       removeSecureProjection(project.child)

      case t: SecureRelationalTable =>
        removeSecureProjection(t.member)

      case plan: LogicalPlan => plan
    }
  }

  def getSecureRelation(plan:LogicalPlan):LogicalPlan= {

    if (CLSUtils.isViewTagPresent(plan)) {
      plan
    } else {
      val pl = plan match {
        case t: SecureRelationalTable => t.member
        case _ => CLSUtils.getSecureDataSource(plan)
      }
      pl
    }
  }

  def validateCreateViewPlan(plan: LogicalPlan): Boolean = {
    !plan.collectLeaves().forall(p => validatatePartialTablePermissionOnDataSources(p))
  }

  def validatatePartialTablePermissionOnDataSources(plan:LogicalPlan):Boolean = {
     plan match {
      case ds@DataSourceV2Relation(table, output, catalog, identifier, options) if !CDCReader.isCDCRead(options) =>
        val (catalogName, dbName, tableName) = getCatalogTableDetails(table)
        val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
        sameFieldsUnordered(table.schema, secureTable.schema)

      case lr@LogicalRelation(relation, output, Some(catalogTable), isStreaming)  =>
        val (catalogName, dbName, tableName) = (catalogTable.identifier.catalog.getOrElse("default"), catalogTable.identifier.database.getOrElse("default"), catalogTable.identifier.table)
        val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
        sameFieldsUnordered(relation.schema, secureTable.schema)

      case u@UnresolvedRelation(multipartIdentifier, options, false) =>
        val (catalogName, dbName, tableName) =if(multipartIdentifier.length == 3){
          (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
        }else if(multipartIdentifier.length == 2){
          ("default", multipartIdentifier(0), multipartIdentifier(1))
        }else{
          ("default", "default", multipartIdentifier(0))
        }
        val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
        val tableIdent = Identifier.of(Seq(dbName).toArray, tableName)
        val ct = plugin.asInstanceOf[TableCatalog].loadTable(tableIdent)
        val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
        sameFieldsUnordered(ct.schema, secureTable.schema)

      case _ => true
    }
  }


  def sameFieldsUnordered(a: StructType, b: StructType): Boolean = {
    if (a.length != b.length) return false
    val bByName = b.fields.map(f => f.name.toLowerCase() -> f.dataType).toMap
    a.fields.forall(fa => bByName.get(fa.name.toLowerCase()).contains(fa.dataType))
  }

  def syncSchemaAtLoadAndOverWrite(table:Table, ct:CatalogTable, catalogName:String):Unit ={
    val trueSchema = table.schema()
    val msSchema = ct.schema
    if(!sameFieldsUnordered(trueSchema,msSchema)){
      val newCt = ct.copy(schema = trueSchema)
      val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
      plugin.asInstanceOf[TableSchemaChangeCatalog].alterUnsafeCatalogTable(newCt)
    }
  }


  def isExternalCatalog(catalogName:String):Boolean = {
    if(SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")){
      if(catalogName.equalsIgnoreCase("ecat")){
        true
      }else{
        false
      }
    }else{
      false//Here we need to put HMSUtils code
    }
  }



  def shouldSyncSchemaAtLoad: Boolean = {
    !isCurrentSqlMergeCommand
  }

  def isCurrentSqlMergeCommand: Boolean =
    isMergeCommand(normalizeSqlForCommandDetection(Option(CrossThreadSqlHolder.getSqlText).getOrElse("")))

  def isDeltaMergeAnalysisStack: Boolean =
    Thread.currentThread().getStackTrace.exists { frame =>
      frame.getClassName == "org.apache.spark.sql.delta.ResolveDeltaMergeInto$"
    }

  private def normalizeSqlForCommandDetection(sqlText: String): String = {
    val withoutLeadingComments = sqlText.trim
      .replaceFirst("(?s)^(?:/\\*.*?\\*/\\s*)+", "")
      .replaceFirst("(?m)^(?:--[^\\n]*(?:\\n|$)\\s*)+", "")
      .trim
      .toUpperCase(java.util.Locale.ROOT)
    withoutLeadingComments.stripPrefix("EXPLAIN ").trim
  }

  private def isMergeCommand(normalizedSql: String): Boolean =
    normalizedSql.startsWith("MERGE ")





}
