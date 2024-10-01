package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.catalyst.analysis.{ResolveInsertionBase, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Cast, EqualNullSafe, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertIntoStatement, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Project}
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode



object CustomResolveInsertInto extends ResolveInsertionBase {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case i@InsertIntoStatement(r: DataSourceV2Relation, _, _, _, _, _, _)
      if i.query.resolved =>
      // ifPartitionNotExists is append with validation, but validation is not supported
      if (i.ifPartitionNotExists) {
        throw QueryCompilationErrors.unsupportedIfNotExistsError(r.table.name)
      }

      // Create a project if this is an INSERT INTO BY NAME query.
      val projectByName = if (i.userSpecifiedCols.nonEmpty) {
        Some(createProjectForByNameQuery(r.table.name, i))
      } else {
        None
      }
      val isByName = projectByName.nonEmpty || i.byName

      val partCols = partitionColumnNames(r.table)
      validatePartitionSpec(partCols, i.partitionSpec)

      val staticPartitions = i.partitionSpec.filter(_._2.isDefined).mapValues(_.get).toMap
      val query = addStaticPartitionColumns(r, projectByName.getOrElse(i.query), staticPartitions,
        isByName)

      if (!i.overwrite) {
        if (isByName) {
          AppendData.byName(r, query)
        } else {
          AppendData.byPosition(r, query)
        }
      } else if (conf.partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC) {
        if (isByName) {
          OverwritePartitionsDynamic.byName(r, query)
        } else {
          OverwritePartitionsDynamic.byPosition(r, query)
        }
      } else {
        if (isByName) {
          OverwriteByExpression.byName(r, query, staticDeleteExpression(r, staticPartitions))
        } else {
          OverwriteByExpression.byPosition(r, query, staticDeleteExpression(r, staticPartitions))
        }
      }
  }

  private def partitionColumnNames(table: Table): Seq[String] = {
    // get partition column names. in v2, partition columns are columns that are stored using an
    // identity partition transform because the partition values and the column values are
    // identical. otherwise, partition values are produced by transforming one or more source
    // columns and cannot be set directly in a query's PARTITION clause.
    table.partitioning.flatMap {
      case IdentityTransform(FieldReference(Seq(name))) => Some(name)
      case _ => None
    }
  }

  private def validatePartitionSpec(
                                     partitionColumnNames: Seq[String],
                                     partitionSpec: Map[String, Option[String]]): Unit = {
    // check that each partition name is a partition column. otherwise, it is not valid
    partitionSpec.keySet.foreach { partitionName =>
      partitionColumnNames.find(name => conf.resolver(name, partitionName)) match {
        case Some(_) =>
        case None =>
          throw QueryCompilationErrors.nonPartitionColError(partitionName)
      }
    }
  }

  private def addStaticPartitionColumns(
                                         relation: DataSourceV2Relation,
                                         query: LogicalPlan,
                                         staticPartitions: Map[String, String],
                                         isByName: Boolean): LogicalPlan = {

    if (staticPartitions.isEmpty) {
      query

    } else {
      // add any static value as a literal column
      val withStaticPartitionValues = {
        // for each static name, find the column name it will replace and check for unknowns.
        val outputNameToStaticName = staticPartitions.keySet.map { staticName =>
          if (isByName) {
            // If this is INSERT INTO BY NAME, the query output's names will be the user specified
            // column names. We need to make sure the static partition column name doesn't appear
            // there to catch the following ambiguous query:
            // INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')
            if (query.output.exists(col => conf.resolver(col.name, staticName))) {
              throw QueryCompilationErrors.staticPartitionInUserSpecifiedColumnsError(staticName)
            }
          }
          relation.output.find(col => conf.resolver(col.name, staticName)) match {
            case Some(attr) =>
              attr.name -> staticName
            case _ =>
              throw QueryCompilationErrors.missingStaticPartitionColumn(staticName)
          }
        }.toMap

        val queryColumns = query.output.iterator

        // for each output column, add the static value as a literal, or use the next input
        // column. this does not fail if input columns are exhausted and adds remaining columns
        // at the end. both cases will be caught by ResolveOutputRelation and will fail the
        // query with a helpful error message.
        relation.output.flatMap { col =>
          outputNameToStaticName.get(col.name).flatMap(staticPartitions.get) match {
            case Some(staticValue) =>
              // SPARK-30844: try our best to follow StoreAssignmentPolicy for static partition
              // values but not completely follow because we can't do static type checking due to
              // the reason that the parser has erased the type info of static partition values
              // and converted them to string.
              val cast = Cast(Literal(staticValue), col.dataType, ansiEnabled = true)
              cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
              Some(Alias(cast, col.name)())
            case _ if queryColumns.hasNext =>
              Some(queryColumns.next)
            case _ =>
              None
          }
        } ++ queryColumns
      }

      Project(withStaticPartitionValues, query)
    }
  }

  private def staticDeleteExpression(
                                      relation: DataSourceV2Relation,
                                      staticPartitions: Map[String, String]): Expression = {
    if (staticPartitions.isEmpty) {
      Literal(true)
    } else {
      staticPartitions.map { case (name, value) =>
        relation.output.find(col => conf.resolver(col.name, name)) match {
          case Some(attr) =>
            // the delete expression must reference the table's column names, but these attributes
            // are not available when CheckAnalysis runs because the relation is not a child of
            // the logical operation. instead, expressions are resolved after
            // ResolveOutputRelation runs, using the query's column names that will match the
            // table names at that point. because resolution happens after a future rule, create
            // an UnresolvedAttribute.
            EqualNullSafe(
              UnresolvedAttribute.quoted(attr.name),
              Cast(Literal(value), attr.dataType))
          case None =>
            throw QueryCompilationErrors.missingStaticPartitionColumn(name)
        }
      }.reduce(And)
    }
  }
}
