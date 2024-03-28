package org.apache.spark.sql.hive.datashare

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.{DEFAULT_PARTITION_NAME, unescapePathName}
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.types.{DataType, DateType, DoubleType, IntegerType, LongType, NullType, StringType, StructField, StructType, TimestampType}

import java.util.{Locale, TimeZone}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import java.lang.{Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}


object DataSharePartitionUtils {

  case class InferSchemaWithPartition(columnName: String, datatype: String, isPartition: Boolean, partitionRank: Int)


  def getInferSchemaWithPartition(schema: StructType, orderedPartitionColumns: Seq[String]): Seq[InferSchemaWithPartition] = {
    if (orderedPartitionColumns.nonEmpty) {
      val orderedPartitionColumnsLowerCase = orderedPartitionColumns.map(x => x.toLowerCase)
      val dataSchema = schema.fields.filter(f => !orderedPartitionColumnsLowerCase.contains(f.name.toLowerCase))
      val partitionSchema = orderedPartitionColumnsLowerCase.map(col => {
        val partitionField = schema.fields.find(f => f.name.equalsIgnoreCase(col)).get
        partitionField
      })
      dataSchema.map(f => InferSchemaWithPartition(f.name, f.dataType.simpleString, false, 0)) ++
        partitionSchema.zipWithIndex.map(f => InferSchemaWithPartition(f._1.name, f._1.dataType.simpleString, true, f._2 + 1))
    } else {
      schema.map(f => InferSchemaWithPartition(f.name, f.dataType.simpleString, false, 0))
    }
  }

  def detectPartitionFromSinglePath(partitionPath: Path, basePaths: Set[Path]): (Option[PartitionValues], Option[Path]) = {

    val dateFormatter = DateFormatter()
    val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)


    parsePartition(
      partitionPath,
      typeInference = true,
      basePaths,
      userSpecifiedDataTypes = Map.empty,
      validatePartitionColumns = false,
      java.util.TimeZone.getDefault,
      dateFormatter,
      timestampFormatter)
  }

  def detectPartitionColumnName(baseTablePath: String): Seq[String] = {
    val listOfDirs = FSUtils.listPartitionDirRecurse(baseTablePath).toSeq
    if (listOfDirs.nonEmpty) {
      val mayBePartitionColumnName = detectPartitionFromSinglePath(listOfDirs.head.getHadoopPath, Set(new Path(baseTablePath)))
      if (mayBePartitionColumnName._1.isDefined) {
        mayBePartitionColumnName._1.get.columnNames
      } else {
        Seq.empty[String]
      }
    } else {
      Seq.empty[String]
    }
  }

  def parsePartition(
                      path: Path,
                      typeInference: Boolean,
                      basePaths: Set[Path],
                      userSpecifiedDataTypes: Map[String, DataType],
                      validatePartitionColumns: Boolean,
                      timeZone: TimeZone,
                      dateFormatter: DateFormatter,
                      timestampFormatter: TimestampFormatter): (Option[PartitionValues], Option[Path]) = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    var finished = path.getParent == null
    var currentPath: Path = path

    while (!finished) {
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        return (None, None)
      }

      if (basePaths.contains(currentPath)) {
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
        parsePartitionColumn(currentPath.getName, typeInference, userSpecifiedDataTypes,
          validatePartitionColumns, timeZone, dateFormatter, timestampFormatter)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop
        finished =
          (maybeColumn.isEmpty && columns.nonEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames.toSeq, values.toSeq)), Some(currentPath))
    }
  }


  def parsePartitionColumn(
                            columnSpec: String,
                            typeInference: Boolean,
                            userSpecifiedDataTypes: Map[String, DataType],
                            validatePartitionColumns: Boolean,
                            timeZone: TimeZone,
                            dateFormatter: DateFormatter,
                            timestampFormatter: TimestampFormatter): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = if (userSpecifiedDataTypes.contains(columnName)) {
        val dataType = userSpecifiedDataTypes(columnName)
        val columnValueLiteral = inferPartitionColumnValue(
          rawColumnValue,
          false,
          timeZone,
          dateFormatter,
          timestampFormatter)
        val columnValue = columnValueLiteral.eval()
        val castedValue = Cast(columnValueLiteral, dataType, Option(timeZone.getID)).eval()
        if (validatePartitionColumns && columnValue != null && castedValue == null) {
          throw DeltaErrors.partitionColumnCastFailed(
            columnValue.toString, dataType.toString, columnName)
        }
        Literal.create(castedValue, dataType)
      } else {
        inferPartitionColumnValue(
          rawColumnValue,
          typeInference,
          timeZone,
          dateFormatter,
          timestampFormatter)
      }
      Some(columnName -> literal)
    }
  }


  def inferPartitionColumnValue(
                                 raw: String,
                                 typeInference: Boolean,
                                 timeZone: TimeZone,
                                 dateFormatter: DateFormatter,
                                 timestampFormatter: TimestampFormatter): Literal = {
    val decimalTry = Try {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new JBigDecimal(raw)
      // It reduces the cases for decimals by disallowing values having scale (eg. `1.1`).
      require(bigDecimal.scale <= 0)
      // `DecimalType` conversion can fail when
      //   1. The precision is bigger than 38.
      //   2. scale is bigger than precision.
      Literal(bigDecimal)
    }

    val dateTry = Try {
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // DateType
      dateFormatter.parse(raw)
      val dateValue = Cast(Literal(raw), DateType).eval()
      // Disallow DateType if the cast returned null
      require(dateValue != null)
      Literal.create(dateValue, DateType)
    }

    val timestampTry = Try {
      val unescapedRaw = unescapePathName(raw)
      timestampFormatter.parse(unescapedRaw)
      val timestampValue = Cast(Literal(unescapedRaw), TimestampType, Some(timeZone.getID)).eval()
      require(timestampValue != null)
      Literal.create(timestampValue, TimestampType)
    }

    if (typeInference) {
      Try(Literal.create(Integer.parseInt(raw), IntegerType))
        .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
        .orElse(decimalTry)
        .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
        .orElse(timestampTry)
        .orElse(dateTry)
        .getOrElse {
          if (raw == DEFAULT_PARTITION_NAME) {
            Literal.default(NullType)
          } else {
            Literal.create(unescapePathName(raw), StringType)
          }
        }
    } else {
      if (raw == DEFAULT_PARTITION_NAME) {
        Literal.default(NullType)
      } else {
        Literal.create(unescapePathName(raw), StringType)
      }
    }
  }

  case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal]) {
    require(columnNames.size == literals.size)
  }

}
