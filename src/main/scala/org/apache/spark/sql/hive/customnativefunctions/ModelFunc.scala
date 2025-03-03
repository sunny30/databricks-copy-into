package org.apache.spark.sql.hive.customnativefunctions

import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.hive.classloader.reflection.ReflectionUtil
import org.apache.spark.sql.hive.extra.FunctionDescription
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.vectorized.ColumnarRow
import org.apache.spark.unsafe.types
import org.apache.spark.unsafe.types.UTF8String
import org.json.JSONObject

case class ModelFunc(name: String, child: Expression) extends UnaryExpression with ImplicitCastInputTypes {



  override def eval(input: InternalRow): Any = super.eval(input)

  override protected def nullSafeEval(input1: Any):Any={

    child.dataType match {
      case StringType => val inputAsString = input1.toString
      ModelFunc.evalString(name,inputAsString)

      case s:StructType => ModelFunc.evalStruct(name,s)

      case _ => throw new IllegalArgumentException("not supported data type")
    }

  }

  override def prettyName: String = "query_model"

  override def nullable: Boolean = false


  override def dataType: DataType = StringType

  final override  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode ={
    val clazz = ModelFunc.getClass.getCanonicalName.stripSuffix("$")
    child.dataType match {
      case StringType =>
        defineCodeGen(ctx, ev, str => s"""$clazz.evalStringCodegen("$name",$str)""")
      case s: StructType =>
        val schemaString = escape(s.json)
        nullSafeCodeGen(ctx, ev, str => {
          val schemaStringVariable = ctx.freshName("schema_string")
          s"""
             |
             |String ${schemaStringVariable} = "${schemaString}" ;
             |${ev.value} = $clazz.evalStructCodegen("$name", ${str}, ${schemaStringVariable}) ;"""
        }
    )
      case _ => throw new IllegalArgumentException("not supported data type")
    }


  }

  private def escape(raw: String) = {
    var escaped = raw
    escaped = escaped.replace("\\", "\\\\")
    escaped = escaped.replace("\"", "\\\"")
    escaped = escaped.replace("\b", "\\b")
    escaped = escaped.replace("\f", "\\f")
    escaped = escaped.replace("\n", "\\n")
    escaped = escaped.replace("\r", "\\r")
    escaped = escaped.replace("\t", "\\t")
    // TODO: escape other non-printing characters using uXXXX notation
    escaped
  }


  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(child.dataType)
}

object ModelFunc{

  val ru1 = ReflectionUtil

  def evalString(modelName: String, input:String):UTF8String={
    UTF8String.fromString(ru1.reflectThis("org.apache.spark.sql.hive.classloader.reflection.SubjectClass1", "inputText",modelName, input))
   // input
  }

  def evalStringCodegen(modelName: String, input: UTF8String): UTF8String = {
    UTF8String.fromString(ru1.reflectThis("org.apache.spark.sql.hive.classloader.reflection.SubjectClass1", "inputText", modelName, input.toString))
    // input
  }

  def evalStructCodegen(modelName: String, input: InternalRow, schemaString:String): UTF8String = {
    val rs = serialize(input, schemaString)
    UTF8String.fromString(ru1.reflectThis("org.apache.spark.sql.hive.classloader.reflection.SubjectClass1", "inputText", modelName, rs.toString))
    // input
  }

  def serialize(input:InternalRow, schemaString:String):UTF8String={

    var s = ""
    val fields = StructType.fromString(schemaString).fields
    for(n<- 0 to input.numFields-1){
      val ss = getStringValueOfStructFields(input,fields.apply(n).dataType,n)
      if(s.length>0)
        s = s+","+ss
      else
        s = ss
    }
    UTF8String.fromString(s)

  }


  def getStringValueOfStructFields(row:InternalRow, dataType: DataType, ordinal:Int): String = {

    if (dataType.isInstanceOf[BooleanType]) return row.getBoolean(ordinal).toString
    else if (dataType.isInstanceOf[ByteType]) return row.getByte(ordinal).toString
    else if (dataType.isInstanceOf[ShortType]) return row.getShort(ordinal).toString
    else if (dataType.isInstanceOf[IntegerType] || dataType.isInstanceOf[YearMonthIntervalType]) return row.getInt(ordinal).toString
    else if (dataType.isInstanceOf[LongType] || dataType.isInstanceOf[DayTimeIntervalType]) return row.getLong(ordinal).toString
    else if (dataType.isInstanceOf[FloatType]) return row.getFloat(ordinal).toString
    else if (dataType.isInstanceOf[DoubleType]) return row.getDouble(ordinal).toString
    else if (dataType.isInstanceOf[StringType]) return row.getUTF8String(ordinal).toString
    else if (dataType.isInstanceOf[BinaryType]) return row.getBinary(ordinal).toString
    else if (dataType.isInstanceOf[DecimalType]) {
      val t = dataType.asInstanceOf[DecimalType]
      return row.getDecimal(ordinal, t.precision, t.scale).toString()
    }
    else if (dataType.isInstanceOf[DateType]) return row.getInt(ordinal).toString
    else if (dataType.isInstanceOf[TimestampType]) return row.getLong(ordinal).toString
    else if (dataType.isInstanceOf[ArrayType]) return row.getArray(ordinal).toString
    else if (dataType.isInstanceOf[StructType]) return row.getStruct(ordinal, dataType.asInstanceOf[StructType].fields.length).toString
    else if (dataType.isInstanceOf[MapType]) return row.getMap(ordinal).toString
    else throw new UnsupportedOperationException("Datatype not supported " + dataType)
  }

  def evalStruct(modelName: String, input: StructType):UTF8String = {
    UTF8String.fromString(input.prettyJson)

  }

  val fd: FunctionDescription = (
    new FunctionIdentifier("query_model"),
    new ExpressionInfo(classOf[ModelFunc].getCanonicalName, "query_model"),
    (children: Seq[Expression]) => ModelFunc(children.head.toString(),children(1)))
}
