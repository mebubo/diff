import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import _root_.org.apache.spark.sql.Column

object CompareCSVSpark {
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder
        .appName("Simple Application")
        .master("local[1]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    case class Order(id: Int, name: String, paid: Option[Boolean])

    val customSchema: StructType = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("paid", BooleanType, false),
        StructField("amount", DoubleType, false)
    ))

    val csv1: DataFrame = spark.read
        .format("csv")
        .schema(customSchema)
        .load("1.csv")

    val orderEncoder: Encoder[Order] = implicitly[Encoder[Order]]
    val orderSchema = orderEncoder.schema

    val csv1typed: Dataset[Order] = spark.read
        .format("csv")
        .schema(orderSchema)
        .load("1.csv")
        .as[Order]

    val csv2: DataFrame = spark.read
        .format("csv")
        .schema(customSchema)
        .load("2.csv")

    def and(columns: List[Column]): Column = {
        columns.tail.fold(columns.head)((c1, c2) => c1 and c2)
    }

    trait JoinType
    case object Left extends JoinType
    case object Right extends JoinType
    case object Inner extends JoinType

    def join(df1: DataFrame, df2: DataFrame, key: List[String], jType: JoinType): DataFrame = {
        val columns: List[Column] = key.map(k => df1.col(k) === df2.col(k))
        val column: Column = and(columns)
        jType match {
            case Left =>
                df1.join(df2, column, "leftanti")
            case Right =>
                df2.join(df1, column, "leftanti")
            case Inner =>
                df1.alias("left").join(df2.alias("right"), column, "inner")
        }
    }

    def allNull(df: DataFrame, columns: List[String]): DataFrame = {
        val cols = columns.map(c => df.col(c).isNull)
        val column: Column = and(cols)
        df.filter(column)
    }

    def allNullWithPrefix(df: DataFrame, prefix: String, schema: StructType): DataFrame = {
        allNull(df, qualifiedFieldNames(prefix, schema))
    }

    def qualifiedFieldNames(prefix: String, schema: StructType): List[String] = {
        val cols = schema.fieldNames.toList
        cols.map(c => s"$prefix.$c")
    }

    case class Report(
        onlyLeftCount: Long,
        onlyRightCount: Long,
        diff: List[(String, Histogram)]
    )

    def report(df1: DataFrame, df2: DataFrame, key: List[String]): Report = {
        val onlyLeft = join(df1, df2, key, Left).count()
        val onlyRight = join(df1, df2, key, Right).count()
        val inner = join(df1, df2, key, Inner)
        val fieldNames = df1.schema.fieldNames
        val notKey: List[String] = fieldNames.diff(key).toList
        val d = diff(inner, ("left", "right"), notKey, df1.schema)
        val x = notKey.map(nk => (nk, histo(d, nk)))
        Report(onlyLeft, onlyRight, x)
    }

    def diff(df: DataFrame, prefixes: (String, String), notKey: List[String], schema: StructType): DataFrame = {
        import org.apache.spark.sql.functions._
         val cols: List[Column] = notKey.map(nk => {
            val ft = schema(nk).dataType
            val leftCol = df.col(s"${prefixes._1}.$nk")
            val rightCol = df.col(s"${prefixes._2}.$nk")
            val c = if (ft == IntegerType  || ft == LongType || ft == FloatType || ft == DoubleType) leftCol - rightCol
                    else when(leftCol < rightCol, -1).when(leftCol > rightCol, 1).otherwise(0)
            c.name(nk)
        })
        df.select(cols: _*)
    }

    type Histogram = (List[Double], List[Long])

    def histo(df: DataFrame, field: String): Histogram = {
        val d2 = df.select(df.col(field) cast "double")
        val x = d2.rdd.map(r => r.getDouble(0)).histogram(10)
        return (x._1.toList, x._2.toList)
    }

    val joined = csv1.crossJoin(csv2).where(csv1.col("id") === csv2.col("id"))

    def main(args: Array[String]): Unit = {
        // println(csv1.collectAsList())
        // println(csv2.collectAsList())
        // println(csv1typed.collectAsList())
        // print(joined.collectAsList())
        // val j = join(csv1, csv2, List("paid", "name"), Left)
        // val x = allNull(j, List("left.id", "left.name", "left.paid"))
        // val x = allNullWithPrefix(j, "left", customSchema)

        // println(j.collectAsList())
        val r = report(csv1, csv2, List("id"))
        println(r)
    }
}