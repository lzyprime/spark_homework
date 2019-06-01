import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{Row, SparkSession}

object RDD2DataFrame {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val inputFile = "file:///home/prime/Documents/Spark/spark_code/shi_yan_3/SparkData/4/employee.txt"
    val conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sql = new SparkSession.Builder().config(conf).getOrCreate()

    val data = sc.textFile(inputFile).map(_.split(",")).map(p => Row(p(0), p(1), p(2)))
    val schema = new StructType(Array(StructField("id", StringType, true), StructField("name", StringType, true), StructField("age", StringType, true)))
    val df = sql.createDataFrame(data, schema)

    df.show()
  }
}
