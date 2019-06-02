
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SqlByDataFrame {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sql = SparkSession.builder().appName("SqlByDataFrame").master("local[*]").getOrCreate()
    val df = sql.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://localhost:3306/sparktest",
      "driver" -> "org.mariadb.jdbc.Driver",
      "user" -> "root",
      "password" -> "3821",
      "dbtable" -> "employee",
    )).load
    df.show

    //// write ////
    val data = SparkContext.getOrCreate().makeRDD(List(Row(3, "Marray", "F", 26), Row(4, "Tom", "M", 23)))
    val df2 = sql.createDataFrame(data, df.schema)
    df2.write.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://localhost:3306/sparktest",
      "driver" -> "org.mariadb.jdbc.Driver",
      "user" -> "root",
      "password" -> "3821",
      "dbtable" -> "employee",
    )).mode(SaveMode.Append).save
  }
}
