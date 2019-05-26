import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AgeAvg {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sc = new SparkContext("local[*]", "AgeAvg")
    val inputFile = "hdfs://hadoop:9000/user/prime/people_age.txt"
    val data = sc.textFile(inputFile).map(_.split(" ")(1).toInt).collect().toList

    println((0 /: data) (_ + _) * 1.0 / data.size)

  }
}
