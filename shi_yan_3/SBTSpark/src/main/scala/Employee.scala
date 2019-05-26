import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Employee {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val inputFile = "/home/prime/Documents/SparkData/4/employee.json"

    val sc = new SparkContext("local[*]", "AgeAvg")
    val data = sc.textFile(inputFile)
    val dataFrame = sc.
  }
}
