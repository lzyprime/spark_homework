import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Avg {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val inputFile = "file:///home/prime/Documents/Spark/spark_code/shi_yan_3/SparkData/2/*.input"
    val outputFile = "/home/prime/Documents/Spark/spark_code/shi_yan_3/SparkData/2/avg.output"

    val conf = new SparkConf().setAppName("Avg").setMaster("local[*]")
    val pw = new PrintWriter(outputFile)

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    textFile.filter(_.trim.length > 0)
      .map(line => (line.trim.split(" ")(0), line.trim.split(" ")(1).toInt))
      .groupByKey()
      .map(x => (x._1, f"${(0 /: x._2.toList) (_ + _) * 1.0 / x._2.size}%.2f"))
      .collect().foreach(pw.println)

    pw.close()
  }
}
