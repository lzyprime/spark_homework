import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val inputFile = "file:///home/prime/Documents/SparkData/1/*.txt"
    val outputFile = "/home/prime/Documents/SparkData/1/C.txt"

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val pw = new PrintWriter(outputFile)

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    textFile.filter(_.trim.length>0)
      .map(line=>(line.trim, ""))
      .groupByKey().sortByKey()
      .keys.collect().foreach(pw.println)

    pw.close()
  }
}
