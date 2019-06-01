import java.io.PrintWriter

import scala.util.Random

object CreteData {
  def main(args: Array[String]) {

    val pw = new PrintWriter("/home/prime/Documents/Spark/spark_code/shi_yan_3/SparkData/3/people_age.txt")
    for (i <- 1 to 1000)
      pw.println(i + " " + Random.nextInt(100))
    pw.close()
  }
}
