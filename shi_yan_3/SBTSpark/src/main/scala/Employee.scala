import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object Employee {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 创建 dataframe 2.0.0 版本后 改用 SparkSession.bulider()
    val inputFile = "file:///home/prime/Documents/Spark/spark_code/shi_yan_3/SparkData/4/employee.json"
    val sql = SparkSession.builder().appName("EmployeeJson").master("local[*]").getOrCreate()
    val df = sql.read.json(inputFile)

    println("(1) 查询所有数据；")
    df.show()

    println("(2) 查询所有数据，并去除重复的数据；")
    df.distinct().show()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          

    println("(3) 查询所有数据，打印时去除id字段；")
    df.drop("id").show()

    println("(4) 筛选出age>30的记录；")
    df.filter("age > 30").show()

    println("(5) 将数据按age分组；")
    df.groupBy(df("age")).count().show()

    println("(6) 将数据按name升序排列；")
    df.orderBy("name").show()

    println("(7) 取出前3行数据；")
    df.take(3)
    df.head(3)
    df.show(3)

    println("(8) 查询所有记录的name列，并为其取别名为username；")
    df.selectExpr("name as username").show()

    println("(9) 查询年龄age的平均值；")
    df.agg("age" -> "avg").show()

    println("(10) 查询年龄age的最小值。")
    df.agg("age" -> "min").show()

  }
}
