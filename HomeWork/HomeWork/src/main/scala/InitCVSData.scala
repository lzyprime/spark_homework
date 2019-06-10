import java.io.PrintWriter

import scala.io.Source
import scala.util.Random

object InitCVSData {
  def main(args: Array[String]): Unit = {
    val province = Seq(
      "北京市",
      "天津市",
      "上海市",
      "重庆市",
      "河北省",
      "山西省",
      "辽宁省",
      "吉林省",
      "黑龙江省",
      "江苏省",
      "浙江省",
      "安徽省",
      "福建省",
      "江西省",
      "山东省",
      "河南省",
      "湖北省",
      "湖南省",
      "广东省",
      "海南省",
      "四川省",
      "贵州省",
      "云南省",
      "陕西省",
      "甘肃省",
      "青海省",
      "台湾省",
      "内蒙古自治区",
      "广西壮族自治区",
      "西藏自治区",
      "宁夏回族自治区",
      "新疆维吾尔自治区",
      "香港特别行政区",
      "澳门特别行政区"
    )

    val inputFile = "/home/prime/Documents/Spark/spark_code/HomeWork/sale_user.csv"
    val outputFile = "/home/prime/Documents/Spark/spark_code/HomeWork/final_user.csv"
    val pr = new PrintWriter(outputFile)

    val file = Source.fromFile(inputFile)
    file.getLines().toList.foreach(i => pr.println(i + "," + province(Random.nextInt(province.size))))
    pr.close()
    file.close()
  }

}
