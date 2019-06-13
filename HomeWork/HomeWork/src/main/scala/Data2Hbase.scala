import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableReducer}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

class HbaseMapper extends Mapper[LongWritable, Text, Text, Text] {
  @throws[IOException]
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val keyValue = value.toString.split(",")
    context.write(new Text(keyValue.head),
      new Text(keyValue.tail.reduce(_ + "," + _)))
  }
}

class HbaseReducer extends TableReducer[Text, Text, ImmutableBytesWritable] {


  @throws[IOException]
  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, ImmutableBytesWritable, Mutation]#Context)
  : Unit = {
    import scala.collection.JavaConverters._

    val k = key.toString
    values.asScala.toList.foreach { self =>
      val put = new Put(k.getBytes())
      val value = self.toString.split(",")
      val finalUser =  Seq("user_id", "item_id", "behavior_type", "item_category", "time", "province") zip Seq(k, value(0), value(1), value(2), value(3), value(4))

      finalUser.tail.foreach(self => put.addColumn("data".getBytes(), self._1.getBytes(), self._2.getBytes()))
      context.write(new ImmutableBytesWritable(k.getBytes()), put)
    }
  }


}

object Data2Hbase {
  def main(args: Array[String]): Unit = {
    val input = "file:///home/prime/Documents/Spark/spark_code/HomeWork/final_user.csv"
    val output = "hdfs://localhost:9000/hbase"
    val table = "hb_action_detail"

    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf)
    job setJarByClass Data2Hbase.getClass
    job setMapperClass classOf[HbaseMapper]
    job setReducerClass classOf[HbaseReducer]
    job setMapOutputKeyClass classOf[Text]
    job setMapOutputValueClass classOf[Text]

    job setOutputKeyClass classOf[ImmutableBytesWritable]
    job setOutputValueClass classOf[Put]
    job setInputFormatClass classOf[TextInputFormat]
    job setOutputFormatClass classOf[TableOutputFormat[Put]]
    FileInputFormat addInputPath (job, new Path(input))
    println(job waitForCompletion true)

  }
}
