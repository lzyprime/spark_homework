import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


class UserMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  @throws[IOException]
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    context.write(new Text(value.toString.split(",").head), new IntWritable(1))
  }
}

class UserReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  import scala.collection.JavaConverters._
  @throws[IOException]
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    context.write(key, values.asScala.reduce((x1, x2) => new IntWritable(x1.get() + x2.get())))
  }
}

object MapReduce {
  def main(args: Array[String]): Unit = {
    val inputFile = "hdfs://hadoop:9000/user/prime/bigdata/final_user.csv"
    val outputFile = "hdfs://hadoop:9000/user/prime/bigdata/result"

    val job = Job.getInstance()
    job setJobName "Counter"
    job setJarByClass MapReduce.getClass
    job setMapperClass classOf[UserMapper]
    job setReducerClass classOf[UserReducer]
    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[IntWritable]
    FileInputFormat addInputPath(job, new Path(inputFile))
    FileOutputFormat setOutputPath(job, new Path(outputFile))
    job waitForCompletion true
    println("Finished")
  }
}
