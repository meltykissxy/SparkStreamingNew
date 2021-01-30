import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transform {
    def main(args: Array[String]): Unit = {

        //创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        //创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //创建DStream
        val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

        //转换为RDD操作
        val wordAndCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {

            val words: RDD[String] = rdd.flatMap(_.split(" "))

            val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

            val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

            value
        })

        //打印
        wordAndCountDStream.print

        //启动
        ssc.start()
        ssc.awaitTermination()

    }

}
