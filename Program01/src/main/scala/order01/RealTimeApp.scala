package order01

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {

    def main(args: Array[String]): Unit = {

        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp ").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.读取数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)

        //4.将从Kafka读出的数据转换为样例类对象
        val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(record => {
            val value: String = record.value()
            val arr: Array[String] = value.split(" ")
            Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
        })

        //5.需求一：根据MySQL中的黑名单过滤当前数据集
        val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler2.filterByBlackList(adsLogDStream)

        //6.需求一：将满足要求的用户写入黑名单
        BlackListHandler2.addBlackList(filterAdsLogDStream)

        //测试打印
        filterAdsLogDStream.cache()
        filterAdsLogDStream.count().print()

        //启动任务
        ssc.start()
        ssc.awaitTermination()
    }

}
