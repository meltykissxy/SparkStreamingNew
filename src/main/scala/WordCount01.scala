import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用netcat工具向9999端口不断的发送数据，通过SparkStreaming读取端口数据并统计不同单词出现的次数
 */

object WordCount01 {
    def main(args: Array[String]): Unit = {
        //1.初始化Spark配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

        //2.初始化SparkStreamingContext。这里的3s指的是3s采集一次数据，不是3s产生一个rdd。仅仅是因为窗口大小默认相同而已。一个窗口一个rdd，切不可理解为一个采集时间一个rdd
        // 既然有Seconds，就表示还有Minutes等的哦，它们是Duration更具体的实现
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.通过监控端口创建DStream，读进来的数据为一行行
        // DStream内置的Stream很少，基本都需要自己定义
        val lineStreams = ssc.socketTextStream("hadoop102", 9999)
        //val lineStreams = ssc.textFileStream("/Users/apple/Downloads/lovely")//监控目录，但是做的很烂，不推荐使用。Spark视频159

        //将每一行数据做切分，形成一个个单词
        val wordStreams = lineStreams.flatMap(_.split(" "))

        //将单词映射成元组（word,1）
        val wordAndOneStreams = wordStreams.map((_, 1))

        //将相同的单词次数做统计
        val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

        //打印
        wordAndCountStreams.print()

        //启动SparkStreamingContext
        //启动数据采集器
        ssc.start()
        // 这里用while (true)不靠谱
        // awaitTermination阻塞当前线程直到采集器停止
        ssc.awaitTermination()
    }
}
