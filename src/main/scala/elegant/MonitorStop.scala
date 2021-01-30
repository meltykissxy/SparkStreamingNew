package elegant

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

import java.net.URI

class MonitorStop(ssc: StreamingContext) extends Runnable {

    override def run(): Unit = {

        val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu")

        while (true) {
            try
                Thread.sleep(5000)
            catch {
                case e: InterruptedException =>
                    e.printStackTrace()
            }
            val state: StreamingContextState = ssc.getState

            val bool: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/stopSpark"))

            if (bool) {
                if (state == StreamingContextState.ACTIVE) {
                    ssc.stop(stopSparkContext = true, stopGracefully = true)
                    System.exit(0) //将当前的这条线程都停掉
                }
            }
        }
    }
}
