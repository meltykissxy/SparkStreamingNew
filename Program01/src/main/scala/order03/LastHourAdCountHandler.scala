package order03

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream


object LastHourAdCountHandler {

    //时间格式化对象
    private val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm")

    /**
     * 统计最近一小时(2分钟)广告分时点击总数
     *
     * @param filterAdsLogDStream 过滤后的数据集
     * @return
     */
    def getAdHourMintToCount(filterAdsLogDStream: DStream[Ads_log]): DStream[(String, List[(String, Long)])] = {

        //1.开窗 => 时间间隔为1个小时 window()
        val windowAdsLogDStream: DStream[Ads_log] = filterAdsLogDStream.window(Minutes(2))

        //2.转换数据结构 ads_log =>((adid,hm),1L) map()
        val adHmToOneDStream: DStream[((String, String), Long)] = windowAdsLogDStream.map(adsLog => {

            val timestamp: Long = adsLog.timestamp
            val hm: String = sdf.format(new Date(timestamp))

            ((adsLog.adid, hm), 1L)
        })

        //3.统计总数 ((adid,hm),1L)=>((adid,hm),sum) reduceBykey(_+_)
        val adHmToCountDStream: DStream[((String, String), Long)] = adHmToOneDStream.reduceByKey(_ + _)

        //4.转换数据结构 ((adid,hm),sum)=>(adid,(hm,sum)) map()
        val adToHmCountDStream: DStream[(String, (String, Long))] = adHmToCountDStream.map { case ((adid, hm), count) =>
            (adid, (hm, count))
        }

        //5.按照adid分组 (adid,(hm,sum))=>(adid,Iter[(hm,sum),...]) groupByKey
        adToHmCountDStream.groupByKey()
            .mapValues(iter =>
                iter.toList.sortWith(_._1 < _._1)
            )
    }

}

