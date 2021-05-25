package com.nwcd.bigdata

import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.search.aggregations.support.format.ValueParser.DateTime
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._


object RealTimeETLApp {
  // CSV文件有一个标题行需要过滤掉，以免影响后续分析,以下是判断首行
  def isHeader(line: String): Boolean = {
    line.contains("VendorID")
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic_get = "get-s3data"
    val topic_put = "put-s3data"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val serialization = "org.apache.kafka.common.serialization.StringSerializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", brokers)
        p.setProperty("key.serializer", serialization)
        p.setProperty("value.serializer", serialization)
        p
      }
      println("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    // 配置sparkSession
    val sparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.setMaxpartitions(sparkSession) //设置最大分区数
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

    //创建DS
    val getkafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic_get))
//    val producer: KafkaProducer[String, String] = createKafkaProducer

    //原始数据 msg
//    num 1:VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
//    num 1:1,2020-01-01 00:28:15,2020-01-01 00:33:03,1,1.20,1,N,238,239,1,6,3,0.5,1.47,0,0.3,11.27,2.5
//    num 1:1,2020-01-01 00:35:39,2020-01-01 00:43:04,1,1.20,1,N,239,238,1,7,3,0.5,1.5,0,0.3,12.3,2.5
//    ……
    //从kafka的kv值中取value即上述msg
    val dataDS: DStream[String] = getkafkaDS.map(_._2)

    // 数据清洗
    val cleanData: DStream[String] = dataDS.map ({
      line => {
        val fields: Array[String] = line.split(",")

        var vendorID = fields(0)
        var puDatetime = fields(1)
        var doDatetime = fields(2)
        var distance = fields(4)
        var puLID = fields(7)
        var doLID = fields(8)

        (vendorID + "," + puDatetime + "," + doDatetime + "," + distance + "," + puLID + "," + doLID)
      }
    }).filter(!isHeader(_))

    // 结构转换 => vendorID，接客时间，落客时间，行驶时间（分钟），行驶距离，平均速度（英里/小时），接客地点，落客地点
    val mapDS: DStream[NycTlcInfo] = cleanData.map{
      line => {
        val fields: Array[String] = line.split(",")

        var dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        var puDT: Date = dateFormat.parse(fields(1))
        var doDT: Date = dateFormat.parse(fields(2))
        var diffDT: Int = ((doDT.getTime - puDT.getTime) / (1000 * 60)).asInstanceOf[Int]
        var distance: Float = fields(3).toFloat
        var avgSpeed: Float = if(diffDT == 0) 0 else distance/diffDT*60

        val a = new Date().getTime
        var str = a+""
        val ts:Long = str.substring(0,10).toLong

        NycTlcInfo(vendorID=fields(0),
          puDT=puDT,
          doDT=doDT,
          diffDT=diffDT,
          distance=distance,
          avgSpeed=avgSpeed,
          puLID=fields(4),
          doLID=fields(5),
          ts)
      }
    }

    //输出到kafka再写入S3
    mapDS.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.foreach(line => {
          kafkaProducer.value.send(topic_put,
            line.vendorID.toString+","+
              line.puDT.toString+","+
              line.doDT.toString+","+
              line.diffDT.toString+","+
              line.distance.toString+","+
              line.avgSpeed.toString+","+
              line.puLID+","+
              line.doLID)
          // do something else
        })
      }
    })

    // 引入对象实例中的隐式转换
    import sparkSession.implicits._
    // 写入hive
    val dataframeData = mapDS
      .foreachRDD(
        rdd =>
          rdd
            .toDF("vendorID", "puDT", "doDT", "diffDT", "distance", "avgSpeed", "puLID", "doLID", "ts")
            .coalesce(1)
            .write
            .mode(SaveMode.Append)
            .insertInto("SparkStreaming2Hive")
      )

    sparkSession.sql("select * from SparkStreaming2Hive").show()
    sparkSession.sql("insert into SparkStreaming2Hive select * from tmptable")

    // 写入ES
    val startLogInfoDStream: DStream[JSONObject] = mapDS.map { line =>
      val content = Serialization.write(line)(DefaultFormats)
      val startupJSONObj: JSONObject = JSON.parseObject(content)
      val ts: java.lang.Long = startupJSONObj.getLong("ts")
      startupJSONObj
    }

    startLogInfoDStream.foreachRDD{rdd=>
      rdd.foreachPartition{tlcInfoItr=>
        val tlcInfoWithIdList: List[JSONObject] = tlcInfoItr.toList.map(tlcInfo=>(tlcInfo))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        EsUtil.bulkDoc(tlcInfoWithIdList,"nyc01_tlc_info_"+dateStr)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

