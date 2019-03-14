package com.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import kafka.message.MessageAndMetadata
import kafka.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._


object DirectKafakExample {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      println("Hello world")

      //参数获取
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "neo-test-01:9092,neo-test-02:9092,neo-test-03:9092,neo-test-04:9092,neo-test-05:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "consume_group",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )


      val topic: String = "test1"
      val topics: Set[String] = Set(topic)


      //初始化驱动
      val conf = new SparkConf().setMaster("local[8]").setAppName("direct example")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))


      //保存偏移量
      //获取zk下该消费者的offset存储路径,一般该路径是/consumers/test_spark_streaming_group/offsets/topic_name
      val topicDirs = new ZKGroupTopicDirs("consume_group", topic)
      //val zkClient = new ZkClient("neo-test-01:2181,neo-test-02:2181,neo-test-03:2181,neo-test-04:2181,neo-test-05:2181")
      val zkClient = ZkUtils.createZkClient("neo-test-01:2181,neo-test-02:2181,neo-test-03:2181,neo-test-04:2181,neo-test-05:2181", 60000, 60000)
      val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")


      //var fromOffsets: Map[TopicAndPartition, Long] = Map()

      var fromOffsets: Map[TopicPartition, Long] = Map()

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

      var stream: InputDStream[ConsumerRecord[String, String]] = null
      if (children > 0) {


        for (i <- 0 until children) {
          // 表示字符串的拼接
          val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/$i")
          val tp = new TopicPartition(topic, i)
          fromOffsets += (tp -> partitionOffset.toLong)

        }
        stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )
      }
      else {
        stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )

      }

      var offSetRanges = Array[OffsetRange]()
      //处理数据
      stream.foreachRDD(rdd => {
        rdd.foreachPartition(aa => {
          aa.take(5).foreach(println)


        })
        //处理成功后获取偏移量，并保存进zk中
        offSetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offSetRanges) {

          println(o.fromOffset.toString)
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //保存偏移量到ZK
          val zkConnection = ZkUtils.createZkClientAndConnection("neo-test-01:2181,neo-test-02:2181,neo-test-03:2181,neo-test-04:2181,neo-test-05:2181", 60000, 60000)
          val zkUtils = new ZkUtils(zkConnection._1, zkConnection._2, false)
          println(zkPath, o.fromOffset.toString)
          zkUtils.updatePersistentPath(zkPath, o.fromOffset.toString)

        }

      })

      // 提交kafka偏移量信息
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offSetRanges)


      //启动程序
      //ssc.checkpoint("/tmp/checkpoint")
      ssc.start()
      ssc.awaitTermination()

    }
  }
}
