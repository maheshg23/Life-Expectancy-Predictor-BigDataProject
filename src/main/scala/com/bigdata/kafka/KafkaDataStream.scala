package com.bigdata.kafka

import com.datastax.spark.connector._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._


object KafkaDataStream {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("LifeExpectancy").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val brokers = "localhost:9092";
    val topics = "testing";
    val groupId = "myGrp";

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    println("Stream is Running ")

    val lines = messages.map(_.value) // split the message into lines
      .flatMap(_.split("\n")) //split into words
      .map(_.toString)
      .map(Tuple1(_))
      .map({case (w) => (w)}) //

    //lines.print() //print it so we can see something is happening

    // Save each RDD to the lifeexpectancy table in Cassandra
    lines.foreachRDD(rdd => {
      rdd.saveToCassandra("streamingdata","lifeexpectancy")
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}