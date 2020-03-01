package com.bigdata.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io

object ProducerKafkaRandom {


  def main(args: Array[String]): Unit = {

    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    var count = 0;
    var lineCount = 0;

    while (count < 10000) {
      val record = getRecord();
      lineCount += 1
      val timestamp: Long = System.currentTimeMillis / 1000
      var recordModified = "";
      if (lineCount == 1) {
        recordModified = "id" + "," + record
      }
      else {
        recordModified = timestamp + "" + lineCount + "" + count + "," + record
      }

      val information = new ProducerRecord[String, String]("testing", "key", recordModified)
      println(recordModified)
      producer.send(information)
      count = count + 1;
    }

    println("send")
    producer.close()

  }

  def getRecord(): String = {
    val bufferedSource = io.Source.fromFile("/Users/mahesh/Desktop/PBDA/Project/Life Expectancy Data_Copy.csv")
    var matrix: Array[Array[String]] = Array.empty
    var i = 0
    var j = 0
    for (line <- bufferedSource.getLines.drop(1)) {
      var cols = line.split(",").map(_.trim.toString())
      if (cols.length < 22) {
        val s = cols.length + 1
        for (x <- s to 22) {
          cols :+= ""
        }
      }
      matrix = matrix :+ cols
    }
    var rows = matrix.length
    var cols = matrix(0).length
    val max = rows - 1
    val min = 0
    var record = ""
    for (j <- 0 to cols - 1) {
      val random = new scala.util.Random
      val i = min + random.nextInt((max - min) + 1)
      val colEle = matrix(i)(j)
      record = record + colEle + ','
    }
    record = record.substring(0, record.length - 1)
    //println(record)
    bufferedSource.close()
    return record
  }
}
