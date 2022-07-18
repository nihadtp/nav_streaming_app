package org.mutualFund

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.mutualFund.dataStructure.{InputMessage, KafkaMessage, Message}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.mutualFund.utils.argsUtil.getNameSpace
import org.mutualFund.utils.columnUtil
import java.io.File
import com.typesafe.config.{ConfigFactory}

object App extends Serializable {

  val logger = Logger.getLogger(App.getClass)
  logger.setLevel(Level.INFO)
  final val GROUP_ID = "group_id"
  final val OFFSET_RESET = "offset_reset"
  final val MAX_PARTITION_FETCH = "max_partition_fetch"
  final val INPUT_TOPIC = "input_topic"
  final val ERROR_TOPIC = "error_topic"
  final val INPUT_BROKER = "input_broker"
  final val OUTPUT_BROKER = "output_broker"
  final val OUTPUT_PATH = "output_path"
  final val OUTPUT_TABLE = "output_table"
  final val SSC_DURATION = "ssc_duration"

  def main(args: Array[String]): Unit = {
    logger.info("Starting the Application...")
    //For fetching values from command line arguments.
    val ns = getNameSpace(args)
    var ns_path: Array[AnyRef] = null
    try {
      ns_path = ns.getList[String]("config").toArray
    } catch {
      case e: NullPointerException =>
        println("-- config property missing.")
        System.exit(0)
    }
    val path = ns_path(0).asInstanceOf[String]

    //Fetch configuration parameters from the .conf file.
    val config = ConfigFactory.parseFile(new File(path))

    val group_id = config.getString(GROUP_ID)
    val offset_reset = config.getString(OFFSET_RESET)
    val max_partition_fetch = config.getString(MAX_PARTITION_FETCH)
    val input_topic = config.getString(INPUT_TOPIC)
    val error_topic = config.getString(ERROR_TOPIC)
    val input_broker = config.getString(INPUT_BROKER)
    val output_broker = config.getString(OUTPUT_BROKER)
    val output_path = config.getString(OUTPUT_PATH)
    val output_table = config.getString(OUTPUT_TABLE)
    val ssc_duration = config.getInt(SSC_DURATION)

    //Setting up spark configurations. Al configurations are picked from conf/spark-default.conf file in spark directory
    val conf = new SparkConf()
      .setAppName("NVA")
    /*
    Setting up streaming context. This will fetch micro batch of streaming data every 10 seconds.
    Producer sends 3-8 records every second. Hence 10 seconds would process the streaming data in much more stable
    condition without any scheduling delays.
     */
    val ssc = new StreamingContext(conf, Seconds(ssc_duration))
    ssc.sparkContext.setLogLevel("ERROR")

    //Kafka Parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> input_broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> offset_reset,
      "group.id" -> group_id,
      "max.partition.fetch.bytes" -> max_partition_fetch,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(input_topic)

    //Input stream from Kafka Consumer.
    val stream = KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

    /** For each record, we check if the data conforms to the schema we expect from Mutual Fund API.
      * If schema matches then we convert the streaming to Dataframe Table, partition accordingly and write as
      * Parquet to HDFS.
      * If schema check fails then we serialize the invalid data and send to an error topic via kafka
      * spark producer. Any application down the pipeline could easily ingest those data from the relevant
      * topic and do analysis.
      * -------------------------------------------------------------------------------------------------------------
      * Kafka offsets are committed manually by the application. Offset commitment is asynchronous for
      * performance reasons.
      * Kafka offsets are retrieved at start of the micro-batches and are committed when all the
      * relevant processing are successful ie -
      * When data is successfully written to HDFS or error data is successfully written to kafka topic.
      */
    stream.foreachRDD(rdd => {
      // Retrieve current offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val spark =
        SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val input_rdd = rdd
        .mapPartitions(partition => {
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          mapper
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
          partition.flatMap(message => {
            val data = message.value()
            try {
              logger.info(
                f"Attempting to deserialize \n ${data.hashCode()} (hash of the data) \n  to schema."
              )
              val m: Message = mapper.readValue(data, classOf[Message])
              logger.info(
                f"Deserialization successful Hash of the message -> \n ${m.hashCode()}"
              )
              m.body.payload.map(payload =>
                InputMessage(true, payload = payload)
              )
            } catch {
              case e: Exception =>
                logger.warn(
                  f"Deserialization failed for the data \n ${data.hashCode()}. \n Will be sending to the error topic for consumption"
                )
                Array(InputMessage(false, garbage = data, exception = e))
            }
          })
        })
        .cache() // Since above RDD is reused multiple times, it is cached, to prevent re-computation everytime it is accessed

      val valid_ds =
        input_rdd.filter(_.isValid).map(_.payload).toDS() // Valid Dataset

      //Invalid dataframe
      val invalid_df = input_rdd
        .filter(!_.isValid)
        .mapPartitions(partition => {
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          mapper
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
          partition.map(message => {
            val invalid_data = message.garbage
            val exception = message.exception.getMessage
            KafkaMessage(invalid_data, exception, mapper, error_topic)
          })
        })
        .toDF()

      // Writing invalid records to error topic via Kafka.
      invalid_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", output_broker)
        .save()

      //Extracting Year, Month and Day from 'Date' field of valid data, so as to use them for partitioning.
      val output_df = valid_ds
        .withColumn("Date", to_date(col("Date"), "dd-MMM-yyyy"))
        .withColumn("Year", year(col("Date")))
        .withColumn("Month", month(col("Date")))
        .withColumn("Day", dayofmonth(col("Date")))

      /*
      Spark external table/Hive requires the table name and column names to be strictly alpha-numeric and underscore. Incoming Mutual Fund
      data contains field names that contain white space which does not bode well with Spark. Hence we replace the
      white space with underscore using a utility method.
       */
      val cleaned_cols =
        columnUtil.rename(output_df)

      /*
      According to problem statement, analyst should be able to query efficiently through Mutual Fund and Date.
      Hence we partition as Mutual Fund, Year, Month and Day.
      Parquet is used due to its column based storage. Since this is an OLAP transaction and due to its query requirement
      column based storage would be efficient.
       */
      logger.info("Writing Valid data to HDFS")
      cleaned_cols.write
        .format("parquet")
        .mode(SaveMode.Append)
        .option("path", output_path)
        .partitionBy(
          "Mutual_Fund_Family",
          "Year",
          "Month",
          "Day"
        ) // Partitioning
        .saveAsTable(output_table)
      logger.info("Write successful and committing the offset.")
      stream
        .asInstanceOf[CanCommitOffsets]
        .commitAsync(offsetRanges) // Committing the offset back to kafka
    })

    ssc.start()
    ssc.awaitTermination() // Run forever until manually shutdown

  }
}
