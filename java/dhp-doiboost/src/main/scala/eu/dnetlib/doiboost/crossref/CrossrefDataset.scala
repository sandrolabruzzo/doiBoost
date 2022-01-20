package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

object CrossrefDataset {

  val logger: Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)

  def to_item(input: String): CrossrefDT = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val ts: Long = (json \ "indexed" \ "timestamp").extract[Long]
    val doi: String = DoiBoostMappingUtil.normalizeDoi((json \ "DOI").extract[String])
    CrossrefDT(doi, input, ts)

  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        CrossrefDataset.getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/doiboost/crossref_to_dataset_params.json"
        )
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkMapDumpIntoOAF.getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()
    import spark.implicits._

    val crossrefAggregator = new Aggregator[CrossrefDT, CrossrefDT, CrossrefDT] with Serializable {

      override def zero: CrossrefDT = null

      override def reduce(b: CrossrefDT, a: CrossrefDT): CrossrefDT = {
        if (b == null)
          return a
        if (a == null)
          return b

        if (a.timestamp > b.timestamp) {
          return a
        }
        b
      }

      override def merge(a: CrossrefDT, b: CrossrefDT): CrossrefDT = {
        if (b == null)
          return a
        if (a == null)
          return b

        if (a.timestamp > b.timestamp) {
          return a
        }
        b
      }

      override def bufferEncoder: Encoder[CrossrefDT] = implicitly[Encoder[CrossrefDT]]

      override def outputEncoder: Encoder[CrossrefDT] = implicitly[Encoder[CrossrefDT]]

      override def finish(reduction: CrossrefDT): CrossrefDT = reduction
    }

    val workingPath: String = parser.get("workingPath")

    val main_ds: Dataset[CrossrefDT] = spark.read.load(s"$workingPath/crossref_ds").as[CrossrefDT]

    val update =
      spark.createDataset(
        spark.sparkContext
          .sequenceFile(s"$workingPath/index_update", classOf[IntWritable], classOf[Text])
          .map(i => CrossrefImporter.decompressBlob(i._2.toString))
          .map(i => to_item(i))
      )

    main_ds
      .union(update)
      .groupByKey(_.doi)
      .agg(crossrefAggregator.toColumn)
      .map(s => s._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/crossref_ds_updated")

  }

}
