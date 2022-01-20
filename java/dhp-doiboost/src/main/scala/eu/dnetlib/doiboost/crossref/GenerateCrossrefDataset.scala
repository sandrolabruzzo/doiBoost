package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object GenerateCrossrefDataset {

  val log: Logger = LoggerFactory.getLogger(GenerateCrossrefDataset.getClass)

  implicit val mrEncoder: Encoder[CrossrefDT] = Encoders.kryo[CrossrefDT]

  def crossrefElement(meta: String): CrossrefDT = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(meta)
    val doi: String = DoiBoostMappingUtil.normalizeDoi((json \ "DOI").extract[String])
    val timestamp: Long = (json \ "indexed" \ "timestamp").extract[Long]
    CrossrefDT(doi, meta, timestamp)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(
      Source
        .fromInputStream(
          getClass.getResourceAsStream(
            "/eu/dnetlib/dhp/doiboost/crossref_dump_reader/generate_dataset_params.json"
          )
        )
        .mkString
    )
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName(UnpackCrtossrefEntries.getClass.getSimpleName)
      .master(master)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val tmp: RDD[String] = sc.textFile(sourcePath, 6000)

    spark
      .createDataset(tmp)
      .map(entry => crossrefElement(entry))
      .write
      .mode(SaveMode.Overwrite)
      .save(targetPath)
    //               .map(meta => crossrefElement(meta))
    //               .toDS.as[CrossrefDT]
    //              .write.mode(SaveMode.Overwrite).save(targetPath)

  }

}
