package eu.dnetlib.doiboost.uw

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.crossref.SparkMapDumpIntoOAF
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object SparkMapUnpayWallToOAF {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        SparkMapDumpIntoOAF.getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/doiboost/convert_uw_to_oaf_params.json"
        )
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]

    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")
    val inputRDD: RDD[String] = spark.sparkContext.textFile(s"$sourcePath")

    logger.info("Converting UnpayWall to OAF")

    val d: Dataset[Publication] = spark
      .createDataset(inputRDD.map(UnpayWallToOAF.convertToOAF).filter(p => p != null))
      .as[Publication]
    d.write.mode(SaveMode.Overwrite).save(targetPath)
  }

}
