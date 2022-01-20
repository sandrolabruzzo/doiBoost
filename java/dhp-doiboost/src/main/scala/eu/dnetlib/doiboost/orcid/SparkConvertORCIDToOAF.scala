package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object SparkConvertORCIDToOAF {
  val logger: Logger = LoggerFactory.getLogger(SparkConvertORCIDToOAF.getClass)

  def run(spark: SparkSession, workingPath: String, targetPath: String): Unit = {
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
    import spark.implicits._
    val dataset: Dataset[ORCIDItem] =
      spark.read.load(s"$workingPath/orcidworksWithAuthor").as[ORCIDItem]

    logger.info("Converting ORCID to OAF")
    dataset.map(o => ORCIDToOAF.convertTOOAF(o)).write.mode(SaveMode.Overwrite).save(targetPath)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        SparkConvertORCIDToOAF.getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/doiboost/convert_orcid_to_oaf_params.json"
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

    val workingPath = parser.get("workingPath")
    val targetPath = parser.get("targetPath")

    run(spark, workingPath, targetPath)

  }

}
