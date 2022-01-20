package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object SparkPreprocessORCID {
  val logger: Logger = LoggerFactory.getLogger(SparkConvertORCIDToOAF.getClass)

  def fixORCIDItem(item: ORCIDItem): ORCIDItem = {
    ORCIDItem(item.doi, item.authors.groupBy(_.oid).map(_._2.head).toList)

  }

  def run(spark: SparkSession, sourcePath: String, workingPath: String): Unit = {
    import spark.implicits._
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]

    val inputRDD: RDD[OrcidAuthor] = spark.sparkContext
      .textFile(s"$sourcePath/authors")
      .map(s => ORCIDToOAF.convertORCIDAuthor(s))
      .filter(s => s != null)
      .filter(s => ORCIDToOAF.authorValid(s))

    spark
      .createDataset(inputRDD)
      .as[OrcidAuthor]
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/author")

    val res = spark.sparkContext
      .textFile(s"$sourcePath/works")
      .flatMap(s => ORCIDToOAF.extractDOIWorks(s))
      .filter(s => s != null)

    spark
      .createDataset(res)
      .as[OrcidWork]
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/works")

    val authors: Dataset[OrcidAuthor] = spark.read.load(s"$workingPath/author").as[OrcidAuthor]

    val works: Dataset[OrcidWork] = spark.read.load(s"$workingPath/works").as[OrcidWork]

    works
      .joinWith(authors, authors("oid").equalTo(works("oid")))
      .map(i => {
        val doi = i._1.doi
        val author = i._2
        (doi, author)
      })
      .groupBy(col("_1").alias("doi"))
      .agg(collect_list(col("_2")).alias("authors"))
      .as[ORCIDItem]
      .map(s => fixORCIDItem(s))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/orcidworksWithAuthor")
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        SparkConvertORCIDToOAF.getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/doiboost/preprocess_orcid_params.json"
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

    val sourcePath = parser.get("sourcePath")
    val workingPath = parser.get("workingPath")

    run(spark, sourcePath, workingPath)

  }

}
