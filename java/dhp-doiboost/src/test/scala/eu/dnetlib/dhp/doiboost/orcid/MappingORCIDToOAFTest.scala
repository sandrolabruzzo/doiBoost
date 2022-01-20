package eu.dnetlib.dhp.doiboost.orcid

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.orcid._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Path
import scala.collection.JavaConversions._
import scala.io.Source

class MappingORCIDToOAFTest {
  val logger: Logger = LoggerFactory.getLogger(ORCIDToOAF.getClass)
  val mapper = new ObjectMapper()

  @Test
  def testExtractData(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/orcid/dataOutput"))
      .mkString
    assertNotNull(json)
    assertFalse(json.isEmpty)
    json.lines.foreach(s => {
      assertNotNull(ORCIDToOAF.extractValueFromInputString(s))
    })
  }

  @Test
  def testOAFConvert(@TempDir testDir: Path): Unit = {
    val sourcePath: String = getClass.getResource("/eu/dnetlib/doiboost/orcid/datasets").getPath
    val targetPath: String = s"${testDir.toString}/output/orcidPublication"
    val workingPath = s"${testDir.toString}/wp/"

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
    import spark.implicits._

    SparkPreprocessORCID.run(spark, sourcePath, workingPath)

    SparkConvertORCIDToOAF.run(spark, workingPath, targetPath)

    val mapper = new ObjectMapper()

    val oA = spark.read.load(s"$workingPath/orcidworksWithAuthor").as[ORCIDItem].count()

    val p: Dataset[Publication] = spark.read.load(targetPath).as[Publication]

    assertTrue(oA == p.count())
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p.first()))

    spark.close()

  }

  @Test
  def testExtractDat1(): Unit = {

    val aList: List[OrcidAuthor] = List(
      OrcidAuthor("0000-0002-4335-5309", Some("Lucrecia"), Some("Curto"), null, null, null),
      OrcidAuthor("0000-0001-7501-3330", Some("Emilio"), Some("Malchiodi"), null, null, null),
      OrcidAuthor("0000-0002-5490-9186", Some("Sofia"), Some("Noli Truant"), null, null, null)
    )

    val orcid: ORCIDItem = ORCIDItem("10.1042/BCJ20160876", aList)

    val oaf = ORCIDToOAF.convertTOOAF(orcid)
    assert(oaf.getPid.size() == 1)
    oaf.getPid.toList.foreach(pid => assert(pid.getQualifier.getClassid.equals("doi")))
    oaf.getPid.toList.foreach(pid => assert(pid.getValue.equals("10.1042/BCJ20160876")))
    //println(mapper.writeValueAsString(ORCIDToOAF.convertTOOAF(orcid)))

  }

}
