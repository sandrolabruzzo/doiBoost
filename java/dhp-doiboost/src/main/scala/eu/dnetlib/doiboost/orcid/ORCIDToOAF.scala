package eu.dnetlib.doiboost.orcid

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Publication}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil.{createSP, generateDataInfo}
import org.apache.commons.lang.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class ORCIDItem(doi: String, authors: List[OrcidAuthor]) {}

case class OrcidAuthor(
  oid: String,
  name: Option[String],
  surname: Option[String],
  creditName: Option[String],
  otherNames: Option[List[String]],
  errorCode: Option[String]
) {}
case class OrcidWork(oid: String, doi: String)

case class ORCIDElement(doi: String, authors: List[ORCIDItem]) {}

object ORCIDToOAF {
  val logger: Logger = LoggerFactory.getLogger(ORCIDToOAF.getClass)
  val mapper = new ObjectMapper()

  def isJsonValid(inputStr: String): Boolean = {
    import java.io.IOException
    try {
      mapper.readTree(inputStr)
      true
    } catch {
      case e: IOException =>
        false
    }
  }

  def extractValueFromInputString(input: String): (String, String) = {
    val i = input.indexOf('[')
    if (i < 5) {
      return null
    }
    val orcidList = input.substring(i, input.length - 1)
    val doi = input.substring(1, i - 1)
    if (isJsonValid(orcidList)) {
      (doi, orcidList)
    } else null
  }

  def strValid(s: Option[String]): Boolean = {
    s.isDefined && s.get.nonEmpty
  }

  def authorValid(author: OrcidAuthor): Boolean = {
    if (strValid(author.name) && strValid(author.surname)) {
      return true
    }
    if (strValid(author.surname)) {
      return true
    }
    if (strValid(author.creditName)) {
      return true

    }
    false
  }

  def extractDOIWorks(input: String): List[OrcidWork] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    val oid = (json \ "workDetail" \ "oid").extractOrElse[String](null)
    if (oid == null)
      return List()
    val doi: List[(String, String)] = for {
      JObject(extIds)                    <- json \ "workDetail" \ "extIds"
      JField("type", JString(typeValue)) <- extIds
      JField("value", JString(value))    <- extIds
      if "doi".equalsIgnoreCase(typeValue)
    } yield (typeValue, DoiBoostMappingUtil.normalizeDoi(value))
    if (doi.nonEmpty) {
      return doi.map(l => OrcidWork(oid, l._2))
    }
    List()
  }

  def convertORCIDAuthor(input: String): OrcidAuthor = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    (json \ "authorData").extractOrElse[OrcidAuthor](null)
  }

  def convertTOOAF(input: ORCIDItem): Publication = {
    val doi = input.doi
    val pub: Publication = new Publication
    pub.setPid(List(createSP(doi, "doi", ModelConstants.DNET_PID_TYPES)).asJava)
    pub.setDataInfo(generateDataInfo())

    pub.setId(IdentifierFactory.createDOIBoostIdentifier(pub))
    if (pub.getId == null)
      return null

    try {

      val l: List[Author] = input.authors.map(a => {
        generateAuthor(a)
      })(collection.breakOut)

      pub.setAuthor(l.asJava)
      pub.setCollectedfrom(List(DoiBoostMappingUtil.createORIDCollectedFrom()).asJava)
      pub.setDataInfo(DoiBoostMappingUtil.generateDataInfo())
      pub
    } catch {
      case e: Throwable =>
        logger.info(s"ERROR ON GENERATE Publication from $input")
        null
    }
  }

  def generateOricPIDDatainfo(): DataInfo = {
    val di = DoiBoostMappingUtil.generateDataInfo("0.91")
    di.getProvenanceaction.setClassid(ModelConstants.SYSIMPORT_CROSSWALK_ENTITYREGISTRY)
    di.getProvenanceaction.setClassname(ModelConstants.HARVESTED)
    di
  }

  def generateAuthor(o: OrcidAuthor): Author = {
    val a = new Author
    if (strValid(o.name)) {
      a.setName(o.name.get.capitalize)
    }
    if (strValid(o.surname)) {
      a.setSurname(o.surname.get.capitalize)
    }
    if (strValid(o.name) && strValid(o.surname))
      a.setFullname(s"${o.name.get.capitalize} ${o.surname.get.capitalize}")
    else if (strValid(o.creditName))
      a.setFullname(o.creditName.get)
    if (StringUtils.isNotBlank(o.oid))
      a.setPid(
        List(
          createSP(
            o.oid,
            ModelConstants.ORCID,
            ModelConstants.DNET_PID_TYPES,
            generateOricPIDDatainfo()
          )
        ).asJava
      )

    a
  }

}
