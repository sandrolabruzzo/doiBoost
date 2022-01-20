package eu.dnetlib.doiboost.uw

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import eu.dnetlib.dhp.schema.oaf.{AccessRight, Instance, OpenAccessRoute, Publication}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class OALocation(
  evidence: Option[String],
  host_type: Option[String],
  is_best: Option[Boolean],
  license: Option[String],
  pmh_id: Option[String],
  updated: Option[String],
  url: Option[String],
  url_for_landing_page: Option[String],
  url_for_pdf: Option[String],
  version: Option[String]
) {}

object UnpayWallToOAF {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def get_unpaywall_color(input: String): Option[OpenAccessRoute] = {
    if (input == null || input.equalsIgnoreCase("close"))
      return None
    if (input.equalsIgnoreCase("green"))
      return Some(OpenAccessRoute.green)
    if (input.equalsIgnoreCase("bronze"))
      return Some(OpenAccessRoute.bronze)
    if (input.equalsIgnoreCase("hybrid"))
      return Some(OpenAccessRoute.hybrid)
    else
      return Some(OpenAccessRoute.gold)

  }

  def get_color(
    is_oa: Boolean,
    location: OALocation,
    journal_is_oa: Boolean
  ): Option[OpenAccessRoute] = {
    if (is_oa) {
      if (location.host_type.isDefined) {
        {
          if (location.host_type.get.equalsIgnoreCase("repository"))
            return Some(OpenAccessRoute.green)
          else if (location.host_type.get.equalsIgnoreCase("publisher")) {
            if (journal_is_oa)
              return Some(OpenAccessRoute.gold)
            else {
              if (location.license.isDefined)
                return Some(OpenAccessRoute.hybrid)
              else
                return Some(OpenAccessRoute.bronze)
            }

          }
        }

      }
    }
    None
  }

  def convertToOAF(input: String): Publication = {
    val pub = new Publication

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    val doi = DoiBoostMappingUtil.normalizeDoi((json \ "doi").extract[String])

    if (doi == null)
      return null

    val is_oa = (json \ "is_oa").extract[Boolean]

    val journal_is_oa = (json \ "journal_is_oa").extract[Boolean]

    val oaLocation: OALocation = (json \ "best_oa_location").extractOrElse[OALocation](null)

    val colour = get_unpaywall_color((json \ "oa_status").extractOrElse[String](null))

    pub.setCollectedfrom(List(createUnpayWallCollectedFrom()).asJava)
    pub.setDataInfo(generateDataInfo())

    if (!is_oa)
      return null

    if (oaLocation == null || oaLocation.url.isEmpty)
      return null
    val i: Instance = new Instance()

    i.setCollectedfrom(createUnpayWallCollectedFrom())
//    i.setAccessright(getOpenAccessQualifier())
    i.setUrl(List(oaLocation.url.get).asJava)

    if (oaLocation.license.isDefined)
      i.setLicense(asField(oaLocation.license.get))
    pub.setPid(List(createSP(doi, "doi", ModelConstants.DNET_PID_TYPES)).asJava)

    // Ticket #6282 Adding open Access Colour
    if (colour.isDefined) {
      val a = new AccessRight
      a.setClassid(ModelConstants.ACCESS_RIGHT_OPEN)
      a.setClassname(ModelConstants.ACCESS_RIGHT_OPEN)
      a.setSchemeid(ModelConstants.DNET_ACCESS_MODES)
      a.setSchemename(ModelConstants.DNET_ACCESS_MODES)
      a.setOpenAccessRoute(colour.get)
      i.setAccessright(a)
      i.setPid(List(createSP(doi, "doi", ModelConstants.DNET_PID_TYPES)).asJava)
    }
    pub.setInstance(List(i).asJava)

    //IMPORTANT
    //The old method pub.setId(IdentifierFactory.createIdentifier(pub))
    //will be replaced using IdentifierFactory
    //pub.setId(generateIdentifier(pub, doi.toLowerCase))
    pub.setId(IdentifierFactory.createDOIBoostIdentifier(pub))
    pub

  }

}
