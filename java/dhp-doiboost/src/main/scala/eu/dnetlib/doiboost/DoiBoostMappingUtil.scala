package eu.dnetlib.doiboost

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.lang3.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

case class HostedByItemType(
  id: String,
  officialname: String,
  issn: String,
  eissn: String,
  lissn: String,
  openAccess: Boolean
) {}

case class DoiBoostAffiliation(
  PaperId: Long,
  AffiliationId: Long,
  GridId: Option[String],
  OfficialPage: Option[String],
  DisplayName: Option[String]
) {}

object DoiBoostMappingUtil {

  def generateMAGAffiliationId(affId: String): String = {
    s"20|microsoft___$SEPARATOR${DHPUtils.md5(affId)}"
  }

  val logger: Logger = LoggerFactory.getLogger(getClass)

  //STATIC STRING
  val MAG = "microsoft"
  val MAG_NAME = "Microsoft Academic Graph"
  val CROSSREF = "Crossref"
  val UNPAYWALL = "UnpayWall"
  val GRID_AC = "grid.ac"
  val WIKPEDIA = "wikpedia"
  val doiBoostNSPREFIX = "doiboost____"
  val OPENAIRE_PREFIX = "openaire____"
  val SEPARATOR = "::"

  val DOI_PREFIX_REGEX = "(^10\\.|\\/10.)"
  val DOI_PREFIX = "10."

  val invalidName = List(
    ",",
    "none none",
    "none, none",
    "none &na;",
    "(:null)",
    "test test test",
    "test test",
    "test",
    "&na; &na;"
  )

  def toActionSet(item: Oaf): (String, String) = {
    val mapper = new ObjectMapper()

    item match {
      case dataset: Dataset =>
        val a: AtomicAction[Dataset] = new AtomicAction[Dataset]
        a.setClazz(classOf[Dataset])
        a.setPayload(dataset)
        (dataset.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case publication: Publication =>
        val a: AtomicAction[Publication] = new AtomicAction[Publication]
        a.setClazz(classOf[Publication])
        a.setPayload(publication)
        (publication.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case organization: Organization =>
        val a: AtomicAction[Organization] = new AtomicAction[Organization]
        a.setClazz(classOf[Organization])
        a.setPayload(organization)
        (organization.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case relation: Relation =>
        val a: AtomicAction[Relation] = new AtomicAction[Relation]
        a.setClazz(classOf[Relation])
        a.setPayload(relation)
        (relation.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case _ =>
        null
    }

  }

  def toHostedByItem(input: String): (String, HostedByItemType) = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(input)
    val c: Map[String, HostedByItemType] = json.extract[Map[String, HostedByItemType]]
    (c.keys.head, c.values.head)
  }

  def toISSNPair(publication: Publication): (String, Publication) = {
    val issn = if (publication.getJournal == null) null else publication.getJournal.getIssnPrinted
    val eissn = if (publication.getJournal == null) null else publication.getJournal.getIssnOnline
    val lissn = if (publication.getJournal == null) null else publication.getJournal.getIssnLinking

    if (issn != null && issn.nonEmpty)
      (issn, publication)
    else if (eissn != null && eissn.nonEmpty)
      (eissn, publication)
    else if (lissn != null && lissn.nonEmpty)
      (lissn, publication)
    else
      (publication.getId, publication)
  }

  def generateGridAffiliationId(gridId: String): String = {
    s"20|grid________::${DHPUtils.md5(gridId.toLowerCase().trim())}"
  }

  def fixResult(result: Dataset): Dataset = {
    val instanceType = extractInstance(result)
    if (instanceType.isDefined) {
      result.getInstance().asScala.foreach(i => i.setInstancetype(instanceType.get.getInstancetype))
    }
    result
      .getInstance()
      .asScala
      .foreach(i => {
        i.setHostedby(ModelConstants.UNKNOWN_REPOSITORY)
      })
    result
  }

  def decideAccessRight(lic: Field[String], date: String): AccessRight = {
    if (lic == null) {
      //Default value Unknown
      return getUnknownQualifier()
    }
    val license: String = lic.getValue
    //CC licenses
    if (
      license.startsWith("cc") ||
      license.startsWith("http://creativecommons.org/licenses") ||
      license.startsWith("https://creativecommons.org/licenses") ||

      //ACS Publications Author choice licenses (considered OPEN also by Unpaywall)
      license.equals("http://pubs.acs.org/page/policy/authorchoice_ccby_termsofuse.html") ||
      license.equals("http://pubs.acs.org/page/policy/authorchoice_termsofuse.html") ||
      license.equals("http://pubs.acs.org/page/policy/authorchoice_ccbyncnd_termsofuse.html") ||

      //APA (considered OPEN also by Unpaywall)
      license.equals("http://www.apa.org/pubs/journals/resources/open-access.aspx")
    ) {

      val oaq: AccessRight = getOpenAccessQualifier()
      oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
      return oaq
    }

    //OUP (BUT ONLY AFTER 12 MONTHS FROM THE PUBLICATION DATE, OTHERWISE THEY ARE EMBARGOED)
    if (
      license.equals(
        "https://academic.oup.com/journals/pages/open_access/funder_policies/chorus/standard_publication_model"
      )
    ) {
      val now = java.time.LocalDate.now

      try {
        val pub_date = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        if (((now.toEpochDay - pub_date.toEpochDay) / 365.0) > 1) {
          val oaq: AccessRight = getOpenAccessQualifier()
          oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
          return oaq
        } else {
          return getEmbargoedAccessQualifier()
        }
      } catch {
        case e: Exception => {
          try {
            val pub_date =
              LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            if (((now.toEpochDay - pub_date.toEpochDay) / 365.0) > 1) {
              val oaq: AccessRight = getOpenAccessQualifier()
              oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
              return oaq
            } else {
              return getEmbargoedAccessQualifier()
            }
          } catch {
            case ex: Exception => return getClosedAccessQualifier()
          }
        }

      }

    }

    return getClosedAccessQualifier()

  }

  def getOpenAccessQualifier(): AccessRight = {

    OafMapperUtils.accessRight(
      ModelConstants.ACCESS_RIGHT_OPEN,
      "Open Access",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getRestrictedQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      "RESTRICTED",
      "Restricted",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getUnknownQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      ModelConstants.UNKNOWN,
      ModelConstants.NOT_AVAILABLE,
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getEmbargoedAccessQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      "EMBARGO",
      "Embargo",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getClosedAccessQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      "CLOSED",
      "Closed Access",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def extractInstance(r: Result): Option[Instance] = {
    r.getInstance()
      .asScala
      .find(i => i.getInstancetype != null && i.getInstancetype.getClassid.nonEmpty)
  }

  def fixPublication(input: ((String, Publication), (String, HostedByItemType))): Publication = {

    val publication = input._1._2

    val item = if (input._2 != null) input._2._2 else null

    val instanceType: Option[Instance] = extractInstance(publication)

    if (instanceType.isDefined) {
      publication
        .getInstance()
        .asScala
        .foreach(i => i.setInstancetype(instanceType.get.getInstancetype))
    }

    publication
      .getInstance()
      .asScala
      .foreach(i => {
        var hb = new KeyValue
        if (item != null) {
          hb.setValue(item.officialname)
          hb.setKey(item.id)
          if (item.openAccess) {
            i.setAccessright(getOpenAccessQualifier())
            i.getAccessright.setOpenAccessRoute(OpenAccessRoute.gold)
          }

        } else {
          hb = ModelConstants.UNKNOWN_REPOSITORY
        }
        i.setHostedby(hb)
      })

    publication.setBestaccessright(OafMapperUtils.createBestAccessRights(publication.getInstance()))

    publication
  }

  def generateDSId(input: String): String = {

    val b = StringUtils.substringBefore(input, "::")
    val a = StringUtils.substringAfter(input, "::")
    s"10|${b}::${DHPUtils.md5(a)}"
  }

  def generateDataInfo(): DataInfo = {
    generateDataInfo("0.9")
  }

  def filterPublication(publication: Publication): Boolean = {

    //Case empty publication
    if (publication == null)
      return false
    if (publication.getId == null || publication.getId.isEmpty)
      return false

    //Case publication with no title
    if (publication.getTitle == null || publication.getTitle.size == 0)
      return false

    val s = publication.getTitle.asScala.count(p =>
      p.getValue != null
      && p.getValue.nonEmpty && !p.getValue.equalsIgnoreCase("[NO TITLE AVAILABLE]")
    )

    if (s == 0)
      return false

    // fixes #4360 (test publisher)
    val publisher =
      if (publication.getPublisher != null) publication.getPublisher.getValue else null

    if (
      publisher != null && (publisher.equalsIgnoreCase("Test accounts") || publisher
        .equalsIgnoreCase("CrossRef Test Account"))
    ) {
      return false;
    }

    //Publication with no Author
    if (publication.getAuthor == null || publication.getAuthor.size() == 0)
      return false

    //filter invalid author
    val authors = publication.getAuthor.asScala.map(s => {
      if (s.getFullname.nonEmpty) {
        s.getFullname
      } else
        s"${s.getName} ${s.getSurname}"
    })

    val c = authors.count(isValidAuthorName)
    if (c == 0)
      return false

    // fixes #4368
    if (
      authors.count(s => s.equalsIgnoreCase("Addie Jackson")) > 0 && "Elsevier BV".equalsIgnoreCase(
        publication.getPublisher.getValue
      )
    )
      return false

    true
  }

  def isValidAuthorName(fullName: String): Boolean = {
    if (fullName == null || fullName.isEmpty)
      return false
    if (invalidName.contains(fullName.toLowerCase.trim))
      return false
    true
  }

  def generateDataInfo(trust: String): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust(trust)
    di.setProvenanceaction(
      OafMapperUtils.qualifier(
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.DNET_PROVENANCE_ACTIONS,
        ModelConstants.DNET_PROVENANCE_ACTIONS
      )
    )
    di
  }

  def createSP(
    value: String,
    classId: String,
    className: String,
    schemeId: String,
    schemeName: String
  ): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(OafMapperUtils.qualifier(classId, className, schemeId, schemeName))
    sp.setValue(value)
    sp

  }

  def createSP(
    value: String,
    classId: String,
    className: String,
    schemeId: String,
    schemeName: String,
    dataInfo: DataInfo
  ): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(OafMapperUtils.qualifier(classId, className, schemeId, schemeName))
    sp.setValue(value)
    sp.setDataInfo(dataInfo)
    sp

  }

  def createSP(value: String, classId: String, schemeId: String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(OafMapperUtils.qualifier(classId, classId, schemeId, schemeId))
    sp.setValue(value)
    sp

  }

  def createSP(
    value: String,
    classId: String,
    schemeId: String,
    dataInfo: DataInfo
  ): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(OafMapperUtils.qualifier(classId, classId, schemeId, schemeId))
    sp.setValue(value)
    sp.setDataInfo(dataInfo)
    sp

  }

  def createCrossrefCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(CROSSREF)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(CROSSREF.toLowerCase))
    cf

  }

  def createUnpayWallCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(UNPAYWALL)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(UNPAYWALL.toLowerCase))
    cf

  }

  def createORIDCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(StringUtils.upperCase(ModelConstants.ORCID))
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(ModelConstants.ORCID.toLowerCase))
    cf

  }

  def generateIdentifier(oaf: Result, doi: String): String = {
    val id = DHPUtils.md5(doi.toLowerCase)
    s"50|${doiBoostNSPREFIX}${SEPARATOR}${id}"
  }

  def createMAGCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(MAG_NAME)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(MAG))
    cf

  }

  def asField[T](value: T): Field[T] = {
    val tmp = new Field[T]
    tmp.setValue(value)
    tmp

  }

  def isEmpty(x: String) = x == null || x.trim.isEmpty

  def normalizeDoi(input: String): String = {
    if (input == null)
      return null
    val replaced = input
      .replaceAll("(?:\\n|\\r|\\t|\\s)", "")
      .toLowerCase
      .replaceFirst(DOI_PREFIX_REGEX, DOI_PREFIX)
    if (isEmpty(replaced))
      return null

    if (replaced.indexOf("10.") < 0)
      return null

    val ret = replaced.substring(replaced.indexOf("10."))

    if (!ret.startsWith(DOI_PREFIX))
      return null

    return ret

  }

}
