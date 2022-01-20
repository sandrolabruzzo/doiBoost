package eu.dnetlib.doiboost.mag

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import eu.dnetlib.dhp.schema.oaf.{Instance, Journal, Publication, StructuredProperty}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

case class MagPapers(
  PaperId: Long,
  Rank: Integer,
  Doi: String,
  DocType: String,
  PaperTitle: String,
  OriginalTitle: String,
  BookTitle: String,
  Year: Option[Integer],
  Date: Option[java.sql.Timestamp],
  Publisher: String,
  JournalId: Option[Long],
  ConferenceSeriesId: Option[Long],
  ConferenceInstanceId: Option[Long],
  Volume: String,
  Issue: String,
  FirstPage: String,
  LastPage: String,
  ReferenceCount: Option[Long],
  CitationCount: Option[Long],
  EstimatedCitation: Option[Long],
  OriginalVenue: String,
  FamilyId: Option[Long],
  CreatedDate: java.sql.Timestamp
) {}

case class MagPaperAbstract(PaperId: Long, IndexedAbstract: String) {}

case class MagAuthor(
  AuthorId: Long,
  Rank: Option[Int],
  NormalizedName: Option[String],
  DisplayName: Option[String],
  LastKnownAffiliationId: Option[Long],
  PaperCount: Option[Long],
  CitationCount: Option[Long],
  CreatedDate: Option[java.sql.Timestamp]
) {}

case class MagAffiliation(
  AffiliationId: Long,
  Rank: Int,
  NormalizedName: String,
  DisplayName: String,
  GridId: String,
  OfficialPage: String,
  WikiPage: String,
  PaperCount: Long,
  CitationCount: Long,
  Latitude: Option[Float],
  Longitude: Option[Float],
  CreatedDate: java.sql.Timestamp
) {}

case class MagPaperAuthorAffiliation(
  PaperId: Long,
  AuthorId: Long,
  AffiliationId: Option[Long],
  AuthorSequenceNumber: Int,
  OriginalAuthor: String,
  OriginalAffiliation: String
) {}

case class MagAuthorAffiliation(author: MagAuthor, affiliation: String, sequenceNumber: Int)

case class MagPaperWithAuthorList(PaperId: Long, authors: List[MagAuthorAffiliation]) {}

case class MagPaperAuthorDenormalized(
  PaperId: Long,
  author: MagAuthor,
  affiliation: String,
  sequenceNumber: Int
) {}

case class MagPaperUrl(
  PaperId: Long,
  SourceType: Option[Int],
  SourceUrl: Option[String],
  LanguageCode: Option[String]
) {}

case class MagUrlInstance(SourceUrl: String) {}

case class MagUrl(PaperId: Long, instances: List[MagUrlInstance])

case class MagSubject(
  FieldOfStudyId: Long,
  DisplayName: String,
  MainType: Option[String],
  Score: Float
) {}

case class MagFieldOfStudy(PaperId: Long, subjects: List[MagSubject]) {}

case class MagJournal(
  JournalId: Long,
  Rank: Option[Int],
  NormalizedName: Option[String],
  DisplayName: Option[String],
  Issn: Option[String],
  Publisher: Option[String],
  Webpage: Option[String],
  PaperCount: Option[Long],
  CitationCount: Option[Long],
  CreatedDate: Option[java.sql.Timestamp]
) {}

case class MagConferenceInstance(
  ci: Long,
  DisplayName: Option[String],
  Location: Option[String],
  StartDate: Option[java.sql.Timestamp],
  EndDate: Option[java.sql.Timestamp],
  PaperId: Long
) {}

case object ConversionUtil {

  def extractMagIdentifier(pids: mutable.Buffer[String]): String = {
    val magIDRegex: Regex = "^[0-9]+$".r
    val s = pids.filter(p => magIDRegex.findAllIn(p).hasNext)

    if (s.nonEmpty)
      return s.head
    null
  }

  def mergePublication(a: Publication, b: Publication): Publication = {
    if ((a != null) && (b != null)) {
      a.mergeFrom(b)
      a
    } else {
      if (a == null) b else a
    }

  }

  def choiceLatestMagArtitcle(p1: MagPapers, p2: MagPapers): MagPapers = {
    var r = if (p1 == null) p2 else p1
    if (p1 != null && p2 != null) {
      if (p1.CreatedDate != null && p2.CreatedDate != null) {
        if (p1.CreatedDate.before(p2.CreatedDate))
          r = p2
        else
          r = p1
      } else {
        r = if (p1.CreatedDate == null) p2 else p1
      }
    }
    r

  }

  def updatePubsWithDescription(
    inputItem: ((String, Publication), MagPaperAbstract)
  ): Publication = {
    val pub = inputItem._1._2
    val abst = inputItem._2
    if (abst != null) {
      pub.setDescription(List(asField(abst.IndexedAbstract)).asJava)
    }
    pub

  }

  def updatePubsWithConferenceInfo(
    inputItem: ((String, Publication), MagConferenceInstance)
  ): Publication = {
    val publication: Publication = inputItem._1._2
    val ci: MagConferenceInstance = inputItem._2

    if (ci != null) {

      val j: Journal = new Journal
      if (ci.Location.isDefined)
        j.setConferenceplace(ci.Location.get)
      j.setName(ci.DisplayName.get)
      if (ci.StartDate.isDefined && ci.EndDate.isDefined) {
        j.setConferencedate(
          s"${ci.StartDate.get.toString.substring(0, 10)} - ${ci.EndDate.get.toString.substring(0, 10)}"
        )
      }

      publication.setJournal(j)
    }
    publication
  }

  def updatePubsWithSubject(item: ((String, Publication), MagFieldOfStudy)): Publication = {

    val publication = item._1._2
    val fieldOfStudy = item._2
    if (fieldOfStudy != null && fieldOfStudy.subjects != null && fieldOfStudy.subjects.nonEmpty) {

      val className = "Microsoft Academic Graph classification"
      val classid = "MAG"

      val p: List[StructuredProperty] = fieldOfStudy.subjects.flatMap(s => {
        val s1 = createSP(
          s.DisplayName,
          classid,
          className,
          ModelConstants.DNET_SUBJECT_TYPOLOGIES,
          ModelConstants.DNET_SUBJECT_TYPOLOGIES
        )
        val di = DoiBoostMappingUtil.generateDataInfo(s.Score.toString)
        var resList: List[StructuredProperty] = List(s1)
        if (s.MainType.isDefined) {
          val maintp = s.MainType.get
          val s2 = createSP(
            s.MainType.get,
            classid,
            className,
            ModelConstants.DNET_SUBJECT_TYPOLOGIES,
            ModelConstants.DNET_SUBJECT_TYPOLOGIES
          )
          s2.setDataInfo(di)
          resList = resList ::: List(s2)
          if (maintp.contains(".")) {
            val s3 = createSP(
              maintp.split("\\.").head,
              classid,
              className,
              ModelConstants.DNET_SUBJECT_TYPOLOGIES,
              ModelConstants.DNET_SUBJECT_TYPOLOGIES
            )
            s3.setDataInfo(di)
            resList = resList ::: List(s3)
          }
        }
        resList
      })
      publication.setSubject(p.asJava)
    }
    publication
  }

  def addInstances(a: (Publication, MagUrl)): Publication = {
    val pub = a._1
    val urls = a._2

    val i = new Instance

    if (urls != null) {

      val l: List[String] = urls.instances
        .filter(k => k.SourceUrl.nonEmpty)
        .map(k => k.SourceUrl) ::: List(
        s"https://academic.microsoft.com/#/detail/${extractMagIdentifier(pub.getOriginalId.asScala)}"
      )

      i.setUrl(l.asJava)
    } else
      i.setUrl(
        List(
          s"https://academic.microsoft.com/#/detail/${extractMagIdentifier(pub.getOriginalId.asScala)}"
        ).asJava
      )

    // Ticket #6281 added pid to Instance
    i.setPid(pub.getPid)

    i.setCollectedfrom(createMAGCollectedFrom())
    pub.setInstance(List(i).asJava)
    pub
  }

  def transformPaperAbstract(input: MagPaperAbstract): MagPaperAbstract = {
    MagPaperAbstract(input.PaperId, convertInvertedIndexString(input.IndexedAbstract))
  }

  def createOAFFromJournalAuthorPaper(
    inputParams: ((MagPapers, MagJournal), MagPaperWithAuthorList)
  ): Publication = {
    val paper = inputParams._1._1
    val journal = inputParams._1._2
    val authors = inputParams._2

    val pub = new Publication
    pub.setPid(List(createSP(paper.Doi, "doi", ModelConstants.DNET_PID_TYPES)).asJava)
    pub.setOriginalId(List(paper.PaperId.toString, paper.Doi).asJava)

    //IMPORTANT
    //The old method result.setId(generateIdentifier(result, doi))
    //will be replaced using IdentifierFactory

    pub.setId(IdentifierFactory.createDOIBoostIdentifier(pub))

    val mainTitles = createSP(paper.PaperTitle, "main title", ModelConstants.DNET_DATACITE_TITLE)
    val originalTitles =
      createSP(paper.OriginalTitle, "alternative title", ModelConstants.DNET_DATACITE_TITLE)
    pub.setTitle(List(mainTitles, originalTitles).asJava)

    pub.setSource(List(asField(paper.BookTitle)).asJava)

    val authorsOAF = authors.authors.map { f: MagAuthorAffiliation =>
      val a: eu.dnetlib.dhp.schema.oaf.Author = new eu.dnetlib.dhp.schema.oaf.Author
      a.setRank(f.sequenceNumber)
      if (f.author.DisplayName.isDefined)
        a.setFullname(f.author.DisplayName.get)
      if (f.affiliation != null)
        a.setAffiliation(List(asField(f.affiliation)).asJava)
      a.setPid(
        List(
          createSP(
            s"https://academic.microsoft.com/#/detail/${f.author.AuthorId}",
            "URL",
            ModelConstants.DNET_PID_TYPES
          )
        ).asJava
      )
      a
    }
    pub.setAuthor(authorsOAF.asJava)

    if (paper.Date != null && paper.Date.isDefined) {
      pub.setDateofacceptance(asField(paper.Date.get.toString.substring(0, 10)))
    }
    pub.setPublisher(asField(paper.Publisher))

    if (journal != null && journal.DisplayName.isDefined) {
      val j = new Journal

      j.setName(journal.DisplayName.get)
      j.setSp(paper.FirstPage)
      j.setEp(paper.LastPage)
      if (journal.Publisher.isDefined)
        pub.setPublisher(asField(journal.Publisher.get))
      if (journal.Issn.isDefined)
        j.setIssnPrinted(journal.Issn.get)
      j.setVol(paper.Volume)
      j.setIss(paper.Issue)
      pub.setJournal(j)
    }
    pub.setCollectedfrom(List(createMAGCollectedFrom()).asJava)
    pub.setDataInfo(generateDataInfo())
    pub
  }

  def createOAF(
    inputParams: ((MagPapers, MagPaperWithAuthorList), MagPaperAbstract)
  ): Publication = {

    val paper = inputParams._1._1
    val authors = inputParams._1._2
    val description = inputParams._2

    val pub = new Publication
    pub.setPid(List(createSP(paper.Doi, "doi", ModelConstants.DNET_PID_TYPES)).asJava)
    pub.setOriginalId(List(paper.PaperId.toString, paper.Doi).asJava)

    //IMPORTANT
    //The old method result.setId(generateIdentifier(result, doi))
    //will be replaced using IdentifierFactory

    pub.setId(IdentifierFactory.createDOIBoostIdentifier(pub))

    val mainTitles = createSP(paper.PaperTitle, "main title", ModelConstants.DNET_DATACITE_TITLE)
    val originalTitles =
      createSP(paper.OriginalTitle, "alternative title", ModelConstants.DNET_DATACITE_TITLE)
    pub.setTitle(List(mainTitles, originalTitles).asJava)

    pub.setSource(List(asField(paper.BookTitle)).asJava)

    if (description != null) {
      pub.setDescription(List(asField(description.IndexedAbstract)).asJava)
    }

    val authorsOAF = authors.authors.map { f: MagAuthorAffiliation =>
      val a: eu.dnetlib.dhp.schema.oaf.Author = new eu.dnetlib.dhp.schema.oaf.Author

      a.setFullname(f.author.DisplayName.get)

      if (f.affiliation != null)
        a.setAffiliation(List(asField(f.affiliation)).asJava)

      a.setPid(
        List(
          createSP(
            s"https://academic.microsoft.com/#/detail/${f.author.AuthorId}",
            "URL",
            ModelConstants.DNET_PID_TYPES
          )
        ).asJava
      )

      a

    }

    if (paper.Date != null) {
      pub.setDateofacceptance(asField(paper.Date.toString.substring(0, 10)))
    }

    pub.setAuthor(authorsOAF.asJava)

    pub

  }

  def convertInvertedIndexString(json_input: String): String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(json_input)
    val idl = (json \ "IndexLength").extract[Int]
    if (idl > 0) {
      val res = Array.ofDim[String](idl)

      val iid = (json \ "InvertedIndex").extract[Map[String, List[Int]]]

      for { (k: String, v: List[Int]) <- iid } {
        v.foreach(item => res(item) = k)
      }
      (0 until idl).foreach(i => {
        if (res(i) == null)
          res(i) = ""
      })
      return res.mkString(" ")
    }
    ""
  }
}
