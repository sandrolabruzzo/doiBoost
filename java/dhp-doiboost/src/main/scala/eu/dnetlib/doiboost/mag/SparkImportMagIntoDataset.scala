package eu.dnetlib.doiboost.mag

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkImportMagIntoDataset {

  val datatypedict = Map(
    "bool"     -> BooleanType,
    "int"      -> IntegerType,
    "uint"     -> IntegerType,
    "long"     -> LongType,
    "ulong"    -> LongType,
    "float"    -> FloatType,
    "string"   -> StringType,
    "DateTime" -> DateType
  )

  val stream = Map(
    "Affiliations" -> Tuple2(
      "mag/Affiliations.txt",
      Seq(
        "AffiliationId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "GridId:string",
        "OfficialPage:string",
        "WikiPage:string",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "Iso3166Code:string",
        "Latitude:float?",
        "Longitude:float?",
        "CreatedDate:DateTime"
      )
    ),
    "AuthorExtendedAttributes" -> Tuple2(
      "mag/AuthorExtendedAttributes.txt",
      Seq("AuthorId:long", "AttributeType:int", "AttributeValue:string")
    ),
    "Authors" -> Tuple2(
      "mag/Authors.txt",
      Seq(
        "AuthorId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "LastKnownAffiliationId:long?",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "ConferenceInstances" -> Tuple2(
      "mag/ConferenceInstances.txt",
      Seq(
        "ConferenceInstanceId:long",
        "NormalizedName:string",
        "DisplayName:string",
        "ConferenceSeriesId:long",
        "Location:string",
        "OfficialUrl:string",
        "StartDate:DateTime?",
        "EndDate:DateTime?",
        "AbstractRegistrationDate:DateTime?",
        "SubmissionDeadlineDate:DateTime?",
        "NotificationDueDate:DateTime?",
        "FinalVersionDueDate:DateTime?",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "Latitude:float?",
        "Longitude:float?",
        "CreatedDate:DateTime"
      )
    ),
    "ConferenceSeries" -> Tuple2(
      "mag/ConferenceSeries.txt",
      Seq(
        "ConferenceSeriesId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "EntityRelatedEntities" -> Tuple2(
      "advanced/EntityRelatedEntities.txt",
      Seq(
        "EntityId:long",
        "EntityType:string",
        "RelatedEntityId:long",
        "RelatedEntityType:string",
        "RelatedType:int",
        "Score:float"
      )
    ),
    "FieldOfStudyChildren" -> Tuple2(
      "advanced/FieldOfStudyChildren.txt",
      Seq("FieldOfStudyId:long", "ChildFieldOfStudyId:long")
    ),
    "FieldOfStudyExtendedAttributes" -> Tuple2(
      "advanced/FieldOfStudyExtendedAttributes.txt",
      Seq("FieldOfStudyId:long", "AttributeType:int", "AttributeValue:string")
    ),
    "FieldsOfStudy" -> Tuple2(
      "advanced/FieldsOfStudy.txt",
      Seq(
        "FieldOfStudyId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "MainType:string",
        "Level:int",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "Journals" -> Tuple2(
      "mag/Journals.txt",
      Seq(
        "JournalId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "Issn:string",
        "Publisher:string",
        "Webpage:string",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "PaperAbstractsInvertedIndex" -> Tuple2(
      "nlp/PaperAbstractsInvertedIndex.txt.*",
      Seq("PaperId:long", "IndexedAbstract:string")
    ),
    "PaperAuthorAffiliations" -> Tuple2(
      "mag/PaperAuthorAffiliations.txt",
      Seq(
        "PaperId:long",
        "AuthorId:long",
        "AffiliationId:long?",
        "AuthorSequenceNumber:uint",
        "OriginalAuthor:string",
        "OriginalAffiliation:string"
      )
    ),
    "PaperCitationContexts" -> Tuple2(
      "nlp/PaperCitationContexts.txt",
      Seq("PaperId:long", "PaperReferenceId:long", "CitationContext:string")
    ),
    "PaperExtendedAttributes" -> Tuple2(
      "mag/PaperExtendedAttributes.txt",
      Seq("PaperId:long", "AttributeType:int", "AttributeValue:string")
    ),
    "PaperFieldsOfStudy" -> Tuple2(
      "advanced/PaperFieldsOfStudy.txt",
      Seq("PaperId:long", "FieldOfStudyId:long", "Score:float")
    ),
    "PaperMeSH" -> Tuple2(
      "advanced/PaperMeSH.txt",
      Seq(
        "PaperId:long",
        "DescriptorUI:string",
        "DescriptorName:string",
        "QualifierUI:string",
        "QualifierName:string",
        "IsMajorTopic:bool"
      )
    ),
    "PaperRecommendations" -> Tuple2(
      "advanced/PaperRecommendations.txt",
      Seq("PaperId:long", "RecommendedPaperId:long", "Score:float")
    ),
    "PaperReferences" -> Tuple2(
      "mag/PaperReferences.txt",
      Seq("PaperId:long", "PaperReferenceId:long")
    ),
    "PaperResources" -> Tuple2(
      "mag/PaperResources.txt",
      Seq(
        "PaperId:long",
        "ResourceType:int",
        "ResourceUrl:string",
        "SourceUrl:string",
        "RelationshipType:int"
      )
    ),
    "PaperUrls" -> Tuple2(
      "mag/PaperUrls.txt",
      Seq("PaperId:long", "SourceType:int?", "SourceUrl:string", "LanguageCode:string")
    ),
    "Papers" -> Tuple2(
      "mag/Papers.txt",
      Seq(
        "PaperId:long",
        "Rank:uint",
        "Doi:string",
        "DocType:string",
        "PaperTitle:string",
        "OriginalTitle:string",
        "BookTitle:string",
        "Year:int?",
        "Date:DateTime?",
        "OnlineDate:DateTime?",
        "Publisher:string",
        "JournalId:long?",
        "ConferenceSeriesId:long?",
        "ConferenceInstanceId:long?",
        "Volume:string",
        "Issue:string",
        "FirstPage:string",
        "LastPage:string",
        "ReferenceCount:long",
        "CitationCount:long",
        "EstimatedCitation:long",
        "OriginalVenue:string",
        "FamilyId:long?",
        "FamilyRank:uint?",
        "DocSubTypes:string",
        "CreatedDate:DateTime"
      )
    ),
    "RelatedFieldOfStudy" -> Tuple2(
      "advanced/RelatedFieldOfStudy.txt",
      Seq(
        "FieldOfStudyId1:long",
        "Type1:string",
        "FieldOfStudyId2:long",
        "Type2:string",
        "Rank:float"
      )
    )
  )

  def getSchema(streamName: String): StructType = {
    var schema = new StructType()
    val d: Seq[String] = stream(streamName)._2
    d.foreach { case t =>
      val currentType = t.split(":")
      val fieldName: String = currentType.head
      var fieldType: String = currentType.last
      val nullable: Boolean = fieldType.endsWith("?")
      if (nullable)
        fieldType = fieldType.replace("?", "")
      schema = schema.add(StructField(fieldName, datatypedict(fieldType), nullable))
    }
    schema
  }

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/mag/convert_mag_to_oaf_params.json")
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

    stream.foreach { case (k, v) =>
      val s: StructType = getSchema(k)
      val df = spark.read
        .option("header", "false")
        .option("charset", "UTF8")
        .option("delimiter", "\t")
        .schema(s)
        .csv(s"${parser.get("sourcePath")}/${v._1}")
      logger.info(s"Converting $k")

      df.write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/$k")
    }

  }

}
