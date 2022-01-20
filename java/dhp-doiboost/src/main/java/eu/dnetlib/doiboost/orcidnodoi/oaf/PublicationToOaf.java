
package eu.dnetlib.doiboost.orcidnodoi.oaf;

import static eu.dnetlib.doiboost.orcidnodoi.util.DumpToActionsUtility.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.*;

import eu.dnetlib.dhp.common.PacePerson;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.doiboost.orcidnodoi.util.DumpToActionsUtility;
import eu.dnetlib.doiboost.orcidnodoi.util.Pair;

/**
 * This class converts an orcid publication from json format to oaf
 */
public class PublicationToOaf implements Serializable {

	static Logger logger = LoggerFactory.getLogger(PublicationToOaf.class);

	public static final String orcidPREFIX = "orcid_______";
	public static final String OPENAIRE_PREFIX = "openaire____";
	public static final String SEPARATOR = IdentifierFactory.ID_SEPARATOR;
	public static final String DEACTIVATED_NAME = "Given Names Deactivated";
	public static final String DEACTIVATED_SURNAME = "Family Name Deactivated";

	private String dateOfCollection = "";
	private final LongAccumulator parsedPublications;
	private final LongAccumulator enrichedPublications;
	private final LongAccumulator errorsInvalidTitle;
	private final LongAccumulator errorsNotFoundAuthors;
	private final LongAccumulator errorsInvalidType;
	private final LongAccumulator otherTypeFound;
	private final LongAccumulator deactivatedAcc;
	private final LongAccumulator titleNotProvidedAcc;
	private final LongAccumulator noUrlAcc;

	public PublicationToOaf(
		LongAccumulator parsedPublications,
		LongAccumulator enrichedPublications,
		LongAccumulator errorsInvalidTitle,
		LongAccumulator errorsNotFoundAuthors,
		LongAccumulator errorsInvalidType,
		LongAccumulator otherTypeFound,
		LongAccumulator deactivatedAcc,
		LongAccumulator titleNotProvidedAcc,
		LongAccumulator noUrlAcc,
		String dateOfCollection) {
		this.parsedPublications = parsedPublications;
		this.enrichedPublications = enrichedPublications;
		this.errorsInvalidTitle = errorsInvalidTitle;
		this.errorsNotFoundAuthors = errorsNotFoundAuthors;
		this.errorsInvalidType = errorsInvalidType;
		this.otherTypeFound = otherTypeFound;
		this.deactivatedAcc = deactivatedAcc;
		this.titleNotProvidedAcc = titleNotProvidedAcc;
		this.noUrlAcc = noUrlAcc;
		this.dateOfCollection = dateOfCollection;
	}

	public PublicationToOaf() {
		this.parsedPublications = null;
		this.enrichedPublications = null;
		this.errorsInvalidTitle = null;
		this.errorsNotFoundAuthors = null;
		this.errorsInvalidType = null;
		this.otherTypeFound = null;
		this.deactivatedAcc = null;
		this.titleNotProvidedAcc = null;
		this.noUrlAcc = null;
		this.dateOfCollection = null;
	}

	// json external id will be mapped to oaf:pid/@classid Map to oaf:pid/@classname
	private static final Map<String, Pair<String, String>> externalIds = new HashMap<String, Pair<String, String>>() {
		{
			put("ark".toLowerCase(), new Pair<>("ark", "ark"));
			put("arxiv".toLowerCase(), new Pair<>("arXiv", "arXiv"));
			put("pmc".toLowerCase(), new Pair<>("pmc", "PubMed Central ID"));
			put("pmid".toLowerCase(), new Pair<>("pmid", "PubMed ID"));
			put("source-work-id".toLowerCase(), new Pair<>("orcidworkid", "orcid workid"));
			put("urn".toLowerCase(), new Pair<>("urn", "urn"));
		}
	};

	static Map<String, Map<String, String>> typologiesMapping;

	static {
		try {
			final String tt = IOUtils
				.toString(
					PublicationToOaf.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/orcidnodoi/mappings/typologies.json"));
			typologiesMapping = new Gson().fromJson(tt, Map.class);
		} catch (Exception e) {
			throw new RuntimeException("loading typologies", e);
		}
	}

	public Oaf generatePublicationActionsFromJson(final String json) {
		if (parsedPublications != null) {
			parsedPublications.add(1);
		}
		JsonElement jElement = new JsonParser().parse(json);
		JsonObject jObject = jElement.getAsJsonObject();
		return generatePublicationActionsFromDump(jObject);
	}

	public Oaf generatePublicationActionsFromDump(final JsonObject rootElement) {

		if (!isValid(rootElement)) {
			return null;
		}

		Publication publication = new Publication();

		final DataInfo dataInfo = new DataInfo();
		dataInfo.setDeletedbyinference(false);
		dataInfo.setInferred(false);
		dataInfo.setTrust("0.9");
		dataInfo
			.setProvenanceaction(
				mapQualifier(
					ModelConstants.SYSIMPORT_ORCID_NO_DOI,
					ModelConstants.SYSIMPORT_ORCID_NO_DOI,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS));
		publication.setDataInfo(dataInfo);

		publication.setLastupdatetimestamp(new Date().getTime());

		publication.setDateofcollection(dateOfCollection);
		publication.setDateoftransformation(DumpToActionsUtility.now_ISO8601());

		// Adding external ids
		externalIds
			.keySet()
			.stream()
			.forEach(jsonExtId -> {
				final String classid = externalIds.get(jsonExtId.toLowerCase()).getKey();
				final String classname = externalIds.get(jsonExtId.toLowerCase()).getValue();
				final String extId = getStringValue(rootElement, jsonExtId);
				if (StringUtils.isNotBlank(extId)) {
					publication
						.getExternalReference()
						.add(
							convertExtRef(
								extId, classid, classname, ModelConstants.DNET_PID_TYPES,
								ModelConstants.DNET_PID_TYPES));
				}
			});

		// Adding source
		final String source = getStringValue(rootElement, "sourceName");
		if (StringUtils.isNotBlank(source)) {
			Field<String> sourceField = mapStringField(source, null);
			if (sourceField == null) {
				publication.setSource(null);
			} else {
				publication.setSource(Arrays.asList(sourceField));
			}
		}

		// Adding titles
		final List<String> titles = createRepeatedField(rootElement, "titles");
		if (titles == null || titles.isEmpty()) {
			if (errorsInvalidTitle != null) {
				errorsInvalidTitle.add(1);
			}
			return null;
		}
		if (titles.stream().filter(t -> (t != null && t.equals("Title Not Supplied"))).count() > 0) {
			if (titleNotProvidedAcc != null) {
				titleNotProvidedAcc.add(1);
			}
			return null;
		}
		publication
			.setTitle(
				titles
					.stream()
					.map(t -> mapStructuredProperty(t, ModelConstants.MAIN_TITLE_QUALIFIER, null))
					.filter(Objects::nonNull)
					.collect(Collectors.toList()));
		// Adding identifier
		final String id = getStringValue(rootElement, "id");
		String sourceId = null;
		if (id != null) {
			publication.setOriginalId(Arrays.asList(id));
			sourceId = String.format("50|%s" + SEPARATOR + "%s", orcidPREFIX, DHPUtils.md5(id.toLowerCase()));
		} else {
			String mergedTitle = titles.stream().map(Object::toString).collect(Collectors.joining(","));
			sourceId = String.format("50|%s" + SEPARATOR + "%s", orcidPREFIX, DHPUtils.md5(mergedTitle.toLowerCase()));
		}
		publication.setId(sourceId);

		// Adding relevant date
		settingRelevantDate(rootElement, publication, "issued", true);

		// Adding collectedfrom
		publication.setCollectedfrom(Arrays.asList(createCollectedFrom()));

		// Adding type
		final String type = getStringValue(rootElement, "type");
		String cobjValue = "";
		if (StringUtils.isNotBlank(type)) {
			publication
				.setResourcetype(
					mapQualifier(
						type, type, ModelConstants.DNET_DATA_CITE_RESOURCE, ModelConstants.DNET_DATA_CITE_RESOURCE));

			Map<String, String> publicationType = typologiesMapping.get(type);
			if ((publicationType == null || publicationType.isEmpty()) && errorsInvalidType != null) {
				errorsInvalidType.add(1);
				logger.error("publication_type_not_found: {}", type);
				return null;
			}

			final String typeValue = typologiesMapping.get(type).get("value");
			cobjValue = typologiesMapping.get(type).get("cobj");
			// this dataset must contain only publication
			if (cobjValue.equals("0020")) {
				if (otherTypeFound != null) {
					otherTypeFound.add(1);
				}
				return null;
			}

			final Instance instance = new Instance();

			// Adding hostedby
			instance.setHostedby(createHostedBy());

			// Adding url
			final List<String> urls = createRepeatedField(rootElement, "urls");
			if (urls != null && !urls.isEmpty()) {
				instance.setUrl(urls);
			} else {
				if (noUrlAcc != null) {
					noUrlAcc.add(1);
				}
				return null;
			}

			dataInfo.setInvisible(true);

			final String pubDate = getPublicationDate(rootElement, "publicationDates");
			if (StringUtils.isNotBlank(pubDate)) {
				instance.setDateofacceptance(mapStringField(pubDate, null));
			}

			instance.setCollectedfrom(createCollectedFrom());

			// Adding accessright
			instance
				.setAccessright(
					OafMapperUtils
						.accessRight(
							ModelConstants.UNKNOWN, "Unknown", ModelConstants.DNET_ACCESS_MODES,
							ModelConstants.DNET_ACCESS_MODES));

			// Adding type
			instance
				.setInstancetype(
					mapQualifier(
						cobjValue, typeValue, ModelConstants.DNET_PUBLICATION_RESOURCE,
						ModelConstants.DNET_PUBLICATION_RESOURCE));

			publication.setInstance(Arrays.asList(instance));
		} else {
			if (errorsInvalidType != null) {
				errorsInvalidType.add(1);
			}
			return null;
		}

		// Adding authors
		final List<Author> authors = createAuthors(rootElement);
		if (authors != null && !authors.isEmpty()) {
			if (authors.stream().filter(a -> {
				return ((Objects.nonNull(a.getName()) && a.getName().equals(DEACTIVATED_NAME)) ||
					(Objects.nonNull(a.getSurname()) && a.getSurname().equals(DEACTIVATED_SURNAME)));
			}).count() > 0) {
				if (deactivatedAcc != null) {
					deactivatedAcc.add(1);
				}
				return null;
			} else {
				publication.setAuthor(authors);
			}
		} else {
			if (authors == null) {
				Gson gson = new GsonBuilder().setPrettyPrinting().create();
				throw new RuntimeException("not_valid_authors: " + gson.toJson(rootElement));
			} else {
				if (errorsNotFoundAuthors != null) {
					errorsNotFoundAuthors.add(1);
				}
				return null;
			}
		}
		String classValue = getDefaultResulttype(cobjValue);
		publication
			.setResulttype(
				mapQualifier(
					classValue, classValue, ModelConstants.DNET_RESULT_TYPOLOGIES,
					ModelConstants.DNET_RESULT_TYPOLOGIES));
		if (enrichedPublications != null) {
			enrichedPublications.add(1);
		}
		return publication;
	}

	public List<Author> createAuthors(final JsonObject root) {

		final String authorsJSONFieldName = "contributors";

		if (root.has(authorsJSONFieldName) && root.get(authorsJSONFieldName).isJsonArray()) {

			final List<Author> authors = new ArrayList<>();
			final JsonArray jsonAuthors = root.getAsJsonArray(authorsJSONFieldName);
			int firstCounter = 0;
			int defaultCounter = 0;
			int rank = 1;
			int currentRank = 0;

			for (final JsonElement item : jsonAuthors) {
				final JsonObject jsonAuthor = item.getAsJsonObject();
				final Author author = new Author();
				if (item.isJsonObject()) {
					final String creditname = getStringValue(jsonAuthor, "creditName");
					final String surname = getStringValue(jsonAuthor, "surname");
					final String name = getStringValue(jsonAuthor, "name");
					final String oid = getStringValue(jsonAuthor, "oid");
					final String seq = getStringValue(jsonAuthor, "sequence");
					if (StringUtils.isNotBlank(seq)) {
						if (seq.equals("first")) {
							firstCounter += 1;
							rank = firstCounter;

						} else if (seq.equals("additional")) {
							rank = currentRank + 1;
						} else {
							defaultCounter += 1;
							rank = defaultCounter;
						}
					}
					if (StringUtils.isNotBlank(oid)) {
						author.setPid(Arrays.asList(mapAuthorId(oid)));
						author.setFullname(name + " " + surname);
						if (StringUtils.isNotBlank(name)) {
							author.setName(name);
						}
						if (StringUtils.isNotBlank(surname)) {
							author.setSurname(surname);
						}
					} else {
						PacePerson p = new PacePerson(creditname, false);
						if (p.isAccurate()) {
							author.setName(p.getNormalisedFirstName());
							author.setSurname(p.getNormalisedSurname());
							author.setFullname(p.getNormalisedFullname());
						} else {
							author.setFullname(creditname);
						}
					}
				}
				author.setRank(rank);
				authors.add(author);
				currentRank = rank;
			}
			return authors;

		}
		return null;
	}

	private List<String> createRepeatedField(final JsonObject rootElement, final String fieldName) {
		if (!rootElement.has(fieldName)) {
			return null;
		}
		if (rootElement.has(fieldName) && rootElement.get(fieldName).isJsonNull()) {
			return null;
		}
		if (rootElement.get(fieldName).isJsonArray()) {
			if (!isValidJsonArray(rootElement, fieldName)) {
				return null;
			}
			return getArrayValues(rootElement, fieldName);
		} else {
			String field = getStringValue(rootElement, fieldName);
			return Arrays.asList(cleanField(field));
		}
	}

	private String cleanField(String value) {
		if (value != null && !value.isEmpty() && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
			value = value.substring(1, value.length() - 1);
		}
		return value;
	}

	private void settingRelevantDate(final JsonObject rootElement,
		final Publication publication,
		final String dictionaryKey,
		final boolean addToDateOfAcceptance) {

		final String pubDate = getPublicationDate(rootElement, "publication_date");
		if (StringUtils.isNotBlank(pubDate)) {
			if (addToDateOfAcceptance) {
				publication.setDateofacceptance(mapStringField(pubDate, null));
			}
			Qualifier q = mapQualifier(
				dictionaryKey, dictionaryKey, ModelConstants.DNET_DATACITE_DATE, ModelConstants.DNET_DATACITE_DATE);
			publication
				.setRelevantdate(
					Arrays
						.asList(pubDate)
						.stream()
						.map(r -> mapStructuredProperty(r, q, null))
						.filter(Objects::nonNull)
						.collect(Collectors.toList()));
		}
	}

	private String getPublicationDate(final JsonObject rootElement,
		final String jsonKey) {

		JsonObject pubDateJson = null;
		try {
			pubDateJson = rootElement.getAsJsonObject(jsonKey);
		} catch (Exception e) {
			return null;
		}
		if (pubDateJson == null) {
			return null;
		}
		final String year = getStringValue(pubDateJson, "year");
		final String month = getStringValue(pubDateJson, "month");
		final String day = getStringValue(pubDateJson, "day");

		if (StringUtils.isBlank(year)) {
			return null;
		}
		String pubDate = "".concat(year);
		if (StringUtils.isNotBlank(month)) {
			pubDate = pubDate.concat("-" + month);
			if (StringUtils.isNotBlank(day)) {
				pubDate = pubDate.concat("-" + day);
			} else {
				pubDate += "-01";
			}
		} else {
			pubDate += "-01-01";
		}
		if (isValidDate(pubDate)) {
			return pubDate;
		}
		return null;
	}

	protected boolean isValid(final JsonObject rootElement/* , final Reporter context */) {

		final String type = getStringValue(rootElement, "type");
		if (!typologiesMapping.containsKey(type)) {
			logger.error("unknowntype_{}", type);
			if (errorsInvalidType != null) {
				errorsInvalidType.add(1);
			}
			return false;
		}

		if (!isValidJsonArray(rootElement, "titles")) {
			if (errorsInvalidTitle != null) {
				errorsInvalidTitle.add(1);
			}
			return false;
		}
		return true;
	}

	private boolean isValidJsonArray(final JsonObject rootElement, final String fieldName) {
		if (!rootElement.has(fieldName)) {
			return false;
		}
		final JsonElement jsonElement = rootElement.get(fieldName);
		if (jsonElement.isJsonNull()) {
			return false;
		}
		if (jsonElement.isJsonArray()) {
			final JsonArray jsonArray = jsonElement.getAsJsonArray();
			if (jsonArray.isJsonNull()) {
				return false;
			}
			return !jsonArray.get(0).isJsonNull();
		}
		return true;
	}

	private Qualifier mapQualifier(String classId, String className, String schemeId, String schemeName) {
		final Qualifier qualifier = new Qualifier();
		qualifier.setClassid(classId);
		qualifier.setClassname(className);
		qualifier.setSchemeid(schemeId);
		qualifier.setSchemename(schemeName);
		return qualifier;
	}

	private ExternalReference convertExtRef(String extId, String classId, String className, String schemeId,
		String schemeName) {
		ExternalReference ex = new ExternalReference();
		ex.setRefidentifier(extId);
		ex.setQualifier(mapQualifier(classId, className, schemeId, schemeName));
		return ex;
	}

	private StructuredProperty mapStructuredProperty(String value, Qualifier qualifier, DataInfo dataInfo) {
		if (value == null || StringUtils.isBlank(value)) {
			return null;
		}

		final StructuredProperty structuredProperty = new StructuredProperty();
		structuredProperty.setValue(value);
		structuredProperty.setQualifier(qualifier);
		structuredProperty.setDataInfo(dataInfo);
		return structuredProperty;
	}

	private Field<String> mapStringField(String value, DataInfo dataInfo) {
		if (value == null || StringUtils.isBlank(value)) {
			return null;
		}

		final Field<String> stringField = new Field<>();
		stringField.setValue(value);
		stringField.setDataInfo(dataInfo);
		return stringField;
	}

	private KeyValue createCollectedFrom() {
		KeyValue cf = new KeyValue();
		cf.setValue(ModelConstants.ORCID.toUpperCase());
		cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + "806360c771262b4d6770e7cdf04b5c5a");
		return cf;
	}

	private KeyValue createHostedBy() {
		return ModelConstants.UNKNOWN_REPOSITORY;
	}

	private StructuredProperty mapAuthorId(String orcidId) {
		final StructuredProperty sp = new StructuredProperty();
		sp.setValue(orcidId);
		final Qualifier q = new Qualifier();
		q.setClassid(ModelConstants.ORCID);
		q.setClassname(ModelConstants.ORCID_CLASSNAME);
		q.setSchemeid(ModelConstants.DNET_PID_TYPES);
		q.setSchemename(ModelConstants.DNET_PID_TYPES);
		sp.setQualifier(q);
		final DataInfo dataInfo = new DataInfo();
		dataInfo.setDeletedbyinference(false);
		dataInfo.setInferred(false);
		dataInfo.setTrust("0.91");
		dataInfo
			.setProvenanceaction(
				mapQualifier(
					ModelConstants.SYSIMPORT_CROSSWALK_ENTITYREGISTRY,
					ModelConstants.HARVESTED,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS));
		sp.setDataInfo(dataInfo);
		return sp;
	}
}
