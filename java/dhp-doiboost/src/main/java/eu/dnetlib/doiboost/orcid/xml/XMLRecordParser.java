
package eu.dnetlib.doiboost.orcid.xml;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.mortbay.log.Log;

import com.ximpleware.*;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.dhp.schema.orcid.AuthorHistory;
import eu.dnetlib.dhp.schema.orcid.AuthorSummary;
import eu.dnetlib.doiboost.orcid.model.WorkData;

public class XMLRecordParser {

	private static final String NS_COMMON_URL = "http://www.orcid.org/ns/common";
	private static final String NS_COMMON = "common";
	private static final String NS_PERSON_URL = "http://www.orcid.org/ns/person";
	private static final String NS_PERSON = "person";
	private static final String NS_DETAILS_URL = "http://www.orcid.org/ns/personal-details";
	private static final String NS_DETAILS = "personal-details";
	private static final String NS_OTHER_URL = "http://www.orcid.org/ns/other-name";
	private static final String NS_OTHER = "other-name";
	private static final String NS_RECORD_URL = "http://www.orcid.org/ns/record";
	private static final String NS_RECORD = "record";
	private static final String NS_ERROR_URL = "http://www.orcid.org/ns/error";
	private static final String NS_ACTIVITIES = "activities";
	private static final String NS_ACTIVITIES_URL = "http://www.orcid.org/ns/activities";
	private static final String NS_WORK = "work";
	private static final String NS_WORK_URL = "http://www.orcid.org/ns/work";
	private static final String NS_HISTORY = "history";
	private static final String NS_HISTORY_URL = "http://www.orcid.org/ns/history";

	private static final String NS_ERROR = "error";

	private XMLRecordParser() {
	}

	public static AuthorData VTDParseAuthorData(byte[] bytes)
		throws VtdException, ParseException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		ap.declareXPathNameSpace(NS_PERSON, NS_PERSON_URL);
		ap.declareXPathNameSpace(NS_DETAILS, NS_DETAILS_URL);
		ap.declareXPathNameSpace(NS_OTHER, NS_OTHER_URL);
		ap.declareXPathNameSpace(NS_RECORD, NS_RECORD_URL);
		ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);
		ap.declareXPathNameSpace(NS_HISTORY, NS_HISTORY_URL);

		AuthorData authorData = new AuthorData();
		final List<String> errors = VtdUtilityParser.getTextValue(ap, vn, "//error:response-code");
		if (!errors.isEmpty()) {
			authorData.setErrorCode(errors.get(0));
			return authorData;
		}

		List<VtdUtilityParser.Node> recordNodes = VtdUtilityParser
			.getTextValuesWithAttributes(
				ap, vn, "//record:record", Arrays.asList("path"));
		if (!recordNodes.isEmpty()) {
			final String oid = (recordNodes.get(0).getAttributes().get("path")).substring(1);
			authorData.setOid(oid);
		} else {
			return null;
		}

		final List<String> names = VtdUtilityParser.getTextValue(ap, vn, "//personal-details:given-names");
		if (!names.isEmpty()) {
			authorData.setName(names.get(0));
		}

		final List<String> surnames = VtdUtilityParser.getTextValue(ap, vn, "//personal-details:family-name");
		if (!surnames.isEmpty()) {
			authorData.setSurname(surnames.get(0));
		}

		final List<String> creditNames = VtdUtilityParser.getTextValue(ap, vn, "//personal-details:credit-name");
		if (!creditNames.isEmpty()) {
			authorData.setCreditName(creditNames.get(0));
		}

		final List<String> otherNames = VtdUtilityParser.getTextValue(ap, vn, "//other-name:content");
		if (!otherNames.isEmpty()) {
			authorData.setOtherNames(otherNames);
		}

		return authorData;
	}

	public static WorkData VTDParseWorkData(byte[] bytes)
		throws VtdException, ParseException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		ap.declareXPathNameSpace(NS_WORK, NS_WORK_URL);
		ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);

		WorkData workData = new WorkData();
		final List<String> errors = VtdUtilityParser.getTextValue(ap, vn, "//error:response-code");
		if (!errors.isEmpty()) {
			workData.setErrorCode(errors.get(0));
			return workData;
		}

		List<VtdUtilityParser.Node> workNodes = VtdUtilityParser
			.getTextValuesWithAttributes(ap, vn, "//work:work", Arrays.asList("path"));
		if (!workNodes.isEmpty()) {
			final String oid = (workNodes.get(0).getAttributes().get("path")).split("/")[1];
			workData.setOid(oid);
		} else {
			return null;
		}

		final List<String> dois = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:external-id-type[text()=\"doi\"]/../common:external-id-value");
		if (!dois.isEmpty()) {
			workData.setDoi(dois.get(0));
			workData.setDoiFound(true);
		}
		return workData;
	}

	public static String retrieveOrcidIdFromSummary(byte[] bytes, String defaultValue)
		throws VtdException, ParseException {
		return retrieveOrcidId(bytes, defaultValue, NS_RECORD, NS_RECORD_URL, "//record:record", "path").substring(1);
	}

	public static String retrieveOrcidIdFromActivity(byte[] bytes, String defaultValue)
		throws VtdException, ParseException {
		return retrieveOrcidId(bytes, defaultValue, NS_WORK, NS_WORK_URL, "//work:work", "put-code");
	}

	public static String retrieveWorkIdFromSummary(byte[] bytes, String defaultValue)
		throws VtdException, ParseException {
		return retrieveOrcidId(
			bytes, defaultValue, NS_ACTIVITIES, NS_ACTIVITIES_URL, "//work:work-summary", "put-code");
	}

	private static String retrieveOrcidId(byte[] bytes, String defaultValue, String ns, String nsUrl, String xpath,
		String idAttributeName)
		throws VtdException, ParseException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(ns, nsUrl);
		ap.declareXPathNameSpace(NS_WORK, NS_WORK_URL);
		List<VtdUtilityParser.Node> recordNodes = VtdUtilityParser
			.getTextValuesWithAttributes(
				ap, vn, xpath, Arrays.asList(idAttributeName));
		if (!recordNodes.isEmpty()) {
			return (recordNodes.get(0).getAttributes().get(idAttributeName));
		}
		Log.info("id not found - default: " + defaultValue);
		return defaultValue;
	}

	public static Map<String, String> retrieveWorkIdLastModifiedDate(byte[] bytes)
		throws ParseException, XPathParseException, NavException, XPathEvalException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_WORK, NS_WORK_URL);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		Map<String, String> workIdLastModifiedDate = new HashMap<>();
		ap.selectXPath("//work:work-summary");
		String workId = "";
		while (ap.evalXPath() != -1) {
			String lastModifiedDate = "";
			int attr = vn.getAttrVal("put-code");
			if (attr > -1) {
				workId = vn.toNormalizedString(attr);
			}
			if (vn.toElement(VTDNav.FIRST_CHILD, "common:last-modified-date")) {
				int val = vn.getText();
				if (val != -1) {
					lastModifiedDate = vn.toNormalizedString(val);
					workIdLastModifiedDate.put(workId, lastModifiedDate);
				}
				vn.toElement(VTDNav.PARENT);
			}
		}
		return workIdLastModifiedDate;
	}

	public static AuthorSummary VTDParseAuthorSummary(byte[] bytes)
		throws VtdException, ParseException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		ap.declareXPathNameSpace(NS_PERSON, NS_PERSON_URL);
		ap.declareXPathNameSpace(NS_DETAILS, NS_DETAILS_URL);
		ap.declareXPathNameSpace(NS_OTHER, NS_OTHER_URL);
		ap.declareXPathNameSpace(NS_RECORD, NS_RECORD_URL);
		ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);
		ap.declareXPathNameSpace(NS_HISTORY, NS_HISTORY_URL);

		AuthorData authorData = retrieveAuthorData(ap, vn);
		AuthorHistory authorHistory = retrieveAuthorHistory(ap, vn);
		AuthorSummary authorSummary = new AuthorSummary();
		authorSummary.setAuthorData(authorData);
		authorSummary.setAuthorHistory(authorHistory);
		return authorSummary;
	}

	private static AuthorData retrieveAuthorData(AutoPilot ap, VTDNav vn)
		throws VtdException {
		AuthorData authorData = new AuthorData();
		final List<String> errors = VtdUtilityParser.getTextValue(ap, vn, "//error:response-code");
		if (!errors.isEmpty()) {
			authorData.setErrorCode(errors.get(0));
			return authorData;
		}

		List<VtdUtilityParser.Node> recordNodes = VtdUtilityParser
			.getTextValuesWithAttributes(
				ap, vn, "//record:record", Arrays.asList("path"));
		if (!recordNodes.isEmpty()) {
			final String oid = (recordNodes.get(0).getAttributes().get("path")).substring(1);
			authorData.setOid(oid);
		} else {
			return null;
		}

		final List<String> names = VtdUtilityParser.getTextValue(ap, vn, "//personal-details:given-names");
		if (!names.isEmpty()) {
			authorData.setName(names.get(0));
		}

		final List<String> surnames = VtdUtilityParser.getTextValue(ap, vn, "//personal-details:family-name");
		if (!surnames.isEmpty()) {
			authorData.setSurname(surnames.get(0));
		}

		final List<String> creditNames = VtdUtilityParser.getTextValue(ap, vn, "//personal-details:credit-name");
		if (!creditNames.isEmpty()) {
			authorData.setCreditName(creditNames.get(0));
		}

		final List<String> otherNames = VtdUtilityParser.getTextValue(ap, vn, "//other-name:content");
		if (!otherNames.isEmpty()) {
			authorData.setOtherNames(otherNames);
		}
		return authorData;
	}

	private static AuthorHistory retrieveAuthorHistory(AutoPilot ap, VTDNav vn)
		throws VtdException {
		AuthorHistory authorHistory = new AuthorHistory();
		final String creationMethod = VtdUtilityParser.getSingleValue(ap, vn, "//history:creation-method");
		if (StringUtils.isNoneBlank(creationMethod)) {
			authorHistory.setCreationMethod(creationMethod);
		}

		final String completionDate = VtdUtilityParser.getSingleValue(ap, vn, "//history:completion-date");
		if (StringUtils.isNoneBlank(completionDate)) {
			authorHistory.setCompletionDate(completionDate);
		}

		final String submissionDate = VtdUtilityParser.getSingleValue(ap, vn, "//history:submission-date");
		if (StringUtils.isNoneBlank(submissionDate)) {
			authorHistory.setSubmissionDate(submissionDate);
		}

		final String claimed = VtdUtilityParser.getSingleValue(ap, vn, "//history:claimed");
		if (StringUtils.isNoneBlank(claimed)) {
			authorHistory.setClaimed(Boolean.parseBoolean(claimed));
		}

		final String verifiedEmail = VtdUtilityParser.getSingleValue(ap, vn, "//history:verified-email");
		if (StringUtils.isNoneBlank(verifiedEmail)) {
			authorHistory.setVerifiedEmail(Boolean.parseBoolean(verifiedEmail));
		}

		final String verifiedPrimaryEmail = VtdUtilityParser.getSingleValue(ap, vn, "//history:verified-primary-email");
		if (StringUtils.isNoneBlank(verifiedPrimaryEmail)) {
			authorHistory.setVerifiedPrimaryEmail(Boolean.parseBoolean(verifiedPrimaryEmail));
		}

		final String deactivationDate = VtdUtilityParser.getSingleValue(ap, vn, "//history:deactivation-date");
		if (StringUtils.isNoneBlank(deactivationDate)) {
			authorHistory.setDeactivationDate(deactivationDate);
		}

		final String lastModifiedDate = VtdUtilityParser
			.getSingleValue(ap, vn, "//history:history/common:last-modified-date");
		if (StringUtils.isNoneBlank(lastModifiedDate)) {
			authorHistory.setLastModifiedDate(lastModifiedDate);
		}
		return authorHistory;
	}
}
