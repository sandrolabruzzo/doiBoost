
package eu.dnetlib.doiboost.orcidnodoi.xml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ximpleware.*;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.dhp.schema.orcid.Contributor;
import eu.dnetlib.dhp.schema.orcid.ExternalId;
import eu.dnetlib.dhp.schema.orcid.PublicationDate;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;

/**
 * This class is used for parsing xml data with vtd parser
 */
public class XMLRecordParserNoDoi {

	private static final String NS_COMMON_URL = "http://www.orcid.org/ns/common";
	private static final String NS_COMMON = "common";
	private static final String NS_ERROR_URL = "http://www.orcid.org/ns/error";

	private static final String NS_WORK = "work";
	private static final String NS_WORK_URL = "http://www.orcid.org/ns/work";

	private static final String NS_ERROR = "error";

	private XMLRecordParserNoDoi() {
	}

	public static WorkDetail VTDParseWorkData(byte[] bytes)
		throws VtdException, ParseException, XPathParseException,
		NavException, XPathEvalException {
		final VTDGen vg = new VTDGen();
		vg.setDoc(bytes);
		vg.parse(true);
		final VTDNav vn = vg.getNav();
		final AutoPilot ap = new AutoPilot(vn);
		ap.declareXPathNameSpace(NS_COMMON, NS_COMMON_URL);
		ap.declareXPathNameSpace(NS_WORK, NS_WORK_URL);
		ap.declareXPathNameSpace(NS_ERROR, NS_ERROR_URL);

		WorkDetail workData = new WorkDetail();
		final List<String> errors = VtdUtilityParser.getTextValue(ap, vn, "//error:response-code");
		if (!errors.isEmpty()) {
			workData.setErrorCode(errors.get(0));
			return workData;
		}

		List<VtdUtilityParser.Node> workNodes = VtdUtilityParser
			.getTextValuesWithAttributes(ap, vn, "//work:work", Arrays.asList("path", "put-code"));
		if (!workNodes.isEmpty()) {
			final String oid = (workNodes.get(0).getAttributes().get("path")).split("/")[1];
			workData.setOid(oid);
			final String id = (workNodes.get(0).getAttributes().get("put-code"));
			workData.setId(id);
		} else {
			return null;
		}

		final List<String> titles = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:title");
		if (!titles.isEmpty()) {
			workData.setTitles(titles);
		}

		final List<String> sourceNames = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:source-name");
		if (!sourceNames.isEmpty()) {
			workData.setSourceName(sourceNames.get(0));
		}

		final List<String> types = VtdUtilityParser
			.getTextValue(
				ap, vn, "//work:type");
		if (!types.isEmpty()) {
			workData.setType(types.get(0));
		}

		final List<String> urls = VtdUtilityParser
			.getTextValue(
				ap, vn, "//common:url");
		if (!urls.isEmpty()) {
			workData.setUrls(urls);
		}

		workData.setPublicationDates(getPublicationDates(vn, ap));
		workData.setExtIds(getExternalIds(vn, ap));
		workData.setContributors(getContributors(vn, ap));
		return workData;

	}

	private static List<PublicationDate> getPublicationDates(VTDNav vn, AutoPilot ap)
		throws XPathParseException, NavException, XPathEvalException {
		List<PublicationDate> publicationDates = new ArrayList<>();
		int yearIndex = 0;
		ap.selectXPath("//common:publication-date/common:year");
		while (ap.evalXPath() != -1) {
			PublicationDate publicationDate = new PublicationDate();
			int t = vn.getText();
			if (t >= 0) {
				publicationDate.setYear(vn.toNormalizedString(t));
				publicationDates.add(yearIndex, publicationDate);
				yearIndex++;
			}
		}
		int monthIndex = 0;
		ap.selectXPath("//common:publication-date/common:month");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				publicationDates.get(monthIndex).setMonth(vn.toNormalizedString(t));
				monthIndex++;
			}
		}
		int dayIndex = 0;
		ap.selectXPath("//common:publication-date/common:day");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				publicationDates.get(dayIndex).setDay(vn.toNormalizedString(t));
				dayIndex++;
			}
		}
		return publicationDates;
	}

	private static List<ExternalId> getExternalIds(VTDNav vn, AutoPilot ap)
		throws XPathParseException, NavException, XPathEvalException {
		List<ExternalId> extIds = new ArrayList<>();
		int typeIndex = 0;
		ap.selectXPath("//common:external-id/common:external-id-type");
		while (ap.evalXPath() != -1) {
			ExternalId extId = new ExternalId();
			int t = vn.getText();
			if (t >= 0) {
				extId.setType(vn.toNormalizedString(t));
				extIds.add(typeIndex, extId);
				typeIndex++;
			}
		}
		int valueIndex = 0;
		ap.selectXPath("//common:external-id/common:external-id-value");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				extIds.get(valueIndex).setValue(vn.toNormalizedString(t));
				valueIndex++;
			}
		}
		int relationshipIndex = 0;
		ap.selectXPath("//common:external-id/common:external-id-relationship");
		while (ap.evalXPath() != -1) {
			int t = vn.getText();
			if (t >= 0) {
				extIds.get(relationshipIndex).setRelationShip(vn.toNormalizedString(t));
				relationshipIndex++;
			}
		}
		if (typeIndex == valueIndex) {
			return extIds;
		}
		return new ArrayList<>();
	}

	private static List<Contributor> getContributors(VTDNav vn, AutoPilot ap)
		throws XPathParseException, NavException, XPathEvalException {
		List<Contributor> contributors = new ArrayList<>();
		ap.selectXPath("//work:contributors/work:contributor");
		while (ap.evalXPath() != -1) {
			Contributor contributor = new Contributor();
			if (vn.toElement(VTDNav.FIRST_CHILD, "work:credit-name")) {
				int val = vn.getText();
				if (val != -1) {
					contributor.setCreditName(vn.toNormalizedString(val));
				}
				vn.toElement(VTDNav.PARENT);
			}
			if (vn.toElement(VTDNav.FIRST_CHILD, "work:contributor-attributes")) {
				if (vn.toElement(VTDNav.FIRST_CHILD, "work:contributor-sequence")) {
					int val = vn.getText();
					if (val != -1) {
						contributor.setSequence(vn.toNormalizedString(val));
					}
					vn.toElement(VTDNav.PARENT);
				}
				if (vn.toElement(VTDNav.FIRST_CHILD, "work:contributor-role")) {
					int val = vn.getText();
					if (val != -1) {
						contributor.setRole(vn.toNormalizedString(val));
					}
					vn.toElement(VTDNav.PARENT);
				}
				vn.toElement(VTDNav.PARENT);
			}
			contributors.add(contributor);
		}
		return contributors;
	}
}
