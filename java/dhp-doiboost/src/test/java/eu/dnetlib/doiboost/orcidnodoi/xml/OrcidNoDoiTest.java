
package eu.dnetlib.doiboost.orcidnodoi.xml;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.dhp.schema.orcid.Contributor;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;
import eu.dnetlib.doiboost.orcidnodoi.similarity.AuthorMatcher;

class OrcidNoDoiTest {

	private static final Logger logger = LoggerFactory.getLogger(OrcidNoDoiTest.class);

	static String nameA = "Khairy";
	static String surnameA = "Abdel Dayem";
	static String orcidIdA = "0000-0003-2760-1191";

	@Test
	void readPublicationFieldsTest()
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		logger.info("running loadPublicationFieldsTest ....");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream("activity_work_0000-0002-2536-4498.xml"));

		if (xml == null) {
			logger.info("Resource not found");
		}
		WorkDetail workData = null;
		try {
			workData = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		assertNotNull(workData);
		assertNotNull(workData.getOid());
		logger.info("oid: " + workData.getOid());
		assertNotNull(workData.getTitles());
		logger.info("titles: ");
		workData.getTitles().forEach(t -> {
			logger.info(t);
		});
		logger.info("source: " + workData.getSourceName());
		logger.info("type: " + workData.getType());
		logger.info("urls: ");
		workData.getUrls().forEach(u -> {
			logger.info(u);
		});
		logger.info("publication date: ");
		workData.getPublicationDates().forEach(d -> {
			logger.info(d.getYear() + " - " + d.getMonth() + " - " + d.getDay());
		});
		logger.info("external id: ");
		workData.getExtIds().removeIf(e -> e.getRelationShip() != null && !e.getRelationShip().equals("self"));
		workData.getExtIds().forEach(e -> {
			logger.info(e.getType() + " - " + e.getValue() + " - " + e.getRelationShip());
		});
		logger.info("contributors: ");
		workData.getContributors().forEach(c -> {
			logger
				.info(
					c.getName() + " - " + c.getRole() + " - " + c.getSequence());
		});

	}

	@Test
	void authorDoubleMatchTest() throws Exception {
		logger.info("running authorSimpleMatchTest ....");
		String orcidWork = "activity_work_0000-0003-2760-1191-similarity.xml";
		AuthorData author = new AuthorData();
		author.setName(nameA);
		author.setSurname(surnameA);
		author.setOid(orcidIdA);
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream(orcidWork));

		WorkDetail workData = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());

		assertNotNull(workData);

		Contributor a = workData.getContributors().get(0);
		assertEquals("Abdel-Dayem K", a.getCreditName());

		AuthorMatcher.match(author, workData.getContributors());

		assertEquals(6, workData.getContributors().size());
	}

	@Test
	void readContributorsTest()
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		logger.info("running loadPublicationFieldsTest ....");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream("activity_work_0000-0003-2760-1191_contributors.xml"));

		WorkDetail workData = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());

		assertNotNull(workData.getContributors());
		assertEquals(5, workData.getContributors().size());
		assertTrue(StringUtils.isBlank(workData.getContributors().get(0).getCreditName()));
		assertEquals("seq0", workData.getContributors().get(0).getSequence());
		assertEquals("role0", workData.getContributors().get(0).getRole());
		assertEquals("creditname1", workData.getContributors().get(1).getCreditName());
		assertTrue(StringUtils.isBlank(workData.getContributors().get(1).getSequence()));
		assertTrue(StringUtils.isBlank(workData.getContributors().get(1).getRole()));
		assertEquals("creditname2", workData.getContributors().get(2).getCreditName());
		assertEquals("seq2", workData.getContributors().get(2).getSequence());
		assertTrue(StringUtils.isBlank(workData.getContributors().get(2).getRole()));
		assertEquals("creditname3", workData.getContributors().get(3).getCreditName());
		assertTrue(StringUtils.isBlank(workData.getContributors().get(3).getSequence()));
		assertEquals("role3", workData.getContributors().get(3).getRole());
		assertTrue(StringUtils.isBlank(workData.getContributors().get(4).getCreditName()));
		assertEquals("seq4", workData.getContributors().get(4).getSequence());
		assertEquals("role4", workData.getContributors().get(4).getRole());
	}

	@Test
	void authorSimpleMatchTest() throws Exception {
		String orcidWork = "activity_work_0000-0002-5982-8983.xml";
		AuthorData author = new AuthorData();
		author.setName("Parkhouse");
		author.setSurname("H.");
		author.setOid("0000-0002-5982-8983");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream(orcidWork));

		if (xml == null) {
			logger.info("Resource not found");
		}
		WorkDetail workData = null;
		try {
			workData = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		assertNotNull(workData);

		Contributor a = workData.getContributors().get(0);
		assertEquals("Parkhouse, H.", a.getCreditName());

		AuthorMatcher.match(author, workData.getContributors());

		assertEquals(2, workData.getContributors().size());
		Contributor c = workData.getContributors().get(0);

		assertEquals("0000-0002-5982-8983", c.getOid());
		assertEquals("Parkhouse", c.getName());
		assertEquals("H.", c.getSurname());
		assertEquals("Parkhouse, H.", c.getCreditName());
	}

	@Test
	void match() {

		AuthorData author = new AuthorData();
		author.setName("Joe");
		author.setSurname("Dodge");
		author.setOid("0000-1111-2222-3333");
		Contributor contributor = new Contributor();
		contributor.setCreditName("Joe Dodge");
		List<Contributor> contributors = Arrays.asList(contributor);
		int matchCounter = 0;
		List<Integer> matchCounters = Arrays.asList(matchCounter);
		contributors
			.stream()
			.filter(c -> !StringUtils.isBlank(c.getCreditName()))
			.forEach(c -> {
				if (AuthorMatcher.simpleMatch(c.getCreditName(), author.getName()) ||
					AuthorMatcher.simpleMatch(c.getCreditName(), author.getSurname()) ||
					AuthorMatcher.simpleMatchOnOtherNames(c.getCreditName(), author.getOtherNames())) {
					matchCounters.set(0, matchCounters.get(0) + 1);
					c.setSimpleMatch(true);
				}
			});

		assertEquals(1, matchCounters.get(0));
		AuthorMatcher.updateAuthorsSimpleMatch(contributors, author);

		assertEquals("Joe", contributors.get(0).getName());
		assertEquals("Dodge", contributors.get(0).getSurname());
		assertEquals("Joe Dodge", contributors.get(0).getCreditName());
		assertEquals("0000-1111-2222-3333", contributors.get(0).getOid());

		AuthorData authorX = new AuthorData();
		authorX.setName(nameA);
		authorX.setSurname(surnameA);
		authorX.setOid(orcidIdA);
		Contributor contributorA = new Contributor();
		contributorA.setCreditName("Abdel-Dayem Khai");
		Contributor contributorB = new Contributor();
		contributorB.setCreditName("Abdel-Dayem Fake");
		List<Contributor> contributorList = new ArrayList<>();
		contributorList.add(contributorA);
		contributorList.add(contributorB);
		int matchCounter2 = 0;
		List<Integer> matchCounters2 = Arrays.asList(matchCounter2);
		contributorList
			.stream()
			.filter(c -> !StringUtils.isBlank(c.getCreditName()))
			.forEach(c -> {
				if (AuthorMatcher.simpleMatch(c.getCreditName(), authorX.getName()) ||
					AuthorMatcher.simpleMatch(c.getCreditName(), authorX.getSurname()) ||
					AuthorMatcher.simpleMatchOnOtherNames(c.getCreditName(), author.getOtherNames())) {
					int currentCounter = matchCounters2.get(0);
					currentCounter += 1;
					matchCounters2.set(0, currentCounter);
					c.setSimpleMatch(true);
				}
			});

		assertEquals(2, matchCounters2.get(0));
		assertTrue(contributorList.get(0).isSimpleMatch());
		assertTrue(contributorList.get(1).isSimpleMatch());

		Optional<Contributor> optCon = contributorList
			.stream()
			.filter(c -> c.isSimpleMatch())
			.filter(c -> !StringUtils.isBlank(c.getCreditName()))
			.map(c -> {
				c.setScore(AuthorMatcher.bestMatch(authorX.getName(), authorX.getSurname(), c.getCreditName()));
				return c;
			})
			.filter(c -> c.getScore() >= AuthorMatcher.THRESHOLD)
			.max(Comparator.comparing(c -> c.getScore()));
		assertTrue(optCon.isPresent());

		final Contributor bestMatchContributor = optCon.get();
		bestMatchContributor.setBestMatch(true);
		assertTrue(bestMatchContributor.getCreditName().equals("Abdel-Dayem Khai"));
		assertTrue(contributorList.get(0).isBestMatch());
		assertTrue(!contributorList.get(1).isBestMatch());
		AuthorMatcher.updateAuthorsSimilarityMatch(contributorList, authorX);

		assertEquals(nameA, contributorList.get(0).getName());
		assertEquals(surnameA, contributorList.get(0).getSurname());
		assertEquals("Abdel-Dayem Khai", contributorList.get(0).getCreditName());
		assertEquals(orcidIdA, contributorList.get(0).getOid());
		assertTrue(StringUtils.isBlank(contributorList.get(1).getOid()));
	}

	@Test
	void authorBestMatchTest() throws Exception {
		String name = "Khairy";
		String surname = "Abdel Dayem";
		String orcidWork = "activity_work_0000-0003-2760-1191.xml";
		AuthorData author = new AuthorData();
		author.setName(name);
		author.setSurname(surname);
		author.setOid(orcidIdA);
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream(orcidWork));

		if (xml == null) {
			logger.info("Resource not found");
		}
		WorkDetail workData = null;
		try {
			workData = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		AuthorMatcher.match(author, workData.getContributors());
		assertEquals(5, workData.getContributors().size());
		List<Contributor> c = workData.getContributors();

		assertEquals(name, c.get(0).getName());
		assertEquals(surname, c.get(0).getSurname());
		assertEquals("Khair Abde Daye", c.get(0).getCreditName());
		assertEquals(orcidIdA, c.get(0).getOid());
	}

	@Test
	void otherNamesMatchTest()
		throws VtdException, ParseException, IOException, XPathEvalException, NavException, XPathParseException {

		AuthorData author = new AuthorData();
		author.setName("Joe");
		author.setSurname("Dodge");
		author.setOid("0000-1111-2222-3333");
		String otherName1 = "Joe Dr. Dodge";
		String otherName2 = "XY";
		List<String> others = Lists.newArrayList();
		others.add(otherName1);
		others.add(otherName2);
		author.setOtherNames(others);
		Contributor contributor = new Contributor();
		contributor.setCreditName("XY");
		List<Contributor> contributors = Arrays.asList(contributor);
		AuthorMatcher.match(author, contributors);

		assertEquals("Joe", contributors.get(0).getName());
		assertEquals("Dodge", contributors.get(0).getSurname());
		assertEquals("0000-1111-2222-3333", contributors.get(0).getOid());
	}
}
