
package eu.dnetlib.doiboost.orcid.xml;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.dhp.schema.orcid.AuthorSummary;
import eu.dnetlib.dhp.schema.orcid.Work;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;
import eu.dnetlib.doiboost.orcid.OrcidClientTest;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;
import eu.dnetlib.doiboost.orcidnodoi.xml.XMLRecordParserNoDoi;

public class XMLRecordParserTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static Path testPath;

	@BeforeAll
	private static void setUp() throws IOException {
		testPath = Files.createTempDirectory(XMLRecordParserTest.class.getName());
	}

	@Test
	void testOrcidAuthorDataXMLParser() throws Exception {

		String xml = IOUtils.toString(this.getClass().getResourceAsStream("summary_0000-0001-6828-479X.xml"));

		AuthorData authorData = XMLRecordParser.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getName());
		System.out.println("name: " + authorData.getName());
		assertNotNull(authorData.getSurname());
		System.out.println("surname: " + authorData.getSurname());
		OrcidClientTest.logToFile(testPath, OBJECT_MAPPER.writeValueAsString(authorData));
	}

	@Test
	void testOrcidXMLErrorRecordParser() throws Exception {

		String xml = IOUtils.toString(this.getClass().getResourceAsStream("summary_error.xml"));

		AuthorData authorData = XMLRecordParser.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getErrorCode());
		System.out.println("error: " + authorData.getErrorCode());
	}

	@Test
	void testOrcidWorkDataXMLParser() throws Exception {

		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("activity_work_0000-0003-2760-1191.xml"));

		WorkData workData = XMLRecordParser.VTDParseWorkData(xml.getBytes());
		assertNotNull(workData);
		assertNotNull(workData.getOid());
		System.out.println("oid: " + workData.getOid());
		assertNull(workData.getDoi());
	}

	@Test
	void testOrcidOtherNamesXMLParser() throws Exception {

		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("summary_0000-0001-5109-1000_othername.xml"));
		AuthorData authorData = XMLRecordParser.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getOtherNames());
		assertEquals("Andrew C. Porteus", authorData.getOtherNames().get(0));
		String jsonData = JsonWriter.create(authorData);
		assertNotNull(jsonData);
	}

	@Test
	void testAuthorSummaryXMLParser() throws Exception {
		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("record_0000-0001-5004-5918.xml"));
		AuthorSummary authorSummary = XMLRecordParser.VTDParseAuthorSummary(xml.getBytes());
		authorSummary.setBase64CompressData(ArgumentApplicationParser.compressArgument(xml));
		OrcidClientTest.logToFile(testPath, JsonWriter.create(authorSummary));
	}

	@Test
	void testWorkDataXMLParser() throws Exception {
		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("activity_work_0000-0003-2760-1191.xml"));
		WorkDetail workDetail = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());
		Work work = new Work();
		work.setWorkDetail(workDetail);
		work.setBase64CompressData(ArgumentApplicationParser.compressArgument(xml));
		OrcidClientTest.logToFile(testPath, JsonWriter.create(work));
	}
}
