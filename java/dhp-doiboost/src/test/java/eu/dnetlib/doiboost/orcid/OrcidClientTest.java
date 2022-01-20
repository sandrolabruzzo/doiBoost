
package eu.dnetlib.doiboost.orcid;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.util.DownloadsReport;
import eu.dnetlib.doiboost.orcid.util.MultiAttemptsHttpConnector;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class OrcidClientTest {
	final int REQ_LIMIT = 24;
	final int REQ_MAX_TEST = 100;
	final int RECORD_DOWNLOADED_COUNTER_LOG_INTERVAL = 10;
	final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	final String toRetrieveDate = "2020-05-06 23:59:46.031145";
	String toNotRetrieveDate = "2019-09-29 23:59:59.000000";
	String lastUpdate = "2019-09-30 00:00:00";
	String shortDate = "2020-05-06 16:06:11";
	final String REQUEST_TYPE_RECORD = "record";
	final String REQUEST_TYPE_WORK = "work/47652866";
	final String REQUEST_TYPE_WORKS = "works";

	private static Path testPath;

	@BeforeAll
	private static void setUp() throws IOException {
		testPath = Files.createTempDirectory(OrcidClientTest.class.getName());
		System.out.println("using test path: " + testPath);
	}

//	curl -i -H "Accept: application/vnd.orcid+xml"
//	-H 'Authorization: Bearer 78fdb232-7105-4086-8570-e153f4198e3d'
//	'https://api.orcid.org/v3.0/0000-0001-7291-3210/record'

	@Test
	void downloadTest() throws Exception {
		final String orcid = "0000-0001-7291-3210";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_RECORD);
		String filename = testPath + "/downloaded_record_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	private String testDownloadRecord(String orcidId, String dataType) throws Exception {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/" + dataType);
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", "Bearer 78fdb232-7105-4086-8570-e153f4198e3d");
			long start = System.currentTimeMillis();
			CloseableHttpResponse response = client.execute(httpGet);
			long end = System.currentTimeMillis();
			if (response.getStatusLine().getStatusCode() != 200) {
				logToFile(
					testPath, "Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
			}
			logToFile(testPath, orcidId + " " + dataType + " " + (end - start) / 1000 + " seconds");
			return IOUtils.toString(response.getEntity().getContent());
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return "";
	}

	// @Test
	private void testLambdaFileParser() throws Exception {
		try (BufferedReader br = new BufferedReader(
			new InputStreamReader(this.getClass().getResourceAsStream("last_modified.csv")))) {
			String line;
			int counter = 0;
			int nReqTmp = 0;
			long startDownload = System.currentTimeMillis();
			long startReqTmp = System.currentTimeMillis();
			while ((line = br.readLine()) != null) {
				counter++;
//		    	skip headers line
				if (counter == 1) {
					continue;
				}
				String[] values = line.split(",");
				List<String> recordInfo = Arrays.asList(values);
				testDownloadRecord(recordInfo.get(0), REQUEST_TYPE_RECORD);
				long endReq = System.currentTimeMillis();
				nReqTmp++;
				if (nReqTmp == REQ_LIMIT) {
					long reqSessionDuration = endReq - startReqTmp;
					if (reqSessionDuration <= 1000) {
						System.out
							.println(
								"\nreqSessionDuration: " + reqSessionDuration + " nReqTmp: " + nReqTmp + " wait ....");
						Thread.sleep(1000 - reqSessionDuration);
					} else {
						nReqTmp = 0;
						startReqTmp = System.currentTimeMillis();
					}
				}

				if (counter > REQ_MAX_TEST) {
					break;
				}
				if ((counter % RECORD_DOWNLOADED_COUNTER_LOG_INTERVAL) == 0) {
					System.out.println("Current record downloaded: " + counter);
				}
			}
			long endDownload = System.currentTimeMillis();
			long downloadTime = endDownload - startDownload;
			System.out.println("Download time: " + ((downloadTime / 1000) / 60) + " minutes");
		}
	}

	// @Test
	private void getRecordDatestamp() throws ParseException {
		Date toRetrieveDateDt = new SimpleDateFormat(DATE_FORMAT).parse(toRetrieveDate);
		Date toNotRetrieveDateDt = new SimpleDateFormat(DATE_FORMAT).parse(toNotRetrieveDate);
		Date lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdate);
		assertTrue(toRetrieveDateDt.after(lastUpdateDt));
		assertTrue(!toNotRetrieveDateDt.after(lastUpdateDt));
	}

	private void testDate(String value) throws ParseException {
		System.out.println(value);
		if (value.length() != 19) {
			value = value.substring(0, 19);
		}
		Date valueDt = new SimpleDateFormat(DATE_FORMAT).parse(value);
		System.out.println(valueDt.toString());
	}

	// @Test
	@Ignore
	private void testModifiedDate() throws ParseException {
		testDate(toRetrieveDate);
		testDate(toNotRetrieveDate);
		testDate(shortDate);
	}

	@Test
	@Disabled
	void testReadBase64CompressedRecord() throws Exception {
		final String base64CompressedRecord = IOUtils
			.toString(getClass().getResourceAsStream("0000-0003-3028-6161.compressed.base64"));
		final String recordFromSeqFile = ArgumentApplicationParser.decompressValue(base64CompressedRecord);
		logToFile(testPath, "\n\ndownloaded \n\n" + recordFromSeqFile);
		final String downloadedRecord = testDownloadRecord("0000-0003-3028-6161", REQUEST_TYPE_RECORD);
		assertEquals(recordFromSeqFile, downloadedRecord);
	}

	@Test
	@Disabled
	void lambdaFileReaderTest() throws Exception {
		String last_update = "2021-01-12 00:00:06.685137";
		TarArchiveInputStream input = new TarArchiveInputStream(
			new GzipCompressorInputStream(new FileInputStream("/tmp/last_modified.csv.tar")));
		TarArchiveEntry entry = input.getNextTarEntry();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		int rowNum = 1;
		int modifiedNum = 1;
		int entryNum = 0;
		boolean firstNotModifiedFound = false;
		while (entry != null) {
			br = new BufferedReader(new InputStreamReader(input)); // Read directly from tarInput
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				List<String> recordInfo = Arrays.asList(values);
				assertEquals(4, recordInfo.size());
				String orcid = recordInfo.get(0);
				String modifiedDate = recordInfo.get(3);
				rowNum++;
				if (rowNum == 2) {
					assertTrue(recordInfo.get(3).equals("last_modified"));
				} else {
//					SparkDownloadOrcidAuthors.lastUpdate = last_update;
//					boolean isModified = SparkDownloadOrcidAuthors.isModified(orcid, modifiedDate);
//					if (isModified) {
//						modifiedNum++;
//					} else {
//						if (!firstNotModifiedFound) {
//							firstNotModifiedFound = true;
//							logToFile(orcid + " - " + modifiedDate + " > " + isModified);
//						}
//					}

				}
			}
			entryNum++;
			assertTrue(entryNum == 1);
			entry = input.getNextTarEntry();

		}
		logToFile(testPath, "modifiedNum : " + modifiedNum + " / " + rowNum);
	}

	public static void logToFile(Path basePath, String log) throws IOException {
		log = log.concat("\n");
		Path path = basePath.resolve("orcid_log.txt");
		if (!Files.exists(path)) {
			Files.createFile(path);
		}
		Files.write(path, log.getBytes(), StandardOpenOption.APPEND);
	}

	@Test
	@Disabled
	private void slowedDownDownloadTest() throws Exception {
		String orcid = "0000-0001-5496-1243";
		String record = slowedDownDownload(orcid);
		String filename = "/tmp/downloaded_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	private String slowedDownDownload(String orcidId) throws Exception {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", "Bearer 78fdb232-7105-4086-8570-e153f4198e3d");
			long start = System.currentTimeMillis();
			CloseableHttpResponse response = client.execute(httpGet);
			long endReq = System.currentTimeMillis();
			long reqSessionDuration = endReq - start;
			logToFile(testPath, "req time (millisec): " + reqSessionDuration);
			if (reqSessionDuration < 1000) {
				logToFile(testPath, "wait ....");
				Thread.sleep(1000 - reqSessionDuration);
			}
			long end = System.currentTimeMillis();
			long total = end - start;
			logToFile(testPath, "total time (millisec): " + total);
			if (response.getStatusLine().getStatusCode() != 200) {
				logToFile(
					testPath, "Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
			}
			return IOUtils.toString(response.getEntity().getContent());
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return "";
	}

	@Test
	void downloadWorkTest() throws Exception {
		String orcid = "0000-0003-0015-1952";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_WORK);
		String filename = "/tmp/downloaded_work_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	void downloadRecordTest() throws Exception {
		String orcid = "0000-0001-5004-5918";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_RECORD);
		String filename = "/tmp/downloaded_record_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	void downloadWorksTest() throws Exception {
		String orcid = "0000-0001-5004-5918";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_WORKS);
		String filename = "/tmp/downloaded_works_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	void downloadSingleWorkTest() throws Exception {
		String orcid = "0000-0001-5004-5918";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_WORK);
		String filename = "/tmp/downloaded_work_47652866_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	void cleanAuthorListTest() throws Exception {
		AuthorData a1 = new AuthorData();
		a1.setOid("1");
		a1.setName("n1");
		a1.setSurname("s1");
		a1.setCreditName("c1");
		AuthorData a2 = new AuthorData();
		a2.setOid("1");
		a2.setName("n1");
		a2.setSurname("s1");
		a2.setCreditName("c1");
		AuthorData a3 = new AuthorData();
		a3.setOid("3");
		a3.setName("n3");
		a3.setSurname("s3");
		a3.setCreditName("c3");
		List<AuthorData> list = Lists.newArrayList();
		list.add(a1);
		list.add(a2);
		list.add(a3);

		Set<String> namesAlreadySeen = new HashSet<>();
		assertTrue(list.size() == 3);
		list.removeIf(a -> !namesAlreadySeen.add(a.getOid()));
		assertTrue(list.size() == 2);
	}

	@Test
	@Ignore
	void testUpdatedRecord() throws Exception {
		final String base64CompressedRecord = IOUtils
			.toString(getClass().getResourceAsStream("0000-0003-3028-6161.compressed.base64"));
		final String record = ArgumentApplicationParser.decompressValue(base64CompressedRecord);
		logToFile(testPath, "\n\nrecord updated \n\n" + record);
	}

	@Test
	@Ignore
	void testUpdatedWork() throws Exception {
		final String base64CompressedWork = "H4sIAAAAAAAAAM1XS2/jNhC+51cQOuxJsiXZSR03Vmq0G6Bo013E6R56oyXaZiOJWpKy4y783zvUg5Ksh5uiCJogisX5Zjj85sHx3f1rFKI94YKyeGE4I9tAJPZZQOPtwvj9+cGaGUhIHAc4ZDFZGEcijHvv6u7A+MtcPVCSSgsUQObYzuzaccBEguVuYYxt+LHgbwKP6a11M3WnY6UzrpB7KuiahlQeF0aSrkPqGwhcisWcxpLwGIcLYydlMh+PD4fDiHGfBvDcjmMxLhGlBglSH8vsIH0qGlLqBFRIGvvDWjWQ1iMJJ2CKBANqGlNqMbkj3IpxRPq1KkypFZFoDRHa0aRfq8JoNjhnfIAJJS6xPouiIQJyeYmGQzE+cO5cXqITcItBlKyASExD0a93jiwtvJDjYXDDAqBPHoH2wMmVWGNf8xyyaEBiSTeUDHHWBpd2Nmmc10yfbgHQrHCyIRxKjQwRUoFKPRwEnIgBnQJQVdGeQgJaCRN0OMnPkaUFVbD9WkpaIndQJowf+8EFoIpTErJjBFQOBavElFpfUxwC9ZcqvQErdQXhe+oPFF8BaObupYzVsYEOARzSoZBWmKqaBMHcV0Wf8oG0beIqD+Gdkz0lhyE3NajUW6fhQFSV9Nw/MCBYyofYa0EN7wrBz13eP+Y+J6obWgE8Pdd2JpYD94P77Ezmjj13b0bu5PqPu3EXumEnxEJaEVxSUIHammsra+53z44zt2/m1/bItaeVtQ6dhs3c4XytvW75IYUchMKvEHVUyqmnWBFAS0VJrqSvQde6vp251ux2NtFuKcVOi+oK9YY0M0Cn6o4J6WkvtEK2XJ1vfPGAZxSoK8lb+SxJBbLQx1CohOLndjJUywQWUFmqEi3G6Zaqf/7buOyYJd5IYpfmf0XipfP18pDR9cQCeEuJQI/Lx36bFbVnpBeL2UwmqQw7ApAvf4GeGGQdEbENgolui/wdpjHaYCmPCIPPAmGBIsxfoLUhyRCB0SeCakEBJRKBtfJ+UBbI15TG4PaGBAhWthx8DmFYtHZQujv1CWbLLdzmmUKmHEOWCe1/zdu78bn/+YH+hCOqOzcXfFwuP6OVT/P710crwqGXFrpNaM2GT3MXarw01i15TIi3pmtJXgtbTVGf3h6HKfF+wBAnPyTfdCChudlm5gZaoG//F9pPZsGQcqqbyZN5hBau5OoIJ3PPwjTKDuG4s5MZp2rMzF5PZoK34IT6PIFOPrk+mTiVO5aJH2C+JJRjE/06eoRfpJxa4VgyYaLlaJUv/EhCfATMU/76gEOfmehL/qbJNNHjaFna+CQYB8wvo9PpPFJ5MOrJ1Ix7USBZqBl7KRNOx1d3jex7SG6zuijqCMWRusBsncjZSrM2u82UJmqzpGhvUJN2t6caIM9QQgO9c0t40UROnWsJd2Rbs+nsxpna9u30ttNkjechmzHjEST+X5CkkuNY0GzQkzyFseAf7lSZuLwdh1xSXKvvQJ4g4abTYgPV7uMt3rskohlJmMa82kQkshtyBEIYqQ+YB8X3oRHg7iFKi/bZP+Ao+T6BJhIT/vNPi8ffZs+flk+r2v0WNroZiyWn6xRmadHqTJXsjLJczElAZX6TnJdoWTM1SI2gfutv3rjeBt5t06rVvNuWup29246tlvluO+u2/G92bK9DXheL6uFd/Q3EaRDZqBIAAA==";
		final String work = ArgumentApplicationParser.decompressValue(base64CompressedWork);
		logToFile(testPath, "\n\nwork updated \n\n" + work);
	}

	@Test
	void downloadUnknownHostExceptionTest() throws Exception {
		logToFile(testPath, "downloadUnknownHostExceptionTest");
		final String orcid = "0000-0001-7291-3210";
		final HttpClientParams clientParams = new HttpClientParams();
		clientParams.setMaxNumberOfRetry(2);
		MultiAttemptsHttpConnector httpConnector = new MultiAttemptsHttpConnector(clientParams);
		httpConnector.setAuthMethod(MultiAttemptsHttpConnector.BEARER);
		httpConnector.setAcceptHeaderValue("application/vnd.orcid+xml");
		httpConnector.setAuthToken("78fdb232-7105-4086-8570-e153f4198e3d");
		String wrongApiUrl = "https://api.orcid_UNKNOWN.org/v3.0/" + orcid + "/" + REQUEST_TYPE_RECORD;
		String url = "UNKNOWN";
		DownloadsReport report = new DownloadsReport();
		try {
			httpConnector.getInputSource(wrongApiUrl, report);
		} catch (CollectorException ce) {
			logToFile(testPath, "CollectorException downloading: " + ce.getMessage());
		} catch (Throwable t) {
			logToFile(testPath, "Throwable downloading: " + t.getMessage());
		}
	}

	@Test
	void downloadAttemptSuccessTest() throws Exception {
		logToFile(testPath, "downloadAttemptSuccessTest");
		final String orcid = "0000-0001-7291-3210";
		final HttpClientParams clientParams = new HttpClientParams();
		clientParams.setMaxNumberOfRetry(2);
		MultiAttemptsHttpConnector httpConnector = new MultiAttemptsHttpConnector(clientParams);
		httpConnector.setAuthMethod(MultiAttemptsHttpConnector.BEARER);
		httpConnector.setAcceptHeaderValue("application/vnd.orcid+xml");
		httpConnector.setAuthToken("78fdb232-7105-4086-8570-e153f4198e3d");
		String apiUrl = "https://api.orcid.org/v3.0/" + orcid + "/" + REQUEST_TYPE_RECORD;
		String url = "UNKNOWN";
		DownloadsReport report = new DownloadsReport();
		String record = httpConnector.getInputSource(apiUrl, report);
		logToFile(testPath, "Downloaded at first attempt record: " + record);
	}

	@Test
	void downloadAttemptNotFoundTest() throws Exception {
		logToFile(testPath, "downloadAttemptNotFoundTest");
		final HttpClientParams clientParams = new HttpClientParams();
		clientParams.setMaxNumberOfRetry(2);
		MultiAttemptsHttpConnector httpConnector = new MultiAttemptsHttpConnector(clientParams);
		httpConnector.setAuthMethod(MultiAttemptsHttpConnector.BEARER);
		httpConnector.setAcceptHeaderValue("application/vnd.orcid+xml");
		httpConnector.setAuthToken("78fdb232-7105-4086-8570-e153f4198e3d");
		String apiUrl = "https://api.orcid.org/v3.0/NOTFOUND/" + REQUEST_TYPE_RECORD;
		DownloadsReport report = new DownloadsReport();
		try {
			httpConnector.getInputSource(apiUrl, report);
		} catch (CollectorException ce) {

		}
		report.forEach((k, v) -> {
			try {
				logToFile(testPath, k + " " + v);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	@Test
	@Ignore
	void testDownloadedAuthor() throws Exception {
		final String base64CompressedWork = "H4sIAAAAAAAAAI2Yy26jMBSG932KiD0hIe1MiwiVZjGLkWbX2XRHsFOsgs3YJmnefszFFy4+mUhtVPz9P/gcH/vQ9PWrrjYXzAVh9Bjst7tgg2nBEKEfx+DP28/wOdgImVOUV4ziY3DDInjNHlKOC8ZRMnxtmlyWxyDaqU+ofg7h/uX7IYwfn+Ngo25ARUKoxJzm1TEopWySKLper1vGC4LU74+IikgTWoFRW+SyfyyfxCBag4iQhBawyoGMDjdqJrnECJAZRquYLDEPaV5jv8oyWlXj+qTiXZLGr7KMiQbnjAOR6IY1W7C6hgIwjGt6SKGfHsY13ajHYipLIcIyJ5Xw6+akdvjEtyt4wxEwM6+VGph5N2zYr2ENhQRhKsmZYChmS1j7nFs6VIBPOwImKhyfMVeFg6GAWEjrcoQ4FoBmBGwVXYhagGHDBIEX+ZzUDiqyn35VN6rJUpUJ4zc/PAI2T03FbrUKJZQszWjV3zavVOjvVfoE01qB+YUUQPGNwHTt3luxJjdqh1AxJFBKLWOrSeCcF13RtxxYtlPOPqH6m+MLwVfoMQ2kdae2ArLajc6fTxkI1nIoegs0yB426pMO+0fSw07xDKMu0XKSde5C2VvrlVMijRzFwqY7XTJI1QMLWcmEzMxtDdxfHiYSgTNJnYJ1K9y5k0tUrMgrnGGaRiuXxxuClulYUbr0nBvpkYLjvgTCGsuSoex3f1CEvRPHKI184NJKtKeaiO7cD5E61bJ4F+9DFd7d01u8Tw6H5BBvvz8f3q3nXLGIeJULGdaqeVBBRK7rS7h/fNvvk/gpedxt4923dxP7Fc3KtKuc1BhlkrfYmeN4dcmrhmbw60+HmWw2CKgbTuqc32CXKTTmeTWT6bDBjPsQ0DTpnchdaYO0ayQ2FyLIiVREqs25aU8VKYLRbK0BsyZuqvr1MU2Sm/rDdhe/2CRN6FU/b+oBVyj1zqRtC5F8kAumfTclsl+s7EoNQu64nfOaVLeezX60Z3XCULLi6GI2IZGTEeey7fec9lBAuXawIHKcpifE7GABHWfoxLVfpUNPBXoMbZWrHFsR3bPAk9J9i2sw9nW6AQT1mpk++7JhW+v44Hmt8PomJqfD13jRnvFOSxCKtu6qHoyBbQ7cMFo750UEfGaXm6bEeplXIXj2hvL6mA7tzvIwmM9pbJFBG834POZdLGi2gH2u9u0K9HMwn5PTioFWLufzmrS4oNuU9Pkt2rf/2jMs7fMdm2rQTTM+j+49AzToAVuXYA1mD2k0+XdE9vAP+JYR5NcQAAA=";
		final String work = ArgumentApplicationParser.decompressValue(base64CompressedWork);
		logToFile(testPath, "\n\ndownloaded author \n\n" + work);
	}

	@Test
	@Ignore
	void testDownloadedWork() throws Exception {
		final String base64CompressedWork = "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";
		final String work = ArgumentApplicationParser.decompressValue(base64CompressedWork);
		logToFile(testPath, "\n\ndownloaded work \n\n" + work);
	}
}
