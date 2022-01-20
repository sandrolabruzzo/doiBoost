
package eu.dnetlib.doiboost.orcid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.mortbay.log.Log;

import com.ximpleware.ParseException;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;

public class SummariesDecompressor {

	private static final int MAX_XML_RECORDS_PARSED = -1;

	private SummariesDecompressor() {
	}

	public static void parseGzSummaries(Configuration conf, String inputUri, Path outputPath) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(inputUri), conf);
		Path inputPath = new Path(inputUri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + inputUri);
			System.exit(1);
		}
		CompressionCodecFactory.removeSuffix(inputUri, codec.getDefaultExtension());
		InputStream gzipInputStream = null;
		try {
			gzipInputStream = codec.createInputStream(fs.open(inputPath));
			parseTarSummaries(conf, gzipInputStream, outputPath);

		} finally {
			Log.debug("Closing gzip stream");
			IOUtils.closeStream(gzipInputStream);
		}
	}

	private static void parseTarSummaries(
		Configuration conf, InputStream gzipInputStream, Path outputPath) {
		int counter = 0;
		int nameFound = 0;
		int surnameFound = 0;
		int creditNameFound = 0;
		int otherNamesFound = 0;
		int errorFromOrcidFound = 0;
		int xmlParserErrorFound = 0;
		try (TarArchiveInputStream tais = new TarArchiveInputStream(gzipInputStream)) {
			TarArchiveEntry entry = null;

			try (SequenceFile.Writer writer = SequenceFile
				.createWriter(
					conf,
					SequenceFile.Writer.file(outputPath),
					SequenceFile.Writer.keyClass(Text.class),
					SequenceFile.Writer.valueClass(Text.class))) {
				while ((entry = tais.getNextTarEntry()) != null) {
					String filename = entry.getName();
					try {
						if (entry.isDirectory()) {
							Log.debug("Directory entry name: " + entry.getName());
						} else {
							Log.debug("XML record entry name: " + entry.getName());
							counter++;
							BufferedReader br = new BufferedReader(new InputStreamReader(tais)); // Read directly from
																									// tarInput
							String line;
							StringBuffer buffer = new StringBuffer();
							while ((line = br.readLine()) != null) {
								buffer.append(line);
							}
							AuthorData authorData = XMLRecordParser.VTDParseAuthorData(buffer.toString().getBytes());
							if (authorData != null) {
								if (authorData.getErrorCode() != null) {
									errorFromOrcidFound += 1;
									Log
										.debug(
											"error from Orcid with code "
												+ authorData.getErrorCode()
												+ " for oid "
												+ entry.getName());
									continue;
								}
								String jsonData = JsonWriter.create(authorData);
								Log.debug("oid: " + authorData.getOid() + " data: " + jsonData);

								final Text key = new Text(authorData.getOid());
								final Text value = new Text(jsonData);

								try {
									writer.append(key, value);
								} catch (IOException e) {
									Log.debug("Writing to sequence file: " + e.getMessage());
									Log.debug(e);
									throw new RuntimeException(e);
								}

								if (authorData.getName() != null) {
									nameFound += 1;
								}
								if (authorData.getSurname() != null) {
									surnameFound += 1;
								}
								if (authorData.getCreditName() != null) {
									creditNameFound += 1;
								}
								if (authorData.getOtherNames() != null && authorData.getOtherNames().size() > 1) {
									otherNamesFound += authorData.getOtherNames().size();
								}

							} else {
								Log.warn("Data not retrievable [" + entry.getName() + "] " + buffer);
								xmlParserErrorFound += 1;
							}
						}
					} catch (Exception e) {
						Log
							.warn(
								"Parsing record from tar archive and xml record: "
									+ filename
									+ "  "
									+ e.getMessage());
						Log.warn(e);
					}

					if ((counter % 100000) == 0) {
						Log.info("Current xml records parsed: " + counter);
					}

					if ((MAX_XML_RECORDS_PARSED > -1) && (counter > MAX_XML_RECORDS_PARSED)) {
						break;
					}
				}
			}
		} catch (IOException e) {
			Log.warn("Parsing record from gzip archive: " + e.getMessage());
			Log.warn(e);
			throw new RuntimeException(e);
		}
		Log.info("Summaries parse completed");
		Log.info("Total XML records parsed: " + counter);
		Log.info("Name found: " + nameFound);
		Log.info("Surname found: " + surnameFound);
		Log.info("Credit name found: " + creditNameFound);
		Log.info("Other names found: " + otherNamesFound);
		Log.info("Error from Orcid found: " + errorFromOrcidFound);
		Log.info("Error parsing xml record found: " + xmlParserErrorFound);
	}

	public static void extractXML(Configuration conf, String inputUri, Path outputPath)
		throws IOException, VtdException, ParseException {
		String uri = inputUri;
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}
		CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		InputStream gzipInputStream = null;
		try {
			gzipInputStream = codec.createInputStream(fs.open(inputPath));
			int counter = 0;
			try (TarArchiveInputStream tais = new TarArchiveInputStream(gzipInputStream)) {
				TarArchiveEntry entry = null;
				CompressionCodec Codec = new GzipCodec();
				org.apache.hadoop.io.SequenceFile.Writer.Option optCom = SequenceFile.Writer
					.compression(SequenceFile.CompressionType.RECORD, Codec);
				try (SequenceFile.Writer writer = SequenceFile
					.createWriter(
						conf,
						SequenceFile.Writer.file(outputPath),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class), optCom)) {
					while ((entry = tais.getNextTarEntry()) != null) {
						String filename = entry.getName();
						if (entry.isDirectory()) {
							Log.debug("Directory entry name: " + entry.getName());
						} else {
							Log.debug("XML record entry name: " + entry.getName());
							counter++;
							BufferedReader br = new BufferedReader(new InputStreamReader(tais));
							String line;
							StringBuffer buffer = new StringBuffer();
							while ((line = br.readLine()) != null) {
								buffer.append(line);
							}
							String xml = buffer.toString();
							final Text key = new Text(
								XMLRecordParser
									.retrieveOrcidIdFromSummary(
										xml.getBytes(), filename.split("/")[2].substring(0, 19)));
							final Text value = new Text(xml);
							writer.append(key, value);
						}
						if ((counter % 100000) == 0) {
							Log.info("Current xml records extracted: " + counter);
						}
					}
				}
			}
			Log.info("Summaries extract completed");
			Log.info("Total XML records parsed: " + counter);

		} finally {
			Log.debug("Closing gzip stream");
			IOUtils.closeStream(gzipInputStream);
		}
	}
}
