
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

import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;

public class ActivitiesDecompressor {

	private static final int MAX_XML_WORKS_PARSED = -1;
	private static final int XML_WORKS_PARSED_COUNTER_LOG_INTERVAL = 100000;

	private ActivitiesDecompressor() {
	}

	public static void parseGzActivities(Configuration conf, String inputUri, Path outputPath) throws IOException {
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
			parseTarActivities(conf, gzipInputStream, outputPath);

		} finally {
			Log.debug("Closing gzip stream");
			IOUtils.closeStream(gzipInputStream);
		}
	}

	private static void parseTarActivities(Configuration conf, InputStream gzipInputStream, Path outputPath) {
		int counter = 0;
		int doiFound = 0;
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
						if (entry.isDirectory() || !filename.contains("works")) {

						} else {
							Log.debug("XML work entry name: " + entry.getName());
							counter++;
							BufferedReader br = new BufferedReader(new InputStreamReader(tais)); // Read directly from
																									// tarInput
							String line;
							StringBuilder builder = new StringBuilder();
							while ((line = br.readLine()) != null) {
								builder.append(line);
							}
							WorkData workData = XMLRecordParser.VTDParseWorkData(builder.toString().getBytes());
							if (workData != null) {
								if (workData.getErrorCode() != null) {
									errorFromOrcidFound += 1;
									Log
										.debug(
											"error from Orcid with code "
												+ workData.getErrorCode()
												+ " for entry "
												+ entry.getName());
									continue;
								}
								if (workData.isDoiFound()) {
									String jsonData = JsonWriter.create(workData);
									Log.debug("oid: " + workData.getOid() + " data: " + jsonData);

									final Text key = new Text(workData.getOid());
									final Text value = new Text(jsonData);

									try {
										writer.append(key, value);
									} catch (IOException e) {
										Log.debug("Writing to sequence file: " + e.getMessage());
										Log.debug(e);
										throw new RuntimeException(e);
									}
									doiFound += 1;
								}

							} else {
								Log.warn("Data not retrievable [" + entry.getName() + "] " + builder);
								xmlParserErrorFound += 1;
							}
						}
					} catch (Exception e) {
						Log
							.warn(
								"Parsing work from tar archive and xml work: " + filename + "  " + e.getMessage());
						Log.warn(e);
					}

					if ((counter % XML_WORKS_PARSED_COUNTER_LOG_INTERVAL) == 0) {
						Log.info("Current xml works parsed: " + counter);
					}

					if ((MAX_XML_WORKS_PARSED > -1) && (counter > MAX_XML_WORKS_PARSED)) {
						break;
					}
				}
			}
		} catch (IOException e) {
			Log.warn("Parsing work from gzip archive: " + e.getMessage());
			Log.warn(e);
			throw new RuntimeException(e);
		}
		Log.info("Activities parse completed");
		Log.info("Total XML works parsed: " + counter);
		Log.info("Total doi found: " + doiFound);
		Log.info("Error from Orcid found: " + errorFromOrcidFound);
		Log.info("Error parsing xml work found: " + xmlParserErrorFound);
	}

	public static void extractXML(Configuration conf, String inputUri, Path outputPath)
		throws Exception {
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
				try (SequenceFile.Writer writer = SequenceFile
					.createWriter(
						conf,
						SequenceFile.Writer.file(outputPath),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class),
						SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new GzipCodec()))) {
					while ((entry = tais.getNextTarEntry()) != null) {
						String filename = entry.getName();
						if (entry.isDirectory() || !filename.contains("works")) {
						} else {
							counter++;
							BufferedReader br = new BufferedReader(new InputStreamReader(tais));
							String line;
							StringBuilder builder = new StringBuilder();
							while ((line = br.readLine()) != null) {
								builder.append(line);
							}
							String xml = builder.toString();
							String[] filenameParts = filename.split("/");
							final Text key = new Text(
								XMLRecordParser
									.retrieveOrcidIdFromActivity(
										xml.getBytes(), filenameParts[filenameParts.length - 1]));
							final Text value = new Text(xml);
							writer.append(key, value);
							if ((counter % 100000) == 0) {
								Log.info("Current xml works extracted: " + counter);
							}
						}
					}
				}
			}
			Log.info("Activities extraction completed");
			Log.info("Total XML works parsed: " + counter);
		} finally {
			Log.debug("Closing gzip stream");
			IOUtils.closeStream(gzipInputStream);
		}
	}

}
