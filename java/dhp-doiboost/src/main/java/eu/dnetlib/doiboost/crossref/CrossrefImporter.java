
package eu.dnetlib.doiboost.crossref;

import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.Inflater;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class CrossrefImporter {

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							CrossrefImporter.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/doiboost/import_from_es.json"))));

		parser.parseArgument(args);

		final String namenode = parser.get("namenode");
		System.out.println("namenode: " + namenode);

		Path targetPath = new Path(parser.get("targetPath"));
		System.out.println("targetPath: " + targetPath);

		final Long timestamp = Optional
			.ofNullable(parser.get("timestamp"))
			.map(s -> {
				try {
					return Long.parseLong(s);
				} catch (NumberFormatException e) {
					return -1L;
				}
			})
			.orElse(-1L);
		System.out.println("timestamp: " + timestamp);

		final String esServer = parser.get("esServer");
		System.out.println("esServer: " + esServer);

		final String esIndex = parser.get("esIndex");
		System.out.println("esIndex: " + esIndex);

		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", namenode);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		// "ip-90-147-167-25.ct1.garrservices.it", "crossref"
		final ESClient client = new ESClient(esServer, esIndex, timestamp);

		try (SequenceFile.Writer writer = SequenceFile
			.createWriter(
				conf,
				SequenceFile.Writer.file(targetPath),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Text.class))) {

			int i = 0;
			long start = System.currentTimeMillis();
			long end = 0;
			final IntWritable key = new IntWritable(i);
			final Text value = new Text();
			while (client.hasNext()) {
				key.set(i++);
				value.set(client.next());
				writer.append(key, value);
				if (i % 100000 == 0) {
					end = System.currentTimeMillis();
					final float time = (end - start) / 1000.0F;
					System.out
						.println(String.format("Imported %s records last 100000 imported in %s seconds", i, time));
					start = System.currentTimeMillis();
				}
			}
		}
	}

	public static String decompressBlob(final String blob) {
		try {
			byte[] byteArray = Base64.decodeBase64(blob.getBytes());
			final Inflater decompresser = new Inflater();
			decompresser.setInput(byteArray);
			final ByteArrayOutputStream bos = new ByteArrayOutputStream(byteArray.length);
			byte[] buffer = new byte[8192];
			while (!decompresser.finished()) {
				int size = decompresser.inflate(buffer);
				bos.write(buffer, 0, size);
			}
			decompresser.end();
			return bos.toString();
		} catch (Throwable e) {
			throw new RuntimeException("Wrong record:" + blob, e);
		}
	}
}
