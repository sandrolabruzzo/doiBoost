
package eu.dnetlib.doiboost.orcid;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ExtractXMLActivitiesData extends OrcidDSManager {
	private String outputWorksPath;
	private String activitiesFileNameTarGz;

	public static void main(String[] args) throws Exception {
		ExtractXMLActivitiesData extractXMLActivitiesData = new ExtractXMLActivitiesData();
		extractXMLActivitiesData.loadArgs(args);
		extractXMLActivitiesData.extractWorks();
	}

	private void loadArgs(String[] args) throws ParseException, IOException {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ExtractXMLActivitiesData.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_orcid_works-no-doi_from_activities.json")));
		parser.parseArgument(args);

		hdfsServerUri = parser.get("hdfsServerUri");
		Log.info("HDFS URI: " + hdfsServerUri);
		workingPath = parser.get("workingPath");
		Log.info("Working Path: " + workingPath);
		activitiesFileNameTarGz = parser.get("activitiesFileNameTarGz");
		Log.info("Activities File Name: " + activitiesFileNameTarGz);
		outputWorksPath = parser.get("outputWorksPath");
		Log.info("Output Author Work Data: " + outputWorksPath);
	}

	private void extractWorks() throws Exception {
		Configuration conf = initConfigurationObject();
		String tarGzUri = hdfsServerUri.concat(workingPath).concat(activitiesFileNameTarGz);
		Path outputPath = new Path(
			hdfsServerUri
				.concat(workingPath)
				.concat(outputWorksPath));
		ActivitiesDecompressor.extractXML(conf, tarGzUri, outputPath);
	}
}
