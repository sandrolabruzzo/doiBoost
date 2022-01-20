
package eu.dnetlib.doiboost.orcid;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class OrcidAuthorsDOIsDataGen extends OrcidDSManager {

	private String activitiesFileNameTarGz;
	private String outputAuthorsDOIsPath;

	public static void main(String[] args) throws Exception {
		OrcidAuthorsDOIsDataGen orcidAuthorsDOIsDataGen = new OrcidAuthorsDOIsDataGen();
		orcidAuthorsDOIsDataGen.loadArgs(args);
		orcidAuthorsDOIsDataGen.generateAuthorsDOIsData();
	}

	public void generateAuthorsDOIsData() throws IOException {
		Configuration conf = initConfigurationObject();
		String tarGzUri = hdfsServerUri.concat(workingPath).concat(activitiesFileNameTarGz);
		Path outputPath = new Path(hdfsServerUri.concat(workingPath).concat(outputAuthorsDOIsPath));
		ActivitiesDecompressor.parseGzActivities(conf, tarGzUri, outputPath);
	}

	private void loadArgs(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					OrcidAuthorsDOIsDataGen.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/create_orcid_authors_dois_data.json")));
		parser.parseArgument(args);

		hdfsServerUri = parser.get("hdfsServerUri");
		Log.info("HDFS URI: " + hdfsServerUri);
		workingPath = parser.get("workingPath");
		Log.info("Default Path: " + workingPath);
		activitiesFileNameTarGz = parser.get("activitiesFileNameTarGz");
		Log.info("Activities File Name: " + activitiesFileNameTarGz);
		outputAuthorsDOIsPath = parser.get("outputAuthorsDOIsPath");
		Log.info("Output Authors DOIs Data: " + outputAuthorsDOIsPath);
	}
}
