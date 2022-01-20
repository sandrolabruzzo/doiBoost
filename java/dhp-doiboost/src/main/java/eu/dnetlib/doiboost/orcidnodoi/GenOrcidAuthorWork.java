
package eu.dnetlib.doiboost.orcidnodoi;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.OrcidDSManager;

/**
 * This job generates one sequence file, the key is an orcid identifier and the
 * value is an orcid publication in json format
 */
public class GenOrcidAuthorWork extends OrcidDSManager {

	private String activitiesFileNameTarGz;
	private String outputWorksPath;

	public static void main(String[] args) throws Exception {
		GenOrcidAuthorWork genOrcidAuthorWork = new GenOrcidAuthorWork();
		genOrcidAuthorWork.loadArgs(args);
		genOrcidAuthorWork.generateAuthorsDOIsData();
	}

	public void generateAuthorsDOIsData() throws Exception {
		Configuration conf = initConfigurationObject();
		String tarGzUri = hdfsServerUri.concat(workingPath).concat(activitiesFileNameTarGz);
		Path outputPath = new Path(hdfsServerUri.concat(workingPath).concat(outputWorksPath));
		ActivitiesDumpReader.parseGzActivities(conf, tarGzUri, outputPath);
	}

	private void loadArgs(String[] args) throws ParseException, IOException {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenOrcidAuthorWork.class
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
}
