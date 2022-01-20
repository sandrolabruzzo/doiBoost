
package eu.dnetlib.doiboost.orcid.json;

import com.google.gson.Gson;

import eu.dnetlib.dhp.schema.orcid.WorkDetail;

public class JsonHelper {

	private JsonHelper() {
	}

	public static String createOidWork(WorkDetail workData) {
		return new Gson().toJson(workData);
	}
}
