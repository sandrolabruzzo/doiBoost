
package eu.dnetlib.doiboost.orcidnodoi.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;

import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.model.WorkData;

/**
 * This class converts an object to json and viceversa
 */

public class JsonWriter {

	public static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	private JsonWriter() {
	}

	public static String create(AuthorData authorData) throws JsonProcessingException {
		return OBJECT_MAPPER.writeValueAsString(authorData);
	}

	public static String create(Object obj) throws JsonProcessingException {
		return OBJECT_MAPPER.writeValueAsString(obj);
	}

	public static String create(WorkData workData) {
		JsonObject work = new JsonObject();
		work.addProperty("oid", workData.getOid());
		work.addProperty("doi", workData.getDoi());
		return work.toString();
	}
}
