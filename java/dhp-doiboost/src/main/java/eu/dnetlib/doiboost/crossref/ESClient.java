
package eu.dnetlib.doiboost.crossref;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.jayway.jsonpath.JsonPath;

public class ESClient implements Iterator<String> {

	private static final String BLOB_PATH = "$.hits.hits[*]._source.blob";
	private static final String SCROLL_ID_PATH = "$._scroll_id";
	private static final String JSON_NO_TS = "{\"size\":1000}";
	private static final String JSON_WITH_TS = "{\"size\":1000, \"query\":{\"range\":{\"timestamp\":{\"gte\":%d}}}}";
	private static final String JSON_SCROLL = "{\"scroll_id\":\"%s\",\"scroll\" : \"1m\"}";

	public static final String APPLICATION_JSON = "application/json";

	public static final String ES_SEARCH_URL = "http://%s:9200/%s/_search?scroll=1m";
	public static final String ES_SCROLL_URL = "http://%s:9200/_search/scroll";

	private final String scrollId;

	private List<String> buffer;

	private final String esHost;

	public ESClient(final String esHost, final String esIndex, final long timestamp) {
		this.esHost = esHost;

		final String body = timestamp > 0
			? getResponse(String.format(ES_SEARCH_URL, esHost, esIndex), String.format(JSON_WITH_TS, timestamp))
			: getResponse(String.format(ES_SEARCH_URL, esHost, esIndex), JSON_NO_TS);
		scrollId = getJPathString(SCROLL_ID_PATH, body);
		buffer = getBlobs(body);
	}

	private String getResponse(final String url, final String json) {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpPost httpPost = new HttpPost(url);
			if (json != null) {
				StringEntity entity = new StringEntity(json);
				httpPost.setEntity(entity);
				httpPost.setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON);
				httpPost.setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
			}
			try (CloseableHttpResponse response = client.execute(httpPost)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		} catch (IOException e) {
			throw new IllegalStateException("Error on executing request ", e);
		}
	}

	private String getJPathString(final String jsonPath, final String json) {
		try {
			Object o = JsonPath.read(json, jsonPath);
			if (o instanceof String)
				return (String) o;
			return null;
		} catch (Exception e) {
			return "";
		}
	}

	private List<String> getBlobs(final String body) {
		final List<String> res = JsonPath.read(body, BLOB_PATH);
		return res;
	}

	@Override
	public boolean hasNext() {
		return (buffer != null && !buffer.isEmpty());
	}

	@Override
	public String next() {
		final String nextItem = buffer.remove(0);
		if (buffer.isEmpty()) {

			final String json_param = String.format(JSON_SCROLL, scrollId);
			final String body = getResponse(String.format(ES_SCROLL_URL, esHost), json_param);
			try {
				buffer = getBlobs(body);
			} catch (Throwable e) {
				System.out.println("Error on  get next page: body:" + body);
			}
		}
		return nextItem;
	}
}
