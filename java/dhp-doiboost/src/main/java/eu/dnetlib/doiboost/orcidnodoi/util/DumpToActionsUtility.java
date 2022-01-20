
package eu.dnetlib.doiboost.orcidnodoi.util;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Utility class
 */

public class DumpToActionsUtility {

	private static final SimpleDateFormat ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US);

	public static String getStringValue(final JsonObject root, final String key) {
		if (root.has(key) && !root.get(key).isJsonNull())
			return root.get(key).getAsString();
		return "";
	}

	public static List<String> getArrayValues(final JsonObject root, final String key) {
		if (root.has(key) && root.get(key).isJsonArray()) {
			final JsonArray asJsonArray = root.get(key).getAsJsonArray();
			final List<String> result = new ArrayList<>();

			asJsonArray.forEach(it -> {
				if (StringUtils.isNotBlank(it.getAsString())) {
					result.add(it.getAsString());
				}
			});
			return result;
		}
		return new ArrayList<>();
	}

	public static List<JsonObject> getArrayObjects(final JsonObject root, final String key) {
		if (root.has(key) && root.get(key).isJsonArray()) {
			final JsonArray asJsonArray = root.get(key).getAsJsonArray();
			final List<JsonObject> result = new ArrayList<>();
			asJsonArray.forEach(it -> {
				if (it.getAsJsonObject() != null) {
					result.add(it.getAsJsonObject());
				}
			});
			return result;
		}
		return new ArrayList<>();
	}

	public static boolean isValidDate(final String date) {
		return date.matches("\\d{4}-\\d{2}-\\d{2}");
	}

	public static String now_ISO8601() { // NOPMD
		String result;
		synchronized (ISO8601FORMAT) {
			result = ISO8601FORMAT.format(new Date());
		}
		// convert YYYYMMDDTHH:mm:ss+HH00 into YYYYMMDDTHH:mm:ss+HH:00
		// - note the added colon for the Timezone
		return result.substring(0, result.length() - 2) + ":" + result.substring(result.length() - 2);
	}

	public static String getDefaultResulttype(final String cobjcategory) {
		switch (cobjcategory) {
			case "0029":
				return "software";
			case "0021":
			case "0024":
			case "0025":
			case "0030":
				return "dataset";
			case "0000":
			case "0010":
			case "0018":
			case "0020":
			case "0022":
			case "0023":
			case "0026":
			case "0027":
			case "0028":
			case "0037":
				return "other";
			case "0001":
			case "0002":
			case "0004":
			case "0005":
			case "0006":
			case "0007":
			case "0008":
			case "0009":
			case "0011":
			case "0012":
			case "0013":
			case "0014":
			case "0015":
			case "0016":
			case "0017":
			case "0019":
			case "0031":
			case "0032":
				return "publication";
			default:
				return "publication";
		}
	}

}
