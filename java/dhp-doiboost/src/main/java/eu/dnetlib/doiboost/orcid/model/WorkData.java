
package eu.dnetlib.doiboost.orcid.model;

import java.io.Serializable;

public class WorkData implements Serializable {

	private String oid;
	private String doi;
	private boolean doiFound = false;

	public boolean isDoiFound() {
		return doiFound;
	}

	public void setDoiFound(boolean doiFound) {
		this.doiFound = doiFound;
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	private String errorCode;
}
