package com.facebook.presto.server;

/**
 * 
 * @author dilip kasana
 * @Date 26-Oct-2015
 */
public class RewriteQueryResponse {
	private final String query;
	private final String proteumQueryId;

	public RewriteQueryResponse(String query, String proteumQueryId) {
		this.query = query;
		this.proteumQueryId = proteumQueryId;
	}

	public String getQuery() {
		return query;
	}

	public String getProteumQueryId() {
		return proteumQueryId;
	}
}