package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.execution.QueryId;
/**
 * 
 * @author Dilip kasana
 * @date 10 Nov 2015
 *
 */
public interface IRuntimeContext {
	public QueryId getPrestoQueryId();

	public void setPrestoQueryId(QueryId queryId);
	
	public String getProteumQueryId();

	public void setProteumQueryId(String queryId);
}
