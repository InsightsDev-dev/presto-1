package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.execution.QueryId;
/**
 * 
 * @author Dilip kasana
 * @date 10 Nov 2015
 *
 */
public interface RuntimeContext {
	public QueryId getQueryId();

	public void setQueryId(QueryId queryId);
}
