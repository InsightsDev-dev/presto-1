package com.facebook.presto.connector.proteum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

/**
 * 
 * @author dilip kasana
 * @Date 20-Apr-2015
 */
public class ProteumPredicatePushDown {
	private List<ProteumColumnFilter> columnFilters = new ArrayList<ProteumColumnFilter>();
	private List<Symbol> groupBy = new ArrayList<Symbol>();
	private List<Expression> aggregates = new ArrayList<Expression>();

	@JsonCreator
	public ProteumPredicatePushDown(
			@JsonProperty("columnFilters") List<ProteumColumnFilter> columnFilters,
			@JsonProperty("groupBy") List<Symbol> groupBy,
			@JsonProperty("aggregates") List<Expression> aggregates) {
		this.columnFilters = columnFilters;
		this.groupBy = groupBy;
		this.aggregates = aggregates;
	}

	public ProteumPredicatePushDown(List<ProteumColumnFilter> columnFilters) {
		this.columnFilters = columnFilters;
	}

	@JsonProperty
	public List<ProteumColumnFilter> getColumnFilters() {
		return columnFilters;
	}

	public void setColumnFilters(List<ProteumColumnFilter> columnFilters) {
		this.columnFilters = columnFilters;
	}

	@JsonProperty
	public List<Symbol> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<Symbol> groupBy) {
		if (groupBy == null) {
			groupBy = Collections.<Symbol> emptyList();
		}
		this.groupBy = groupBy;
	}

	@JsonProperty
	public List<Expression> getAggregates() {
		return aggregates;
	}

	public void setAggregates(List<Expression> aggregates) {
		if (aggregates == null) {
			aggregates = Collections.<Expression> emptyList();
		}
		this.aggregates = aggregates;
	}

}
