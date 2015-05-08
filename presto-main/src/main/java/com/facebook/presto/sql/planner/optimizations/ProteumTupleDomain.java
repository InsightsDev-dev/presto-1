package com.facebook.presto.sql.planner.optimizations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;

/**
 * 
 * @author dilipsingh
 * @Date 18-Feb-2015
 */
public class ProteumTupleDomain<T> extends TupleDomain<T> {

	private Expression remainingExpresstion;
	private List<Symbol> groupBy;
	private List<Expression> pushDownAggregationList;
	private AtomicBoolean isAggregatePushDownable = new AtomicBoolean(true);
	private Expression minimumExpression;

	protected ProteumTupleDomain(Map<T, Domain> domains,
			Expression extractionRemainingExpression) {
		super(domains);
		this.remainingExpresstion = extractionRemainingExpression;
	}

	protected ProteumTupleDomain(Map<T, Domain> domains,
			Expression extractionRemainingExpression, List<Symbol> groupBy,
			List<Expression> pushDownAggregationList,
			Expression minimumExpression, AtomicBoolean isAggregatePushDownable) {
		super(domains);
		this.remainingExpresstion = extractionRemainingExpression;
		this.groupBy = groupBy;
		this.pushDownAggregationList = pushDownAggregationList;
		this.minimumExpression = minimumExpression;
		this.isAggregatePushDownable = isAggregatePushDownable;
	}

	protected ProteumTupleDomain(Map<T, Domain> domains) {
		super(domains);
	}

	public Expression getRemainingExpresstion() {
		return remainingExpresstion;
	}

	public <U> ProteumTupleDomain<U> transform(Function<T, U> function) {
		return new ProteumTupleDomain<U>(
				super.transform(function).getDomains(), remainingExpresstion,
				groupBy, pushDownAggregationList, minimumExpression,
				isAggregatePushDownable);
	}

	public void setGroupBy(Iterable<Symbol> groupByIterable) {
		this.groupBy = new ArrayList<Symbol>();
		for (Symbol s : groupByIterable) {
			this.groupBy.add(s);
		}

	}

	public void setAggregateList(
			Iterable<Expression> pushDownAggregationListIterable) {
		this.pushDownAggregationList = new ArrayList<Expression>();
		for (Expression e : pushDownAggregationListIterable) {
			this.pushDownAggregationList.add(e);
		}

	}

	public boolean isAggregatePushDownable() {
		return isAggregatePushDownable.get();
	}

	public void setAggregatePushDownable(boolean isAggregatePushDownable) {
		this.isAggregatePushDownable.set(isAggregatePushDownable);
	}

	public List<Symbol> getGroupBy() {
		return groupBy;
	}

	public List<Expression> getPushDownAggregationList() {
		return pushDownAggregationList;
	}

	public void setMinimumExpression(Expression minimumExpression) {
		this.minimumExpression = minimumExpression;
	}

	public Expression getMinimumExpression() {
		return minimumExpression;
	}

}