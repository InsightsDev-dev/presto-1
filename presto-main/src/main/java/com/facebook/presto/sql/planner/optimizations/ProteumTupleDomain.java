package com.facebook.presto.sql.planner.optimizations;

import java.util.Map;

import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.tree.Expression;

/**
 * 
 * @author dilipsingh
 * @Date 18-Feb-2015
 */
public class ProteumTupleDomain<T> extends TupleDomain<T> {

	Expression remainingExpresstion;

	protected ProteumTupleDomain(Map<T, Domain> domains,
			Expression extractionRemainingExpression) {
		super(domains);
		this.remainingExpresstion = extractionRemainingExpression;
	}

	protected ProteumTupleDomain(Map<T, Domain> domains) {
		super(domains);
	}

	public Expression getRemainingExpresstion() {
		return remainingExpresstion;
	}

	public <U> ProteumTupleDomain<U> transform(Function<T, U> function) {
		return new ProteumTupleDomain<>(super.transform(function).getDomains(),
				remainingExpresstion);
	}

}