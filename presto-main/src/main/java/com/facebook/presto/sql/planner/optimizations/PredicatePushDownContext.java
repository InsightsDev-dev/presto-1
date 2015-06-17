package com.facebook.presto.sql.planner.optimizations;

import java.util.List;
import java.util.Map;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
/**
 * @author Dilip Kasana
 * @Date 17 Jun 2015
 */
class PredicatePushDownContext {

	public static final PredicatePushDownContext TRUE_LITERAL() {
		return new PredicatePushDownContext(BooleanLiteral.TRUE_LITERAL);
	}

	private Expression expression;
	private Map<Symbol, FunctionCall> aggregations;
	private Map<Symbol, Signature> functionMap;
	private List<Symbol> groupBy;

	public Map<Symbol, FunctionCall> getAggregations() {
		return aggregations;
	}

	public List<Symbol> getGroupBy() {
		return groupBy;
	}

	public PredicatePushDownContext(Expression expression) {
		this.expression = expression;
	}

	public PredicatePushDownContext setAggregations(Map<Symbol, FunctionCall> aggregations) {
		this.aggregations = aggregations;
		return this;
	}

	public void setGroupBy(List<Symbol> groupBy) {
		this.groupBy = groupBy;
	}

	public PredicatePushDownContext(Expression expression,
			PredicatePushDownContext predicatePushDownContext) {
		this.expression = expression;
		this.groupBy = predicatePushDownContext.groupBy;
		this.aggregations = predicatePushDownContext.aggregations;
		this.functionMap = predicatePushDownContext.functionMap;
	}

	public Expression getExpression() {
		return expression;
	}

	public PredicatePushDownContext setExpression(Expression expression) {
		this.expression = expression;
		return this;
	}

	public Map<Symbol, Signature> getFunctionMap() {
		return functionMap;
	}

	public void setFunctionMap(Map<Symbol, Signature> functionMap) {
		this.functionMap = functionMap;
	}

	public void clear() {
		this. expression=null;
		this. aggregations=null;
		this.functionMap=null;
		this.groupBy=null;
		
	}

}