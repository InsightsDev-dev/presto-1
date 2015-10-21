package com.facebook.presto.sql.planner.optimizations;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author Dilip Kasana
 * @Date 01-May-2015
 */
public class CustomizedPredicatePushDownContext {
	private final HashMap<PlanNodeId, PredicatePushDownContext> pushDownPredicateMap;
	private final HashMap<PlanNodeId, Map<FunctionCall,FunctionCall>> newFunctionCallVsPreviousFunctionCall;
	private boolean predicatePushdownable=false;
	private final Map<PlanNodeId, Map<Symbol, Expression>> symbolToExpressionMap;

	public CustomizedPredicatePushDownContext() {
		pushDownPredicateMap = new HashMap<PlanNodeId, PredicatePushDownContext>();
		symbolToExpressionMap = new HashMap<PlanNodeId, Map<Symbol, Expression>>();
		newFunctionCallVsPreviousFunctionCall=new HashMap<PlanNodeId, Map<FunctionCall,FunctionCall>>();
	}

	public HashMap<PlanNodeId, PredicatePushDownContext> getPushDownPredicateMap() {
		return pushDownPredicateMap;
	}

	public Map<PlanNodeId, Map<Symbol, Expression>> getSymbolToExpressionMap() {
		return symbolToExpressionMap;
	}

	public HashMap<PlanNodeId, Map<FunctionCall, FunctionCall>> getNewFunctionCallVsPreviousFunctionCall() {
		return newFunctionCallVsPreviousFunctionCall;
	}

	public void setPredicatePushdownable(boolean isPredicatePushdownable) {
		this.predicatePushdownable=isPredicatePushdownable;
	}
	public boolean isPredicatePushdownable() {
		return this.predicatePushdownable;
	}
	

}
