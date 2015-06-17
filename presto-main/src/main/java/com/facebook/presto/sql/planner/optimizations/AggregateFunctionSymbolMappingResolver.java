package com.facebook.presto.sql.planner.optimizations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer.IndexKeyTracer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

/**
 * 
 * @author Dilip Kasana
 * @Date 29-Apr-2015
 */
public final class AggregateFunctionSymbolMappingResolver {

	private AggregateFunctionSymbolMappingResolver() {
	}

	public static Map<FunctionCall, FunctionCall> resolve(AggregationNode plan,
			Map<PlanNodeId, Map<Symbol, Expression>> map) {
		Map<FunctionCall, FunctionCall> functionCallMappings = new HashMap<FunctionCall, FunctionCall>();
		plan.accept(new Visitor(functionCallMappings, map), null);
		return functionCallMappings;
	}

	private static class Visitor extends PlanVisitor<Void, Void> {
		private final Map<FunctionCall, FunctionCall> functionCallMappings;
		private final Map<PlanNodeId, Map<Symbol, Expression>> symbolToExpressionMap;

		public Visitor(
				final Map<FunctionCall, FunctionCall> functionCallMappings,
				Map<PlanNodeId, Map<Symbol, Expression>> symbolToExpressionMap) {
			this.functionCallMappings = functionCallMappings;
			this.symbolToExpressionMap = symbolToExpressionMap;
		}

		@Override
		protected Void visitPlan(PlanNode node, Void context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ node.getClass().getName());
		}

		@Override
		public Void visitAggregation(AggregationNode node, Void context) {
			for (FunctionCall call : node.getAggregations().values()) {
				functionCallMappings.put(call, call);
			}
			PlanNode source = node.getSource();
			source.accept(this, context);
			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getGroupBy(),
					"Invalid node. Group by symbols (%s) not in source plan output (%s)",
					node.getGroupBy(), node.getSource().getOutputSymbols());

			if (node.getSampleWeight().isPresent()) {
				checkArgument(
						inputs.contains(node.getSampleWeight().get()),
						"Invalid node. Sample weight symbol (%s) is not in source plan output (%s)",
						node.getSampleWeight().get(), node.getSource()
								.getOutputSymbols());
			}

			for (FunctionCall call : node.getAggregations().values()) {
				Set<Symbol> dependencies = DependencyExtractor
						.extractUnique(call);
				checkDependencies(
						inputs,
						dependencies,
						"Invalid node. Aggregation dependencies (%s) not in source plan output (%s)",
						dependencies, node.getSource().getOutputSymbols());
			}

			return null;
		}

		@Override
		public Void visitMarkDistinct(MarkDistinctNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			checkDependencies(
					source.getOutputSymbols(),
					node.getDistinctSymbols(),
					"Invalid node. Mark distinct symbols (%s) not in source plan output (%s)",
					node.getDistinctSymbols(), source.getOutputSymbols());

			return null;
		}

		@Override
		public Void visitWindow(WindowNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());

			checkDependencies(
					inputs,
					node.getPartitionBy(),
					"Invalid node. Partition by symbols (%s) not in source plan output (%s)",
					node.getPartitionBy(), node.getSource().getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOrderBy(),
					"Invalid node. Order by symbols (%s) not in source plan output (%s)",
					node.getOrderBy(), node.getSource().getOutputSymbols());

			for (FunctionCall call : node.getWindowFunctions().values()) {
				Set<Symbol> dependencies = DependencyExtractor
						.extractUnique(call);
				checkDependencies(
						inputs,
						dependencies,
						"Invalid node. Window function dependencies (%s) not in source plan output (%s)",
						dependencies, node.getSource().getOutputSymbols());
			}

			return null;
		}

		@Override
		public Void visitTopNRowNumber(TopNRowNumberNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getPartitionBy(),
					"Invalid node. Partition by symbols (%s) not in source plan output (%s)",
					node.getPartitionBy(), node.getSource().getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOrderBy(),
					"Invalid node. Order by symbols (%s) not in source plan output (%s)",
					node.getOrderBy(), node.getSource().getOutputSymbols());

			return null;
		}

		@Override
		public Void visitRowNumber(RowNumberNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			checkDependencies(
					source.getOutputSymbols(),
					node.getPartitionBy(),
					"Invalid node. Partition by symbols (%s) not in source plan output (%s)",
					node.getPartitionBy(), node.getSource().getOutputSymbols());

			return null;
		}

		@Override
		public Void visitFilter(FilterNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOutputSymbols(),
					"Invalid node. Output symbols (%s) not in source plan output (%s)",
					node.getOutputSymbols(), node.getSource()
							.getOutputSymbols());

			Set<Symbol> dependencies = DependencyExtractor.extractUnique(node
					.getPredicate());
			checkDependencies(
					inputs,
					dependencies,
					"Invalid node. Predicate dependencies (%s) not in source plan output (%s)",
					dependencies, node.getSource().getOutputSymbols());

			return null;
		}

		@Override
		public Void visitSample(SampleNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			return null;
		}

		private static class ExpressionSymbolInliner extends
				ExpressionRewriter<Void> {
			private final Map<Symbol, ? extends Expression> mappings;

			public ExpressionSymbolInliner(
					Map<Symbol, ? extends Expression> mappings) {
				this.mappings = mappings;
			}

			@Override
			public Expression rewriteQualifiedNameReference(
					QualifiedNameReference node, Void context,
					ExpressionTreeRewriter<Void> treeRewriter) {
				return mappings.get(Symbol.fromQualifiedName(node.getName()));
			}
		}

		@Override
		public Void visitProject(ProjectNode node, Void context) {
			ExpressionSymbolInliner expressionSymbolInliner;
			if (symbolToExpressionMap.get(node.getId()) == null) {
				expressionSymbolInliner = new ExpressionSymbolInliner(
						node.getAssignments());
			} else {
				expressionSymbolInliner = new ExpressionSymbolInliner(
						symbolToExpressionMap.get(node.getId()));
			}
			for (FunctionCall key : functionCallMappings.keySet()) {
				functionCallMappings.put(key, ExpressionTreeRewriter
						.rewriteWith(expressionSymbolInliner,
								functionCallMappings.get(key)));
			}
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());
			for (Expression expression : node.getExpressions()) {
				Set<Symbol> dependencies = DependencyExtractor
						.extractUnique(expression);
				checkDependencies(
						inputs,
						dependencies,
						"Invalid node. Expression dependencies (%s) not in source plan output (%s)",
						dependencies, inputs);
			}

			return null;
		}

		@Override
		public Void visitTopN(TopNNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOutputSymbols(),
					"Invalid node. Output symbols (%s) not in source plan output (%s)",
					node.getOutputSymbols(), node.getSource()
							.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOrderBy(),
					"Invalid node. Order by dependencies (%s) not in source plan output (%s)",
					node.getOrderBy(), node.getSource().getOutputSymbols());

			return null;
		}

		@Override
		public Void visitSort(SortNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			Set<Symbol> inputs = ImmutableSet.copyOf(source.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOutputSymbols(),
					"Invalid node. Output symbols (%s) not in source plan output (%s)",
					node.getOutputSymbols(), node.getSource()
							.getOutputSymbols());
			checkDependencies(
					inputs,
					node.getOrderBy(),
					"Invalid node. Order by dependencies (%s) not in source plan output (%s)",
					node.getOrderBy(), node.getSource().getOutputSymbols());

			return null;
		}

		@Override
		public Void visitOutput(OutputNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			checkDependencies(
					source.getOutputSymbols(),
					node.getOutputSymbols(),
					"Invalid node. Output column dependencies (%s) not in source plan output (%s)",
					node.getOutputSymbols(), source.getOutputSymbols());

			return null;
		}

		@Override
		public Void visitLimit(LimitNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			return null;
		}

		@Override
		public Void visitDistinctLimit(DistinctLimitNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);
			return null;
		}

		@Override
		public Void visitJoin(JoinNode node, Void context) {
			node.getLeft().accept(this, context);
			node.getRight().accept(this, context);

			verifyUniqueId(node);

			Set<Symbol> leftInputs = ImmutableSet.copyOf(node.getLeft()
					.getOutputSymbols());
			Set<Symbol> rightInputs = ImmutableSet.copyOf(node.getRight()
					.getOutputSymbols());

			for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
				checkArgument(leftInputs.contains(clause.getLeft()),
						"Symbol from join clause (%s) not in left source (%s)",
						clause.getLeft(), node.getLeft().getOutputSymbols());
				checkArgument(
						rightInputs.contains(clause.getRight()),
						"Symbol from join clause (%s) not in right source (%s)",
						clause.getRight(), node.getRight().getOutputSymbols());
			}

			return null;
		}

		@Override
		public Void visitSemiJoin(SemiJoinNode node, Void context) {
			node.getSource().accept(this, context);
			node.getFilteringSource().accept(this, context);

			verifyUniqueId(node);

			checkArgument(
					node.getSource().getOutputSymbols()
							.contains(node.getSourceJoinSymbol()),
					"Symbol from semi join clause (%s) not in source (%s)",
					node.getSourceJoinSymbol(), node.getSource()
							.getOutputSymbols());
			checkArgument(
					node.getFilteringSource().getOutputSymbols()
							.contains(node.getFilteringSourceJoinSymbol()),
					"Symbol from semi join clause (%s) not in filtering source (%s)",
					node.getSourceJoinSymbol(), node.getFilteringSource()
							.getOutputSymbols());

			Set<Symbol> outputs = ImmutableSet.copyOf(node.getOutputSymbols());
			checkArgument(
					outputs.containsAll(node.getSource().getOutputSymbols()),
					"Semi join output symbols (%s) must contain all of the source symbols (%s)",
					node.getOutputSymbols(), node.getSource()
							.getOutputSymbols());
			checkArgument(
					outputs.contains(node.getSemiJoinOutput()),
					"Semi join output symbols (%s) must contain join result (%s)",
					node.getOutputSymbols(), node.getSemiJoinOutput());

			return null;
		}

		@Override
		public Void visitIndexJoin(IndexJoinNode node, Void context) {
			node.getProbeSource().accept(this, context);
			node.getIndexSource().accept(this, context);

			verifyUniqueId(node);

			Set<Symbol> probeInputs = ImmutableSet.copyOf(node.getProbeSource()
					.getOutputSymbols());
			Set<Symbol> indexSourceInputs = ImmutableSet.copyOf(node
					.getIndexSource().getOutputSymbols());
			for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
				checkArgument(
						probeInputs.contains(clause.getProbe()),
						"Probe symbol from index join clause (%s) not in probe source (%s)",
						clause.getProbe(), node.getProbeSource()
								.getOutputSymbols());
				checkArgument(
						indexSourceInputs.contains(clause.getIndex()),
						"Index symbol from index join clause (%s) not in index source (%s)",
						clause.getIndex(), node.getIndexSource()
								.getOutputSymbols());
			}
//@Todo : dilip 
//Uncomment and resolve this
//			Set<Symbol> lookupSymbols = FluentIterable.from(node.getCriteria())
//					.transform(IndexJoinNode.EquiJoinClause)
//					.toSet();
//			Map<Symbol, Symbol> trace = IndexKeyTracer.trace(
//					node.getIndexSource(), lookupSymbols);
//			checkArgument(
//					!trace.isEmpty()
//							&& lookupSymbols.containsAll(trace.keySet()),
//					"Index lookup symbols are not traceable to index source: %s",
//					lookupSymbols);

			return null;
		}

		@Override
		public Void visitIndexSource(IndexSourceNode node, Void context) {
			verifyUniqueId(node);

			checkDependencies(node.getOutputSymbols(), node.getLookupSymbols(),
					"Lookup symbols must be part of output symbols");
			checkDependencies(node.getAssignments().keySet(),
					node.getOutputSymbols(),
					"Assignments must contain mappings for output symbols");

			return null;
		}

		@Override
		public Void visitTableScan(TableScanNode node, Void context) {
			verifyUniqueId(node);

			checkArgument(
					node.getAssignments().keySet()
							.containsAll(node.getOutputSymbols()),
					"Assignments must contain mappings for output symbols");

			return null;
		}

		@Override
		public Void visitValues(ValuesNode node, Void context) {
			verifyUniqueId(node);
			return null;
		}

		@Override
		public Void visitUnnest(UnnestNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context);

			verifyUniqueId(node);

			Set<Symbol> required = ImmutableSet.<Symbol> builder()
					.addAll(node.getReplicateSymbols())
					.addAll(node.getUnnestSymbols().keySet()).build();

			checkDependencies(
					source.getOutputSymbols(),
					required,
					"Invalid node. Dependencies (%s) not in source plan output (%s)",
					required, source.getOutputSymbols());

			return null;
		}

		@Override
		public Void visitExchange(ExchangeNode node, Void context) {
			verifyUniqueId(node);

			return null;
		}

		
		@Override
		public Void visitTableWriter(TableWriterNode node, Void context) {
			PlanNode source = node.getSource();
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			if (node.getSampleWeightSymbol().isPresent()) {
				checkArgument(
						source.getOutputSymbols().contains(
								node.getSampleWeightSymbol().get()),
						"Invalid node. Sample weight symbol (%s) is not in source plan output (%s)",
						node.getSampleWeightSymbol().get(), node.getSource()
								.getOutputSymbols());
			}

			return null;
		}

		@Override
		public Void visitTableCommit(TableCommitNode node, Void context) {
			PlanNode source = node.getSource();
			checkArgument(
					source instanceof TableWriterNode,
					"Invalid node. TableCommit source must be a TableWriter not %s",
					source.getClass().getSimpleName());
			source.accept(this, context); // visit child

			verifyUniqueId(node);

			return null;
		}

		@Override
		public Void visitUnion(UnionNode node, Void context) {
			for (int i = 0; i < node.getSources().size(); i++) {
				PlanNode subplan = node.getSources().get(i);
				checkDependencies(subplan.getOutputSymbols(),
						node.sourceOutputLayout(i),
						"UNION subplan must provide all of the necessary symbols");
				subplan.accept(this, context); // visit child
			}

			verifyUniqueId(node);

			return null;
		}

		private void verifyUniqueId(PlanNode node) {
			return;
			// PlanNodeId id = node.getId();
			// checkArgument(!nodesById.containsKey(id),
			// "Duplicate node id found %s between %s and %s",
			// node.getId(), node, nodesById.get(id));
			//
			// nodesById.put(id, node);
		}
	}

	private static void checkDependencies(Collection<Symbol> inputs,
			Collection<Symbol> required, String message, Object... parameters) {
		return;
		// checkArgument(ImmutableSet.copyOf(inputs).containsAll(required),
		// message, parameters);
	}

}
