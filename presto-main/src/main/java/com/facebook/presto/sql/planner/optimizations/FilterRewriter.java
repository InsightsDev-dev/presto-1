package com.facebook.presto.sql.planner.optimizations;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author dilipsingh
 * @Date 01-May-2015
 */
public class FilterRewriter {

	public static Expression remainingAfterRemovingAggregateFilterExpression(
			Map<Symbol, Type> symbolTypes, Iterable<Symbol> groupByIterable,
			Map<Symbol, QualifiedNameReference> symbolToColumnName,
			Expression expression) {
		return ExpressionTreeRewriter.rewriteWith(
				new RemainingAfterRemovingAggregateFilterExpressionRewriter(
						symbolTypes, groupByIterable, symbolToColumnName),
				expression);
	}

	public static Expression getAggregateFilterExpression(
			Map<Symbol, Type> symbolTypes, Iterable<Symbol> groupByIterable,
			Map<Symbol, QualifiedNameReference> symbolToColumnName,
			Expression expression) {
		return ExpressionTreeRewriter.rewriteWith(new AggregateFilterRewriter(
				symbolTypes, groupByIterable, symbolToColumnName), expression);
	}

}

class RemainingAfterRemovingAggregateFilterExpressionRewriter extends
		ExpressionRewriter<Void> {
	Map<Symbol, Type> symbolTypes;
	Set<Symbol> groupBy;
	Map<Symbol, QualifiedNameReference> symbolToColumnName;

	RemainingAfterRemovingAggregateFilterExpressionRewriter(
			Map<Symbol, Type> symbolTypes, Iterable<Symbol> groupByIterable,
			Map<Symbol, QualifiedNameReference> symbolToColumnName) {
		this.symbolToColumnName = symbolToColumnName;
		this.symbolTypes = symbolTypes;
		groupBy = new HashSet<Symbol>();
		for (Symbol s : groupByIterable) {
			groupBy.add(s);
		}
	}

	@Override
	public Expression rewriteExpression(Expression node, Void context,
			ExpressionTreeRewriter<Void> treeRewriter) {
		boolean isOnlyAggregates = isOnlyAggregates(node);
		if (isOnlyAggregates) {
			return BooleanLiteral.TRUE_LITERAL;
		} else {
			return node;
		}
	}

	@Override
	public Expression rewriteLogicalBinaryExpression(
			LogicalBinaryExpression node, Void context,
			ExpressionTreeRewriter<Void> treeRewriter) {
		if (LogicalBinaryExpression.Type.AND.equals(node.getType())) {
			boolean onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft());
			boolean onlyAggregatesAtRight = isOnlyAggregates(node.getRight());
			if (onlyAggregatesAtLeft && onlyAggregatesAtRight) {
				return BooleanLiteral.TRUE_LITERAL;
			} else if (onlyAggregatesAtLeft) {
				return node.getRight();
			} else if (onlyAggregatesAtRight) {
				return node.getLeft();
			} else {
				return node;
			}
		} else {
			boolean onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft());
			boolean onlyAggregatesAtRight = isOnlyAggregates(node.getRight());
			if (onlyAggregatesAtLeft && onlyAggregatesAtRight) {
				return BooleanLiteral.TRUE_LITERAL;
			} else {
				return node;
			}
		}

		// if (LogicalBinaryExpression.Type.AND.equals(node.getType())) {
		// boolean onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft());
		// boolean onlyAggregatesAtRight = isOnlyAggregates(node.getRight());
		// if (onlyAggregatesAtLeft && onlyAggregatesAtRight) {
		// return new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.OR, node,
		// BooleanLiteral.TRUE_LITERAL);
		// // BooleanLiteral.TRUE_LITERAL
		// } else if (onlyAggregatesAtLeft) {
		// return new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.AND,
		// new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.OR,
		// node.getLeft(), BooleanLiteral.TRUE_LITERAL),
		// node.getRight());
		// // return node.getRight();
		// } else if (onlyAggregatesAtRight) {
		// return new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.AND,
		// new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.OR, node
		// .getRight(), node.getLeft()),
		// BooleanLiteral.TRUE_LITERAL); // return node.getLeft();
		// } else {
		// return node;
		// }
		// } else {
		// return super.rewriteLogicalBinaryExpression(node, context,
		// treeRewriter);
		// }
	}

	private boolean isOnlyAggregates(Expression expression) {
		Builder<Symbol> symbols = ExpressionSymbolExtractor.getSymbols(
				symbolTypes, groupBy, symbolToColumnName, expression);
		Function<Symbol, Symbol> syFunction = new Function<Symbol, Symbol>() {
			@Override
			public Symbol apply(Symbol symbol) {
				return new Symbol(symbolToColumnName.get(symbol).getName()
						.toString());
			}
		};

		for (Symbol symbol : transform(symbols.build(), syFunction)) {
			if (!groupBy.contains(symbol)) {
				return false;
			}
		}
		return true;
	}
}

class AggregateFilterRewriter extends ExpressionRewriter<Void> {
	Map<Symbol, Type> symbolTypes;
	Set<Symbol> groupBy;
	Map<Symbol, QualifiedNameReference> symbolToColumnName;

	AggregateFilterRewriter(Map<Symbol, Type> symbolTypes,
			Iterable<Symbol> groupByIterable,
			Map<Symbol, QualifiedNameReference> symbolToColumnName) {
		this.symbolToColumnName = symbolToColumnName;
		this.symbolTypes = symbolTypes;
		groupBy = new HashSet<Symbol>();
		for (Symbol s : groupByIterable) {
			groupBy.add(s);
		}
	}

	@Override
	public Expression rewriteExpression(Expression node, Void context,
			ExpressionTreeRewriter<Void> treeRewriter) {
		boolean isOnlyAggregates = isOnlyAggregates(node);
		if (isOnlyAggregates) {
			return node;
		} else {
			return BooleanLiteral.TRUE_LITERAL;
		}
	}

	@Override
	public Expression rewriteLogicalBinaryExpression(
			LogicalBinaryExpression node, Void context,
			ExpressionTreeRewriter<Void> treeRewriter) {
		if (LogicalBinaryExpression.Type.AND.equals(node.getType())) {
			boolean onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft());
			boolean onlyAggregatesAtRight = isOnlyAggregates(node.getRight());
			if (onlyAggregatesAtLeft && onlyAggregatesAtRight) {
				return node;
			} else if (onlyAggregatesAtLeft) {
				return node.getLeft();
			} else if (onlyAggregatesAtRight) {
				return node.getRight();
			} else {
				return BooleanLiteral.TRUE_LITERAL;
			}
		} else {
			boolean onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft());
			boolean onlyAggregatesAtRight = isOnlyAggregates(node.getRight());
			if (onlyAggregatesAtLeft && onlyAggregatesAtRight) {
				return node;
			} else {
				return BooleanLiteral.TRUE_LITERAL;
			}
			// return super.rewriteLogicalBinaryExpression(node, context,
			// treeRewriter);
			// return BooleanLiteral.TRUE_LITERAL;
		}

		// if (LogicalBinaryExpression.Type.AND.equals(node.getType())) {
		// boolean onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft());
		// boolean onlyAggregatesAtRight = isOnlyAggregates(node.getRight());
		// if (onlyAggregatesAtLeft && onlyAggregatesAtRight) {
		// return new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.OR, node,
		// BooleanLiteral.TRUE_LITERAL);
		// // BooleanLiteral.TRUE_LITERAL
		// } else if (onlyAggregatesAtLeft) {
		// return new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.AND,
		// new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.OR,
		// node.getLeft(), BooleanLiteral.TRUE_LITERAL),
		// node.getRight());
		// // return node.getRight();
		// } else if (onlyAggregatesAtRight) {
		// return new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.AND,
		// new LogicalBinaryExpression(
		// LogicalBinaryExpression.Type.OR, node
		// .getRight(), node.getLeft()),
		// BooleanLiteral.TRUE_LITERAL); // return node.getLeft();
		// } else {
		// return node;
		// }
		// } else {
		// return super.rewriteLogicalBinaryExpression(node, context,
		// treeRewriter);
		// }
	}

	private boolean isOnlyAggregates(Expression expression) {
		Builder<Symbol> symbols = ExpressionSymbolExtractor.getSymbols(
				symbolTypes, groupBy, symbolToColumnName, expression);
		Function<Symbol, Symbol> syFunction = new Function<Symbol, Symbol>() {
			@Override
			public Symbol apply(Symbol symbol) {
				return new Symbol(symbolToColumnName.get(symbol).getName()
						.toString());
			}
		};

		for (Symbol symbol : transform(symbols.build(), syFunction)) {
			if (!groupBy.contains(symbol)) {
				return false;
			}
		}
		return true;
	}
}

class ExpressionSymbolExtractor extends
		DefaultExpressionTraversalVisitor<Void, Void> {
	private final ImmutableList.Builder<Symbol> symbols = ImmutableList
			.builder();
	Set<Symbol> set = new HashSet<Symbol>();

	Map<Symbol, Type> symbolTypes;
	Set<Symbol> groupBy;
	Map<Symbol, QualifiedNameReference> symbolToColumnName;

	private ExpressionSymbolExtractor(Map<Symbol, Type> symbolTypes,
			Iterable<Symbol> groupByIterable,
			Map<Symbol, QualifiedNameReference> symbolToColumnName) {
		groupBy = new HashSet<Symbol>();
		for (Symbol s : groupByIterable) {
			groupBy.add(s);
		}
		this.symbolTypes = symbolTypes;
		this.symbolToColumnName = symbolToColumnName;
	}

	public static ImmutableList.Builder<Symbol> getSymbols(
			Map<Symbol, Type> symbolTypes, Iterable<Symbol> groupByIterable,
			Map<Symbol, QualifiedNameReference> symbolToColumnName,
			Expression expression) {
		ExpressionSymbolExtractor expressionSymbolExtractor = new ExpressionSymbolExtractor(
				symbolTypes, groupByIterable, symbolToColumnName);
		expressionSymbolExtractor.process(expression, null);
		return expressionSymbolExtractor.symbols
				.addAll(expressionSymbolExtractor.set);
	}

	@Override
	protected Void visitQualifiedNameReference(QualifiedNameReference node,
			Void context) {
		List<String> parts = node.getName().getParts();
		for (String symbol : parts) {
			if (symbolTypes.containsKey(new Symbol(symbol))) {
				set.add(new Symbol(symbol));
			}
		}
		return null;
	}
}

/*
 * 
 * if (LogicalBinaryExpression.Type.AND.equals(node.getType())) { boolean
 * onlyAggregatesAtLeft = isOnlyAggregates(node.getLeft()); boolean
 * onlyAggregatesAtRight = isOnlyAggregates(node.getLeft()); if
 * (onlyAggregatesAtLeft && onlyAggregatesAtRight) { return new
 * LogicalBinaryExpression( LogicalBinaryExpression.Type.OR, node,
 * BooleanLiteral.TRUE_LITERAL); // BooleanLiteral.TRUE_LITERAL } else if
 * (onlyAggregatesAtLeft) { return new LogicalBinaryExpression(
 * LogicalBinaryExpression.Type.AND, new LogicalBinaryExpression(
 * LogicalBinaryExpression.Type.OR, node.getLeft(),
 * BooleanLiteral.TRUE_LITERAL), node.getRight()); // return node.getRight(); }
 * else if (onlyAggregatesAtRight) { return new LogicalBinaryExpression(
 * LogicalBinaryExpression.Type.AND, new LogicalBinaryExpression(
 * LogicalBinaryExpression.Type.OR, node .getRight(), node.getLeft()),
 * BooleanLiteral.TRUE_LITERAL); // return node.getLeft(); } else { return node;
 * } } else { return super.rewriteLogicalBinaryExpression(node, context,
 * treeRewriter); }
 */