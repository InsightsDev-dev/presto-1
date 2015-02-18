package com.mobileum.range.presto;

/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015 
 */
import com.facebook.presto.Session;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;
import com.mobileum.range.RangeUtils;
import com.mobileum.range.TimeStamp;
import com.mobileum.range.TimeStampRange;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sound.midi.VoiceStatus;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LEFT_CONTAINS_RIGHT;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.RIGHT_CONTAINS_LEFT;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.OVERLAPPING_WITH;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.STRICTLY_LEFT;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.STRICTLY_RIGHT;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_RIGHT;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_LEFT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.primitives.Primitives.wrap;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.FLOOR;
import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public class RewritingVisitor<C> extends AstVisitor<Expression, Context<C>> {
	private static final String DATE_LITERAL = getMagicLiteralFunctionSignature(
			DATE).getName();
	private static final String TSRANZE_LITERAL = getMagicLiteralFunctionSignature(
			TSRangeType.TS_RANGE_TYPE).getName();
	private static final String TSTZRANE_LITERAL = getMagicLiteralFunctionSignature(
			TSTZRangeType.TSTZ_RANGE_TYPE).getName();
	private static final String INT4RANGE_LITERAL = getMagicLiteralFunctionSignature(
			Int4RangeType.INT_4_RANGE_TYPE).getName();
	private static final String TIMESTAMP_LITERAL = getMagicLiteralFunctionSignature(
			TIMESTAMP).getName();

	@Override
	protected Expression visitBooleanLiteral(BooleanLiteral node,
			Context<C> context) {
		Boolean b = node.getValue();
		return new BooleanLiteral(b.toString());
	}

	@Override
	protected Expression visitNullLiteral(NullLiteral node, Context<C> context) {
		return TRUE_LITERAL;
	}

	@Override
	protected Expression visitExpression(Expression node, Context<C> context) {

		throw new UnsupportedOperationException("not yet implemented: "
				+ getClass().getSimpleName() + " for "
				+ node.getClass().getName());
	}

	public Expression rewrite(Expression node, C context) {
		return (Expression) super.process(node, new Context<C>(context, false));
	}

	@Override
	protected Expression visitNegativeExpression(NegativeExpression node,
			Context<C> context) {
		Expression child = rewrite(node.getValue(), context.get());
		return new NegativeExpression(child);
	}

	@Override
	public Expression visitArithmeticExpression(ArithmeticExpression node,
			Context<C> context) {
		Expression left = rewrite(node.getLeft(), context.get());
		Expression right = rewrite(node.getRight(), context.get());
		return new ArithmeticExpression(node.getType(), left, right);
	}

	@SuppressWarnings("ObjectEquality")
	private static <T> boolean sameElements(Iterable<? extends T> a,
			Iterable<? extends T> b) {
		if (Iterables.size(a) != Iterables.size(b)) {
			return false;
		}

		Iterator<? extends T> first = a.iterator();
		Iterator<? extends T> second = b.iterator();

		while (first.hasNext() && second.hasNext()) {
			if (first.next() != second.next()) {
				return false;
			}
		}

		return true;
	}

	@Override
	protected Expression visitArrayConstructor(ArrayConstructor node,
			Context<C> context) {
		ImmutableList.Builder<Expression> builder = ImmutableList.builder();
		for (Expression expression : node.getValues()) {
			builder.add(rewrite(expression, context.get()));
		}
		return new ArrayConstructor(builder.build());
	}

	@Override
	protected Expression visitSubscriptExpression(SubscriptExpression node,
			Context<C> context) {

		Expression base = rewrite(node.getBase(), context.get());
		Expression index = rewrite(node.getIndex(), context.get());
		return new SubscriptExpression(base, index);
	}

	@Override
	public Expression visitComparisonExpression(ComparisonExpression node,
			Context<C> context) {
		Expression left = rewrite(node.getLeft(), context.get());
		Expression right = rewrite(node.getRight(), context.get());
		return new ComparisonExpression(node.getType(), left, right);
	}

	@Override
	protected Expression visitBetweenPredicate(BetweenPredicate node,
			Context<C> context) {
		// Expression value = rewrite(node.getValue(), context.get());
		// Expression min = rewrite(node.getMin(), context.get());
		// Expression max = rewrite(node.getMax(), context.get());
		// return new BetweenPredicate(value, min, max);
		return process(
				and(new ComparisonExpression(GREATER_THAN_OR_EQUAL,
						node.getValue(), node.getMin()),
						new ComparisonExpression(LESS_THAN_OR_EQUAL, node
								.getValue(), node.getMax())), context);
	}

	@Override
	public Expression visitLogicalBinaryExpression(
			LogicalBinaryExpression node, Context<C> context) {
		Expression left = rewrite(node.getLeft(), context.get());
		Expression right = rewrite(node.getRight(), context.get());
		return new LogicalBinaryExpression(node.getType(), left, right);
	}

	@Override
	public Expression visitNotExpression(NotExpression node, Context<C> context) {

		Expression value = rewrite(node.getValue(), context.get());
		return new NotExpression(value);
	}

	@Override
	protected Expression visitIsNullPredicate(IsNullPredicate node,
			Context<C> context) {
		Expression value = rewrite(node.getValue(), context.get());
		return new IsNullPredicate(value);
	}

	@Override
	protected Expression visitIsNotNullPredicate(IsNotNullPredicate node,
			Context<C> context) {
		Expression value = rewrite(node.getValue(), context.get());
		return new IsNotNullPredicate(value);
	}

	@Override
	protected Expression visitNullIfExpression(NullIfExpression node,
			Context<C> context) {
		Expression first = rewrite(node.getFirst(), context.get());
		Expression second = rewrite(node.getSecond(), context.get());
		return new NullIfExpression(first, second);
	}

	@Override
	protected Expression visitIfExpression(IfExpression node, Context<C> context) {
		Expression condition = rewrite(node.getCondition(), context.get());
		Expression trueValue = rewrite(node.getTrueValue(), context.get());
		Expression falseValue = null;
		if (node.getFalseValue().isPresent()) {
			falseValue = rewrite(node.getFalseValue().get(), context.get());
		}
		return new IfExpression(condition, trueValue, falseValue);
	}

	@Override
	protected Expression visitSearchedCaseExpression(
			SearchedCaseExpression node, Context<C> context) {
		ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
		for (WhenClause expression : node.getWhenClauses()) {
			builder.add((WhenClause) rewrite(expression, context.get()));
		}

		Expression defaultValue = null;
		if (node.getDefaultValue() != null) {
			defaultValue = rewrite(node.getDefaultValue(), context.get());
		}
		return new SearchedCaseExpression(builder.build(), defaultValue);
	}

	@Override
	protected Expression visitSimpleCaseExpression(SimpleCaseExpression node,
			Context<C> context) {
		Expression operand = rewrite(node.getOperand(), context.get());

		ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
		for (WhenClause expression : node.getWhenClauses()) {
			builder.add((WhenClause) rewrite(expression, context.get()));
		}

		Expression defaultValue = null;
		if (node.getDefaultValue() != null) {
			defaultValue = rewrite(node.getDefaultValue(), context.get());
		}
		return new SimpleCaseExpression(operand, builder.build(), defaultValue);
	}

	@Override
	protected Expression visitWhenClause(WhenClause node, Context<C> context) {

		Expression operand = rewrite(node.getOperand(), context.get());
		Expression result = rewrite(node.getResult(), context.get());
		return new WhenClause(operand, result);
	}

	@Override
	protected Expression visitCoalesceExpression(CoalesceExpression node,
			Context<C> context) {
		ImmutableList.Builder<Expression> builder = ImmutableList.builder();
		for (Expression expression : node.getOperands()) {
			builder.add(rewrite(expression, context.get()));
		}
		return new CoalesceExpression(builder.build());
	}

	@Override
	public Expression visitFunctionCall(FunctionCall node, Context<C> context) {
		Window rewrittenWindow = node.getWindow().orNull();
		if (rewrittenWindow != null) {
			ImmutableList.Builder<Expression> partitionBy = ImmutableList
					.builder();
			for (Expression expression : rewrittenWindow.getPartitionBy()) {
				partitionBy.add(rewrite(expression, context.get()));
			}

			// Since SortItem is not an Expression, but contains Expressions,
			// just process the default rewrite inline with FunctionCall
			ImmutableList.Builder<SortItem> orderBy = ImmutableList.builder();
			for (SortItem sortItem : rewrittenWindow.getOrderBy()) {
				Expression sortKey = rewrite(sortItem.getSortKey(),
						context.get());
				if (sortItem.getSortKey() != sortKey) {
					orderBy.add(new SortItem(sortKey, sortItem.getOrdering(),
							sortItem.getNullOrdering()));
				} else {
					orderBy.add(sortItem);
				}
			}

			// TODO: rewrite frame
			if (!sameElements(rewrittenWindow.getPartitionBy(),
					partitionBy.build())
					|| !sameElements(rewrittenWindow.getOrderBy(),
							orderBy.build())) {
				rewrittenWindow = new Window(partitionBy.build(),
						orderBy.build(), rewrittenWindow.getFrame().orNull());
			}
		}

		ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
		for (Expression expression : node.getArguments()) {
			arguments.add(rewrite(expression, context.get()));
		}
		FunctionCall fc = new FunctionCall(node.getName(), rewrittenWindow,
				node.isDistinct(), arguments.build());
		String name = fc.getName().getSuffix();

		if (DATE_LITERAL.equals(name)) {
			return fc.getArguments().get(0);
		} else if (TIMESTAMP_LITERAL.equals(name)) {
			return fc.getArguments().get(0);
		} else if (TSRANZE_LITERAL.equals(name)) {
			try {
				StringLiteral stringLiteral = (StringLiteral) fc.getArguments()
						.get(0);
				TimeStampRange timeStampRange = TSRange.deSerialize(Slices
						.utf8Slice(stringLiteral.getValue()));
				TimeStamp lower = timeStampRange.lower();
				TimeStamp upper = timeStampRange.upper();
				return new StringLiteral((lower == null ? 0
						: lower.getTimestamp()/1000)
						+ "#" + (upper == null ? 0 : upper.getTimestamp()/1000));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		} else if (TSTZRANE_LITERAL.equals(name)) {
			return fc.getArguments().get(0);
		} else if (INT4RANGE_LITERAL.equals(name)) {
			return fc.getArguments().get(0);
		}
		return fc;
	}

	@Override
	public Expression visitLikePredicate(LikePredicate node, Context<C> context) {

		Expression value = rewrite(node.getValue(), context.get());
		Expression pattern = rewrite(node.getPattern(), context.get());
		Expression escape = null;
		if (node.getEscape() != null) {
			escape = rewrite(node.getEscape(), context.get());
		}
		return new LikePredicate(value, pattern, escape);
	}

	@Override
	public Expression visitInPredicate(InPredicate node, Context<C> context) {
		Expression value = rewrite(node.getValue(), context.get());
		Expression list = rewrite(node.getValueList(), context.get());
		return new InPredicate(value, list);
	}

	@Override
	protected Expression visitInListExpression(InListExpression node,
			Context<C> context) {

		ImmutableList.Builder<Expression> builder = ImmutableList.builder();
		for (Expression expression : node.getValues()) {
			builder.add(rewrite(expression, context.get()));
		}
		return new InListExpression(builder.build());
	}

	@Override
	public Expression visitSubqueryExpression(SubqueryExpression node,
			Context<C> context) {
		// return node;
		throw new UnsupportedOperationException(this.getClass().getName()
				+ " visitSubqueryExpression");
	}

	@Override
	public Expression visitLiteral(Literal node, Context<C> context) {
		throw new UnsupportedOperationException(this.getClass().getName()
				+ " visitSubqueryExpression");
	}

	protected Expression visitDoubleLiteral(DoubleLiteral node,
			Context<C> context) {
		return new DoubleLiteral(((Double) node.getValue()).toString());
	}

	protected Expression visitGenericLiteral(GenericLiteral node,
			Context<C> context) {
		return new GenericLiteral(node.getType(), node.getValue());
	}

	protected Expression visitTimeLiteral(TimeLiteral node, Context<C> context) {
		return new TimeLiteral(node.getValue());
	}

	protected Expression visitTimestampLiteral(TimestampLiteral node,
			Context<C> context) {
		return new TimestampLiteral(node.getValue());
	}

	protected Expression visitIntervalLiteral(IntervalLiteral node,
			Context<C> context) {
		return new IntervalLiteral(node.getValue(), node.getSign(),
				node.getStartField());
	}

	protected Expression visitStringLiteral(StringLiteral node,
			Context<C> context) {
		return new StringLiteral(node.getValue());
	}

	protected Expression visitLongLiteral(LongLiteral node, Context<C> context) {
		return new LongLiteral(((Long) node.getValue()).toString());
	}

	@Override
	public Expression visitQualifiedNameReference(QualifiedNameReference node,
			Context<C> context) {
		return new QualifiedNameReference(node.getName());
	}

	@Override
	protected Expression visitExtract(Extract node, Context<C> context) {

		Expression expression = rewrite(node.getExpression(), context.get());
		return new Extract(expression, node.getField());
	}

	@Override
	protected Expression visitCurrentTime(CurrentTime node, Context<C> context) {
		// return node;
		throw new UnsupportedOperationException(this.getClass().getName()
				+ " visitSubqueryExpression");
	}

	@Override
	public Expression visitCast(Cast node, Context<C> context) {

		Expression expression = rewrite(node.getExpression(), context.get());
		return new Cast(expression, node.getType(), node.isSafe());
	}
}