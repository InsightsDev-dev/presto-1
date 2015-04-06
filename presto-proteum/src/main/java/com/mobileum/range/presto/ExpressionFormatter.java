/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mobileum.range.presto;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.google.common.collect.Iterables.transform;

/**
 * 
 * @author dilip kasana
 * @Date 13-Feb-2015
 */
public final class ExpressionFormatter {
	private ExpressionFormatter() {
	}

	public static String formatExpression(Expression expression) {
		return new Formatter().process(expression, null);
	}

	public static Function<Expression, String> expressionFormatterFunction() {
		return new Function<Expression, String>() {
			@Override
			public String apply(Expression input) {
				return formatExpression(input);
			}
		};
	}

	public static class Formatter extends AstVisitor<String, Void> {

		@Override
		protected String visitLogicalBinaryExpression(
				LogicalBinaryExpression node, Void context) {
			if (node.getType().equals(LogicalBinaryExpression.Type.AND)) {
				Expression left = node.getLeft();
				Expression right = node.getRight();
				String operator = node.getType().toString();
				String leftString = null;
				try {
					leftString = process(left, null);
				} catch (Exception e) {

				}
				String rightString = null;
				try {
					rightString = process(right, null);
				} catch (Exception e) {

				}
				if(leftString==null && rightString==null){
					throw new UnsupportedOperationException();
				}
				else
				if (leftString != null && rightString == null) {
					return '(' + leftString + ')';
				} else if (leftString == null && rightString != null) {
					return '(' + rightString + ')';
				}
				return '(' + process(left, null) + ' ' + operator + ' '
						+ process(right, null) + ')';
			} else if (node.getType().equals(LogicalBinaryExpression.Type.OR)) {
				Expression left = node.getLeft();
				Expression right = node.getRight();
				String operator = node.getType().toString();
				String leftString = null;
				try {
					leftString = process(left, null);
				} catch (Exception e) {

				}
				String rightString = null;
				try {
					rightString = process(right, null);
				} catch (Exception e) {

				}
				if (leftString == null || rightString == null) {
					throw new UnsupportedOperationException();
				}
				return '(' + process(left, null) + ' ' + operator + ' '
						+ process(right, null) + ')';
			}
			return formatBinaryExpression(node.getType().toString(),
					node.getLeft(), node.getRight());
		}

		@Override
		protected String visitNotExpression(NotExpression node, Void context) {
			return "(NOT " + process(node.getValue(), null) + ")";
		}

		@Override
		protected String visitComparisonExpression(ComparisonExpression node,
				Void context) {
			if (node.getType() == ComparisonExpression.Type.OVERLAPPING_WITH) {
				return formatBinaryExpression("###", node.getLeft(),
						node.getRight());
			}
			return formatBinaryExpression(node.getType().getValue(),
					node.getLeft(), node.getRight());
		}

		@Override
		protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
			return "(" + process(node.getValue(), null) + " IS NULL)";
		}

		@Override
		protected String visitIsNotNullPredicate(IsNotNullPredicate node,
				Void context) {
			return "(" + process(node.getValue(), null) + " IS NOT NULL)";
		}

		@Override
		protected String visitNegativeExpression(NegativeExpression node,
				Void context) {
			String value = process(node.getValue(), null);
			String separator = value.startsWith("-") ? " " : "";
			return "-" + separator + value;
		}

		@Override
		protected String visitArithmeticExpression(ArithmeticExpression node,
				Void context) {
			return formatBinaryExpression(node.getType().getValue(),
					node.getLeft(), node.getRight());
		}

		@Override
		protected String visitBetweenPredicate(BetweenPredicate node,
				Void context) {

			String expression = process(node.getValue(), context);
			Expression min = node.getMin();
			Expression max = node.getMax();
			String minString = null;
			try {
				minString = process(min, null);
			} catch (Exception e) {

			}
			String maxString = null;
			try {
				maxString = process(max, null);
			} catch (Exception e) {

			}
			if (minString == null && maxString != null) {
				return "(" + expression + " <= " + maxString + ")";
			} else if (minString != null && maxString == null) {
				return "(" + expression + " >= " + maxString + ")";
			}

			return "(" + expression + " BETWEEN " + minString + " AND "
					+ maxString + ")";
		}

		@Override
		protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
			boolean b = node.getValue();
			if (b) {
				return TRUE_CAST.accept(this, context);
			} else {
				return FALSE_CAST.accept(this, context);
			}
		}

		Cast TRUE_CAST = new Cast(new StringLiteral("true"), "Boolean");
		Cast FALSE_CAST = new Cast(new StringLiteral("false"), "Boolean");

		ComparisonExpression TRUE = new ComparisonExpression(
				ComparisonExpression.Type.EQUAL, new LongLiteral("1"),
				new LongLiteral("1"));
		ComparisonExpression FALSE = new ComparisonExpression(
				ComparisonExpression.Type.EQUAL, new LongLiteral("1"),
				new LongLiteral("2"));

		@Override
		protected String visitStringLiteral(StringLiteral node, Void context) {
			return formatStringLiteral(node.getValue());
		}

		@Override
		protected String visitLongLiteral(LongLiteral node, Void context) {
			return Long.toString(node.getValue());
		}

		@Override
		protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
			return Double.toString(node.getValue());
		}

		@Override
		protected String visitNullLiteral(NullLiteral node, Void context) {
			return "null";
		}

		@Override
		protected String visitQualifiedNameReference(
				QualifiedNameReference node, Void context) {
			return formatQualifiedName(node.getName());
		}

		private static String formatQualifiedName(QualifiedName name) {
			List<String> parts = new ArrayList<>();
			for (String part : name.getParts()) {
				parts.add(formatIdentifier(part));
			}
			return Joiner.on('.').join(parts);
		}

		private String formatBinaryExpression(String operator, Expression left,
				Expression right) {
			return '(' + process(left, null) + ' ' + operator + ' '
					+ process(right, null) + ')';
		}

		static String formatStringLiteral(String s) {
			return "'" + s.replace("'", "''") + "'";
			// return s.replace("'", "''") ;
		}

		@Override
		protected String visitNode(Node node, Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitExpression(Expression node, Void context) {
			throw new UnsupportedOperationException(String.format(
					"not yet implemented: %s.visit%s", getClass().getName(),
					node.getClass().getSimpleName()));
		}

		@Override
		protected String visitCurrentTime(CurrentTime node, Void context) {
			// StringBuilder builder = new StringBuilder();
			//
			// builder.append(node.getType().getName());
			//
			// if (node.getPrecision() != null) {
			// builder.append('(').append(node.getPrecision()).append(')');
			// }
			//
			// return builder.toString();
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitExtract(Extract node, Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitArrayConstructor(ArrayConstructor node,
				Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitSubscriptExpression(SubscriptExpression node,
				Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitGenericLiteral(GenericLiteral node, Void context) {
			return node.getType() + " '" + node.getValue() + "'";
		}

		@Override
		protected String visitTimeLiteral(TimeLiteral node, Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitTimestampLiteral(TimestampLiteral node,
				Void context) {
			throw new UnsupportedOperationException();

		}

		@Override
		protected String visitIntervalLiteral(IntervalLiteral node, Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitSubqueryExpression(SubqueryExpression node,
				Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitExists(ExistsPredicate node, Void context) {
			return "EXISTS (" + formatSql(node.getSubquery()) + ")";
		}

		@Override
		public String visitInputReference(InputReference node, Void context) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitFunctionCall(FunctionCall node, Void context) {
			StringBuilder builder = new StringBuilder();

			String arguments = joinExpressions(node.getArguments());
			if (node.getArguments().isEmpty()
					&& "count".equalsIgnoreCase(node.getName().getSuffix())) {
				arguments = "*";
				throw new UnsupportedOperationException();
			}
			if (node.isDistinct()) {
				arguments = "DISTINCT " + arguments;
				throw new UnsupportedOperationException();
			}

			builder.append(formatQualifiedName(node.getName())).append('(')
					.append(arguments).append(')');

			if (node.getWindow().isPresent()) {
				builder.append(" OVER ").append(
						visitWindow(node.getWindow().get(), null));
				throw new UnsupportedOperationException();
			}

			return builder.toString();
		}

		@Override
		protected String visitNullIfExpression(NullIfExpression node,
				Void context) {
			// return "NULLIF(" + process(node.getFirst(), null) + ", " +
			// process(node.getSecond(), null) + ')';
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitIfExpression(IfExpression node, Void context) {
			// StringBuilder builder = new StringBuilder();
			// builder.append("IF(").append(process(node.getCondition(),
			// context))
			// .append(", ").append(process(node.getTrueValue(), context));
			// if (node.getFalseValue().isPresent()) {
			// builder.append(", ").append(
			// process(node.getFalseValue().get(), context));
			// }
			// builder.append(")");
			// return builder.toString();
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitCoalesceExpression(CoalesceExpression node,
				Void context) {
			// return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitLikePredicate(LikePredicate node, Void context) {
			StringBuilder builder = new StringBuilder();

			builder.append('(').append(process(node.getValue(), null))
					.append(" LIKE ").append(process(node.getPattern(), null));

			if (node.getEscape() != null) {
				builder.append(" ESCAPE ").append(
						process(node.getEscape(), null));
			}

			builder.append(')');

			return builder.toString();
		}

		@Override
		protected String visitAllColumns(AllColumns node, Void context) {
			// if (node.getPrefix().isPresent()) {
			// return node.getPrefix().get() + ".*";
			// }
			//
			// return "*";
			throw new UnsupportedOperationException();
		}

		@Override
		public String visitCast(Cast node, Void context) {
			if (node.isSafe()) {
				throw new UnsupportedOperationException();
			} else {
				return "CAST" + "(" + process(node.getExpression(), context)
						+ " AS " + node.getType() + ")";
			}
		}

		@Override
		protected String visitSearchedCaseExpression(
				SearchedCaseExpression node, Void context) {
			ImmutableList.Builder<String> parts = ImmutableList.builder();
			parts.add("CASE");
			for (WhenClause whenClause : node.getWhenClauses()) {
				parts.add(process(whenClause, context));
			}
			if (node.getDefaultValue() != null) {
				parts.add("ELSE").add(process(node.getDefaultValue(), context));
			}
			parts.add("END");

			return "(" + Joiner.on(' ').join(parts.build()) + ")";
		}

		@Override
		protected String visitSimpleCaseExpression(SimpleCaseExpression node,
				Void context) {
			ImmutableList.Builder<String> parts = ImmutableList.builder();

			parts.add("CASE").add(process(node.getOperand(), context));

			for (WhenClause whenClause : node.getWhenClauses()) {
				parts.add(process(whenClause, context));
			}
			if (node.getDefaultValue() != null) {
				parts.add("ELSE").add(process(node.getDefaultValue(), context));
			}
			parts.add("END");

			return "(" + Joiner.on(' ').join(parts.build()) + ")";
			// throw new UnsupportedOperationException();
		}

		@Override
		protected String visitWhenClause(WhenClause node, Void context) {
			return "WHEN " + process(node.getOperand(), context) + " THEN "
					+ process(node.getResult(), context);
		}

		@Override
		protected String visitInPredicate(InPredicate node, Void context) {
			// return "(" + process(node.getValue(), context) + " IN " +
			// process(node.getValueList(), context) + ")";
			throw new UnsupportedOperationException();
		}

		@Override
		protected String visitInListExpression(InListExpression node,
				Void context) {
			// return "(" + joinExpressions(node.getValues()) + ")";
			throw new UnsupportedOperationException();
		}

		// TODO: add tests for window clause formatting, as these are not really
		// expressions
		@Override
		public String visitWindow(Window node, Void context) {
			// List<String> parts = new ArrayList<>();
			//
			// if (!node.getPartitionBy().isEmpty()) {
			// parts.add("PARTITION BY " +
			// joinExpressions(node.getPartitionBy()));
			// }
			// if (!node.getOrderBy().isEmpty()) {
			// parts.add("ORDER BY " + formatSortItems(node.getOrderBy()));
			// }
			// if (node.getFrame().isPresent()) {
			// parts.add(process(node.getFrame().get(), null));
			// }
			//
			// return '(' + Joiner.on(' ').join(parts) + ')';
			throw new UnsupportedOperationException();
		}

		@Override
		public String visitWindowFrame(WindowFrame node, Void context) {
			// StringBuilder builder = new StringBuilder();
			//
			// builder.append(node.getType().toString()).append(' ');
			//
			// if (node.getEnd().isPresent()) {
			// builder.append("BETWEEN ")
			// .append(process(node.getStart(), null))
			// .append(" AND ")
			// .append(process(node.getEnd().get(), null));
			// }
			// else {
			// builder.append(process(node.getStart(), null));
			// }
			//
			// return builder.toString();
			throw new UnsupportedOperationException();
		}

		@Override
		public String visitFrameBound(FrameBound node, Void context) {
			throw new UnsupportedOperationException();
			// switch (node.getType()) {
			// case UNBOUNDED_PRECEDING:
			// return "UNBOUNDED PRECEDING";
			// case PRECEDING:
			// return process(node.getValue().get(), null) + " PRECEDING";
			// case CURRENT_ROW:
			// return "CURRENT ROW";
			// case FOLLOWING:
			// return process(node.getValue().get(), null) + " FOLLOWING";
			// case UNBOUNDED_FOLLOWING:
			// return "UNBOUNDED FOLLOWING";
			// }
			// throw new IllegalArgumentException("unhandled type: " +
			// node.getType());
		}

		private String joinExpressions(List<Expression> expressions) {
			return Joiner.on(", ").join(
					transform(expressions, new Function<Expression, Object>() {
						@Override
						public Object apply(Expression input) {
							return process(input, null);
						}
					}));
		}

		private static String formatIdentifier(String s) {
			// TODO: handle escaping properly
			// return '"' + s + '"';
			return s;
		}
	}

	static String formatSortItems(List<SortItem> sortItems) {
		return Joiner.on(", ").join(
				transform(sortItems, sortItemFormatterFunction()));
	}

	private static Function<SortItem, String> sortItemFormatterFunction() {
		return new Function<SortItem, String>() {
			@Override
			public String apply(SortItem input) {
				StringBuilder builder = new StringBuilder();

				builder.append(formatExpression(input.getSortKey()));

				switch (input.getOrdering()) {
				case ASCENDING:
					builder.append(" ASC");
					break;
				case DESCENDING:
					builder.append(" DESC");
					break;
				default:
					throw new UnsupportedOperationException(
							"unknown ordering: " + input.getOrdering());
				}

				switch (input.getNullOrdering()) {
				case FIRST:
					builder.append(" NULLS FIRST");
					break;
				case LAST:
					builder.append(" NULLS LAST");
					break;
				case UNDEFINED:
					// no op
					break;
				default:
					throw new UnsupportedOperationException(
							"unknown null ordering: " + input.getNullOrdering());
				}

				return builder.toString();
			}
		};
	}
}
