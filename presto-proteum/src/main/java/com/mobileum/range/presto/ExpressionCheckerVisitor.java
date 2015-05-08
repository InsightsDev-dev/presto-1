package com.mobileum.range.presto;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;

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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;

/**
 * 
 * @author dilip kasana
 * @Date 22-Apr-2015
 */
public class ExpressionCheckerVisitor {

	private ExpressionCheckerVisitor() {
	}

	public static boolean isOkExpression(Expression expression) {
		Expression newExpression;
		String TRUE_LITERAL = ExpressionFormatter
				.formatExpression(BooleanLiteral.TRUE_LITERAL);
		try {
			newExpression = new ExpressionSanityChecker<Void>().process(
					expression, new Context<Void>(null, false));
			Expression rewritedExpression = new RewritingVisitor<Void>()
					.process(newExpression, new Context<Void>(null, false));
			String formattedExpression = ExpressionFormatter
					.formatExpression(rewritedExpression);
			if (TRUE_LITERAL.equalsIgnoreCase(formattedExpression)) {
				return true;
			}
			String tempFilterString = formattedExpression.replaceAll("\\|\\|",
					" OR ").replaceAll("&&", " AND ");
			tempFilterString = tempFilterString.replaceAll("###", " &&");
			CCJSqlParserUtil.parseCondExpression(tempFilterString);
		} catch (Exception e) {
			System.out
					.println("Unable to Push Down Group By.Exception while parsing "
							+ expression);
			return false;
		}
		return true;
	}

	static class ExpressionSanityChecker<C> extends
			AstVisitor<Expression, Context<C>> {
		@Override
		protected Expression visitBooleanLiteral(BooleanLiteral node,
				Context<C> context) {
			return node;
		}

		@Override
		protected Expression visitNullLiteral(NullLiteral node,
				Context<C> context) {
			return node;
		}

		@Override
		protected Expression visitExpression(Expression node, Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		public Expression rewrite(Expression node, C context) {
			return (Expression) super.process(node, new Context<C>(context,
					false));
		}

		@Override
		protected Expression visitNegativeExpression(NegativeExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		public Expression visitArithmeticExpression(ArithmeticExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitArrayConstructor(ArrayConstructor node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitSubscriptExpression(SubscriptExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
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
			Expression value = rewrite(node.getValue(), context.get());
			Expression min = rewrite(node.getMin(), context.get());
			Expression max = rewrite(node.getMax(), context.get());
			return new BetweenPredicate(value, min, max);
		}

		@Override
		public Expression visitLogicalBinaryExpression(
				LogicalBinaryExpression node, Context<C> context) {
			Expression left = rewrite(node.getLeft(), context.get());
			Expression right = rewrite(node.getRight(), context.get());
			return new LogicalBinaryExpression(node.getType(), left, right);
		}

		@Override
		public Expression visitNotExpression(NotExpression node,
				Context<C> context) {
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
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitIfExpression(IfExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitSearchedCaseExpression(
				SearchedCaseExpression node, Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitSimpleCaseExpression(
				SimpleCaseExpression node, Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitWhenClause(WhenClause node, Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitCoalesceExpression(CoalesceExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		public Expression visitFunctionCall(FunctionCall node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		public Expression visitLikePredicate(LikePredicate node,
				Context<C> context) {
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
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitInListExpression(InListExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		public Expression visitSubqueryExpression(SubqueryExpression node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		public Expression visitLiteral(Literal node, Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		protected Expression visitDoubleLiteral(DoubleLiteral node,
				Context<C> context) {
			return new DoubleLiteral(((Double) node.getValue()).toString());
		}

		protected Expression visitGenericLiteral(GenericLiteral node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		protected Expression visitTimeLiteral(TimeLiteral node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		protected Expression visitTimestampLiteral(TimestampLiteral node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		protected Expression visitIntervalLiteral(IntervalLiteral node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		protected Expression visitStringLiteral(StringLiteral node,
				Context<C> context) {
			return new StringLiteral(node.getValue());
		}

		protected Expression visitLongLiteral(LongLiteral node,
				Context<C> context) {
			return new LongLiteral(((Long) node.getValue()).toString());
		}

		@Override
		public Expression visitQualifiedNameReference(
				QualifiedNameReference node, Context<C> context) {
			return new QualifiedNameReference(node.getName());
		}

		@Override
		protected Expression visitExtract(Extract node, Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		protected Expression visitCurrentTime(CurrentTime node,
				Context<C> context) {
			throw new UnsupportedOperationException("not yet implemented: "
					+ getClass().getSimpleName() + " for "
					+ node.getClass().getName());
		}

		@Override
		public Expression visitCast(Cast node, Context<C> context) {
			Expression expression = rewrite(node.getExpression(), context.get());
			return new Cast(expression, node.getType(), node.isSafe());
		}
	}
}
