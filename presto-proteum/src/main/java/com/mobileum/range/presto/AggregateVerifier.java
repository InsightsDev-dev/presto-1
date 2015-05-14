package com.mobileum.range.presto;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.ObjectValue;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RangeOperators;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * 
 * @author dilip kasana
 * @Date 12-May-2015
 */
public class AggregateVerifier {
	public static boolean verify(String aggregates) {
		try {
			List<Function> functions = new ArrayList<Function>();
			if (!aggregates.isEmpty()) {
				functions.addAll(getFunctions(URLDecoder.decode(aggregates,
						"UTF-8")));
			}
			boolean ret = true;
			for (Function f : functions) {
				ret = ret & AggregateFunctionVerfier.verify(f);
				if (!ret) {
					return ret;
				}
			}
			return ret;
		} catch (Exception e) {
			return false;
		}
	}

	private static Collection<? extends Function> getFunctions(String aggregates) {
		if (aggregates.startsWith("aggregates=")) {
			aggregates = aggregates.substring("aggregates=".length());
		} else {
			throw new RuntimeException("Invalid Aggregates");
		}
		if (aggregates.isEmpty()) {
			return null;
		}
		List<Function> functions = new ArrayList<Function>();
		List<SelectItem> selectItems = null;
		try {
			selectItems = ((PlainSelect) ((Select) CCJSqlParserUtil
					.parse("SELECT " + aggregates.replace("\"", "")
							+ " from t1")).getSelectBody()).getSelectItems();
		} catch (JSQLParserException e) {
			throw new RuntimeException(e);
		}
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpressionItem.getExpression();
				if (expression instanceof Function) {
					Function aggregateFunction = (Function) expression;
					functions.add(aggregateFunction);
				}
			}
		}
		return functions;
	}
}

class AggregateFunctionVerfier {
	public static boolean verify(Function function) {
		if (function.getName().toLowerCase().equals("sum")) {
			if (function.getParameters() == null) {
				return false;
			} else if (function.getParameters().getExpressions() == null
					|| function.getParameters().getExpressions().isEmpty()) {
				return false;
			} else {
				boolean ret = true;
				for (Expression e : function.getParameters().getExpressions()) {
					ret = ret & AggregateExpressionVerfier.verify(e);
					if (!ret) {
						return ret;
					}
				}
				return ret;
			}
		} else if (function.getName().toLowerCase().equals("count")) {
			if (function.getParameters() == null) {
				return false;
			} else if (function.getParameters().getExpressions() == null
					|| function.getParameters().getExpressions().size() == 0) {
				return false;
			} else {
				Expression expression = function.getParameters()
						.getExpressions().get(0);
				if (!(expression instanceof Column)) {
					return false;
				} else {
					return true;
				}
			}
		}
		return false;
	}
}

class AggregateExpressionVerfier {
	static boolean verify(Expression e) {
		try {
			e.accept(new Visitor(), new Analysis());
		} catch (UnsupportedOperationException e1) {
			return false;
		}
		return true;
	}
}

class Analysis {
	List<String> columnNames;

	public Analysis() {
		columnNames = new ArrayList<String>();
	}
}

class Visitor implements ExpressionVisitor<Object, Analysis>,
		ItemsListVisitor<Object, Analysis> {
	public Visitor() {

	}

	@Override
	public Object visit(ExpressionList expressionList, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(MultiExpressionList multiExprList, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(NullValue nullValue, Analysis Object) {
		return null;
	}

	@Override
	public Object visit(Function function, Analysis Object) {
		List<Expression> list = function.getParameters().getExpressions();
		for (Expression e : list) {
			e.accept(this, Object);
		}
		String functionName = function.getName().toLowerCase();
		if (functionName.equals("sum") || functionName.equals("ceil")) {
			return true;
		} else {
			throw new UnsupportedOperationException(functionName);
		}
	}

	@Override
	public Object visit(SignedExpression signedExpression, Analysis Object) {
		signedExpression.getExpression().accept(this, Object);
		return null;
	}

	@Override
	public Object visit(JdbcParameter jdbcParameter, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(JdbcNamedParameter jdbcNamedParameter, Analysis Object) {
		throw new UnsupportedOperationException();

	}

	@Override
	public Object visit(DoubleValue doubleValue, Analysis Object) {
		return null;
	}

	@Override
	public Object visit(LongValue longValue, Analysis Object) {
		return null;
	}

	@Override
	public Object visit(DateValue dateValue, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(TimeValue timeValue, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(TimestampValue timestampValue, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(Parenthesis parenthesis, Analysis Object) {
		parenthesis.getExpression().accept(this, Object);
		return null;
	}

	@Override
	public Object visit(StringValue stringValue, Analysis Object) {
		return null;
	}

	@Override
	public Object visit(Addition addition, Analysis Object) {
		visitBinaryExpression(addition, Object);
		return null;
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression,
			Analysis Object) {
		binaryExpression.getLeftExpression().accept(this, Object);
		binaryExpression.getRightExpression().accept(this, Object);
	}

	@Override
	public Object visit(Division division, Analysis Object) {
		visitBinaryExpression(division, Object);
		return null;
	}

	@Override
	public Object visit(Multiplication multiplication, Analysis Object) {
		visitBinaryExpression(multiplication, Object);
		return null;
	}

	@Override
	public Object visit(Subtraction subtraction, Analysis Object) {
		visitBinaryExpression(subtraction, Object);
		return null;
	}

	@Override
	public Object visit(AndExpression andExpression, Analysis Object) {
		andExpression.getLeftExpression().accept(this, Object);
		andExpression.getRightExpression().accept(this, Object);
		return null;
	}

	@Override
	public Object visit(OrExpression orExpression, Analysis Object) {
		orExpression.getLeftExpression().accept(this, Object);
		orExpression.getRightExpression().accept(this, Object);
		return null;
	}

	@Override
	public Object visit(Between between, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	public void visitComparisionExpression(BinaryExpression binaryExpression,
			Analysis Object) {
		binaryExpression.getLeftExpression().accept(this, Object);
		binaryExpression.getRightExpression().accept(this, Object);
	}

	@Override
	public Object visit(EqualsTo equalsTo, Analysis Object) {
		visitComparisionExpression(equalsTo, Object);
		return null;
	}

	@Override
	public Object visit(GreaterThan greaterThan, Analysis Object) {
		visitComparisionExpression(greaterThan, Object);
		return null;
	}

	@Override
	public Object visit(GreaterThanEquals greaterThanEquals, Analysis Object) {
		visitComparisionExpression(greaterThanEquals, Object);
		return null;
	}

	@Override
	public Object visit(InExpression inExpression, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(IsNullExpression isNullExpression, Analysis Object) {
		isNullExpression.getLeftExpression().accept(this, Object);
		return null;
	}

	@Override
	public Object visit(LikeExpression likeExpression, Analysis Object) {
		likeExpression.getLeftExpression().accept(this, Object);
		likeExpression.getRightExpression().accept(this, Object);
		return null;
	}

	@Override
	public Object visit(MinorThan minorThan, Analysis Object) {
		visitComparisionExpression(minorThan, Object);
		return null;
	}

	@Override
	public Object visit(MinorThanEquals minorThanEquals, Analysis Object) {
		visitComparisionExpression(minorThanEquals, Object);
		return null;
	}

	@Override
	public Object visit(NotEqualsTo notEqualsTo, Analysis Object) {
		visitComparisionExpression(notEqualsTo, Object);
		return null;
	}

	@Override
	public Object visit(Column tableColumn, Analysis Object) {
		Object.columnNames.add(tableColumn.getColumnName());
		return null;
	}

	@Override
	public Object visit(SubSelect subSelect, Analysis Object) {
		throw new UnsupportedOperationException("Not Supported Yet");
	}

	@Override
	public Object visit(CaseExpression caseExpression, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(WhenClause whenClause, Analysis Object) {
		// whenClause.getWhenExpression().accept(this, Object);
		// whenClause.getThenExpression().accept(this, Object);
		throw new UnsupportedOperationException();

	}

	@Override
	public Object visit(ExistsExpression existsExpression, Analysis Object) {
		// existsExpression.getRightExpression().accept(this, Object);
		// return null;
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(AllComparisonExpression allComparisonExpression,
			Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(AnyComparisonExpression anyComparisonExpression,
			Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(Concat concat, Analysis Object) {
		visitBinaryExpression(concat, Object);
		return null;
	}

	@Override
	public Object visit(Matches matches, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	public void visitBitwiseExpression(BinaryExpression binaryExpression,
			Analysis Object) {
		binaryExpression.getLeftExpression().accept(this, Object);
		binaryExpression.getRightExpression().accept(this, Object);
	}

	@Override
	public Object visit(BitwiseAnd bitwiseAnd, Analysis Object) {
		visitBitwiseExpression(bitwiseAnd, Object);
		return null;
	}

	@Override
	public Object visit(BitwiseOr bitwiseOr, Analysis Object) {
		visitBitwiseExpression(bitwiseOr, Object);
		return null;
	}

	@Override
	public Object visit(BitwiseXor bitwiseXor, Analysis Object) {
		visitBitwiseExpression(bitwiseXor, Object);
		return null;
	}

	@Override
	public Object visit(CastExpression cast, Analysis Object) {
		cast.getLeftExpression().accept(this, Object);
		String castClass = cast.getType().getDataType();
		if (castClass.equalsIgnoreCase("String")
				|| castClass.equalsIgnoreCase("VARCHAR")) {
			return null;
		} else if (castClass.equalsIgnoreCase("Boolean")) {
			return null;
		} else if (castClass.equalsIgnoreCase("Long")
				|| castClass.equalsIgnoreCase("BIGINT")) {
			return null;
		} else if (castClass.equalsIgnoreCase("int")
				|| castClass.equalsIgnoreCase("Integer")) {
			return null;
		} else if (castClass.equalsIgnoreCase("Double")) {
			return null;
		} else if (castClass.equalsIgnoreCase("tsrange")) {
			return null;
		}
		throw new RuntimeException("Unsupported cast type " + castClass);// null;
	}

	@Override
	public Object visit(Modulo modulo, Analysis Object) {
		visitBinaryExpression(modulo, Object);
		return null;
	}

	@Override
	public Object visit(AnalyticExpression aexpr, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(ExtractExpression eexpr, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(IntervalExpression iexpr, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(OracleHierarchicalExpression oexpr, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(RegExpMatchOperator rexpr, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(JsonExpression jsonExpr, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(RegExpMySQLOperator regExpMySQLOperator, Analysis Object) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(RangeOperators rangeOperators, Analysis Object) {
		visitBinaryExpression(rangeOperators, Object);
		return null;
	}

	@Override
	public Object visit(BooleanValue booleanValue, Analysis Object) {
		return null;
	}

	@Override
	public Object visit(ObjectValue objectValue, Analysis Object) {
		return null;
	}

}
