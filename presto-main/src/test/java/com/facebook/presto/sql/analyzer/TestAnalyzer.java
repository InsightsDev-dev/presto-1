package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.*;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestAnalyzer
{
    private Analyzer analyzer;

    @Test
    public void testDuplicateRelation()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION, "SELECT * FROM t1 JOIN t1 USING (a)");
        assertFails(DUPLICATE_RELATION, "SELECT * FROM t1 x JOIN t2 x USING (a)");
    }

    @Test
    public void testHavingReferencesOutputAlias()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT sum(a) x FROM t1 HAVING x > 5");
    }

    @Test
    public void testWildcardWithInvalidPrefix()
            throws Exception
    {
        assertFails(MISSING_TABLE, "SELECT foo.* FROM t1");
    }

    @Test
    public void testGroupByWithWildcard()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 USING (a) GROUP BY u1.a, u2.a, 3");
    }

    @Test
    public void testGroupByInvalidOrdinal()
            throws Exception
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 0");
    }

    @Test
    public void testOrderByInvalidOrdinal()
            throws Exception
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 0");
    }

    @Test
    public void testNestedAggregation()
            throws Exception
    {
        assertFails(NESTED_AGGREGATION, "SELECT sum(count(*)) FROM t1");
    }

    @Test
    public void testAggregationsNotAllowed()
            throws Exception
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 WHERE sum(a) > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 GROUP BY sum(a)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a");
    }

    @Test
    public void testWindowsNotAllowed()
            throws Exception
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 WHERE foo() over () > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 GROUP BY rank() over ()");
        assertFails(CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a");
    }

    @Test
    public void testInvalidTable()
            throws Exception
    {
        assertFails(MISSING_TABLE, "SELECT * FROM foo");
    }

    @Test
    public void testNonAggregate()
            throws Exception
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1 GROUP BY c");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) FROM t1 ORDER BY a + 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1 GROUP BY a HAVING c > 5");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (PARTITION BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN b PRECEDING AND a PRECEDING) FROM t1 GROUP BY b");
    }

    @Test
    public void testInvalidAttribute()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT f FROM t1");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 ORDER BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT count(*) FROM t1 GROUP BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 WHERE f > 1");
    }

    @Test
    public void testOrderByMustAppearInSelectWithDistinct()
            throws Exception
    {
        assertFails(ORDER_BY_MUST_BE_IN_SELECT, "SELECT DISTINCT a FROM t1 ORDER BY b");
    }

    @Test
    public void testNonBooleanWhereClause()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE a");
    }

    @Test
    public void testDistinctAggregations()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT COUNT(DISTINCT a) FROM t1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT a x FROM t1 ORDER BY x + 1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");
    }

    @Test
    public void testOrderByWithWildcard()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testMismatchedColumnAliasCount()
            throws Exception
    {
        assertFails(MISMATCHED_COLUMN_ALIASES, "SELECT * FROM t1 u (x, y)");
    }

    @Test
    public void testJoinOnConstantExpression()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testJoinOnAmbiguousName()
            throws Exception
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT * FROM t1 JOIN t2 ON a = a");
    }

    @Test
    public void testNonEquiJoin()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 JOIN t2 ON t1.a < t2.a");
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 JOIN t2 ON t1.a + t2.a = 1");
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testNonBooleanHaving()
            throws Exception
    {
        assertFails(TYPE_MISMATCH, "SELECT sum(a) FROM t1 HAVING sum(a)");
    }

    @Test
    public void testAmbiguousReferenceInOrderBy()
            throws Exception
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, b x FROM t1 ORDER BY x");
    }

    @Test
    public void testNaturalJoinNotSupported()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 NATURAL JOIN t2");
    }

    @Test
    public void testOuterJoinNotSupported()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 LEFT OUTER JOIN t2 USING (a)");
    }

    @Test
    public void testNestedWindowFunctions()
            throws Exception
    {
        assertFails(NESTED_WINDOW, "SELECT avg(sum(a) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT sum(sum(a) OVER ()) OVER () FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (PARTITION BY sum(b) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (ORDER BY sum(b) OVER ()) FROM t1");
    }

    @Test
    public void testDistinctInWindowFunctionParameter()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT a, count(DISTINCT b) OVER () FROM t1");
    }

    @Test
    public void testWindowFrameNotSupported()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT count(*) over (ORDER BY a ROWS UNBOUNDED PRECEDING) FROM t1");
        assertFails(NOT_SUPPORTED, "SELECT count(*) over (ORDER BY a ROWS UNBOUNDED FOLLOWING) FROM t1");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testHaving()
            throws Exception
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }

    @Test
    public void testDuplicateWithQuery()
            throws Exception
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     a AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testWithForwardReference()
            throws Exception
    {
        assertFails(MISSING_TABLE,
                "WITH a AS (SELECT * FROM b)," +
                        "     b AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testExpressions()
            throws Exception
    {
        assertFails(NOT_SUPPORTED, "SELECT CURRENT_TIME FROM t1");
        assertFails(NOT_SUPPORTED, "SELECT CURRENT_TIMESTAMP (1) FROM t1");
        assertFails(NOT_SUPPORTED, "SELECT CURRENT_DATE FROM t1");

        // logical not
        assertFails(TYPE_MISMATCH, "SELECT NOT 1 FROM t1");

        // logical and/or
        assertFails(TYPE_MISMATCH, "SELECT 1 AND TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE AND 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 OR TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE OR 1 FROM t1");

        // comparison
        assertFails(TYPE_MISMATCH, "SELECT 1 = 'a' FROM t1");

        // nullif
        assertFails(TYPE_MISMATCH, "SELECT NULLIF(1, 'a') FROM t1");

        // case
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN TRUE THEN 'a' ELSE 1 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN '1' THEN 1 ELSE 2 END FROM t1");

        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 'a' THEN 2 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 1 THEN 2 ELSE 'a' END FROM t1");

        // coalesce
        assertFails(TYPE_MISMATCH, "SELECT COALESCE(1, 'a') FROM t1");

        // arithmetic negation
        assertFails(TYPE_MISMATCH, "SELECT -'a' FROM t1");

        // arithmetic addition/subtraction
        assertFails(TYPE_MISMATCH, "SELECT 'a' + 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 + 'a'  FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' - 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 - 'a' FROM t1");

        // like
        assertFails(TYPE_MISMATCH, "SELECT 1 LIKE 'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 'b' ESCAPE 1 FROM t1");

        // extract
        assertFails(TYPE_MISMATCH, "SELECT EXTRACT(DAY FROM 'a') FROM t1");

        // between
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 2 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 0 AND 'b' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 'b' FROM t1");

        // in
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 1 IN ('a')");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1, 'b')");
    }

    @Test(enabled=false) // TODO: need to support widening conversion for numbers
    public void testInWithNumericTypes()
            throws Exception
    {
        analyze("SELECT * FROM t1 WHERE 1 IN (1, 2, 3.5)");
    }

    @Test
    public void testWildcardWithoutFrom()
            throws Exception
    {
        assertFails(WILDCARD_WITHOUT_FROM, "SELECT *");
    }


    @Test
    public void testReferenceWithoutFrom()
            throws Exception
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT dummy");
    }

    @Test
    public void testGroupBy()
            throws Exception
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        TestingMetadata metadata = new TestingMetadata();

        QualifiedTableName table1 = new QualifiedTableName("default", "default", "t1");
        metadata.createTable(new TableMetadata(table1,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(1)),
                        new ColumnMetadata("b", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(2)),
                        new ColumnMetadata("c", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(3)),
                        new ColumnMetadata("d", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(4))
                ), new NativeTableHandle(table1, 1)));

        QualifiedTableName table2 = new QualifiedTableName("default", "default", "t2");
        metadata.createTable(new TableMetadata(table2,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(1)),
                        new ColumnMetadata("b", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(2))
                ), new NativeTableHandle(table2, 2)));

        QualifiedTableName table3 = new QualifiedTableName("default", "default", "t3");
        metadata.createTable(new TableMetadata(table3,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(1)),
                        new ColumnMetadata("b", TupleInfo.Type.FIXED_INT_64, new NativeColumnHandle(2))
                ), new NativeTableHandle(table3, 3)));

        analyzer = new Analyzer(new Session(null, "default", "default"), metadata);
    }

    private void analyze(@Language("SQL") String query)
    {
        Statement statement = SqlParser.createStatement(query);
        analyzer.analyze(statement);
    }

    private void assertFails(SemanticErrorCode error, @Language("SQL") String query)
    {
        try {
            Statement statement = SqlParser.createStatement(query);
            analyzer.analyze(statement);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }
        }
    }
}
