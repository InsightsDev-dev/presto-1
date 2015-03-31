package com.mobileum.proteum.tests;

import static com.mobileum.proteum.tests.SessionTestUtils.PROTEUM_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.mobileum.range.presto.TSRangeType.TS_RANGE_TYPE;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
/**
 * 
 * @author Dilip Kasana
 * @Date 27 Mar 2015
 */
public class TestQueriesUtils {
	private DistributedQueryRunner testDefaultProteumQueryRunner;
	private DistributedQueryRunner testNoFilterProteumQueryRunner;

	public static void main(String[] args) throws Exception {
		new TestQueriesUtils().testDescribeTable();
		// TestQueriesUtils t = new TestQueriesUtils();
		// System.out
		// .println(t.testNoFilterProteumQueryRunner
		// .execute("select * from customer where num=53 AND str like '% %'"));

	}

	public TestQueriesUtils() throws Exception {
		testDefaultProteumQueryRunner = TestProteumQueryRunner
				.createDefaultQueryRunner();
		testNoFilterProteumQueryRunner = TestProteumQueryRunner
				.createNoFilterQueryRunner();
	}

	@AfterClass
	public void destroy() {
		// if (testDefaultProteumQueryRunner != null) {
		// testDefaultProteumQueryRunner.close();
		// }
		// if (testNoFilterProteumQueryRunner != null) {
		// testNoFilterProteumQueryRunner.close();
		// }
	}

	@Test
	public void testDescribeTable() throws Exception {
		MaterializedResult default_proteum = MaterializedResult
				.resultBuilder(PROTEUM_SESSION, BIGINT, BIGINT, VARCHAR,
						TS_RANGE_TYPE, BIGINT, DOUBLE)
				.row("seq", "bigint", true, false, "")
				.row("num", "bigint", true, false, "")
				.row("str", "varchar", true, false, "")
				.row("timerange", "tsrange", true, false, "")
				.row("small", "bigint", true, false, "")
				.row("float", "double", true, false, "").build();
		assertEquals(testNoFilterProteumQueryRunner.execute("DESC CUSTOMER"),
				default_proteum);
		assertEqualsQueryQuery("DESC CUSTOMER");
		final String orderByCustomer = " ORDER BY seq,num,str,timerange,small,float";
		assertEqualsQueryQuery("SELECT * FROM CUSTOMER" + orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num in (10,100,150,200,250,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15"
				+ ",16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44"
				+ ",45,46,47,48,49,50) and str not like '% %'"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num>5 AND num<3900 "
				+ "OR (num >4000 AND num < 4005) OR str like '% %'"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num>5 AND num<3900 "
				+ "OR (num >4000 AND num < 4005) OR str not like '% %'"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where NOT num>5 AND "
				+ "num<3900 OR NOT (num >4000 AND num < 4905) OR str not like '%A%'"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num is not null OR"
				+ " str is not null OR timerange is not null" + orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num is null OR"
				+ " str is null OR timerange is null" + orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num=-1"
				+ orderByCustomer);
		System.out
				.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out
				.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out
				.println("!!!!!!!!!!!!!!!!!!!!!!!!!DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out
				.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out
				.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

	}

	private void assertEqualsQueryQuery(String actual) {
		assertEqualsQueryQuery(actual, actual);
	}

	private void assertEqualsQueryQuery(String actual, String expected) {
		assertEquals(testDefaultProteumQueryRunner.execute(actual),
				testNoFilterProteumQueryRunner.execute(expected));
	}
}
