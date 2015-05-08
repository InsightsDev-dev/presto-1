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
				.row("timerange", "varchar", true, false, "")
				.row("small", "bigint", true, false, "")
				.row("float", "double", true, false, "").build();
		assertEquals(testNoFilterProteumQueryRunner.execute("DESC CUSTOMER"),
				default_proteum);
		assertEqualsQueryQuery("DESC CUSTOMER");
		final String orderByCustomer = " ORDER BY seq,num,str,timerange,small,float";
		final String orderBySeqEqualsSeqVirtualNumMod3 = " ORDER BY small,num,str,float,num_mod_three";
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
		assertEqualsQueryQuery("select * from customer where num>5 AND num<3900 OR NOT(num >4000 AND num < 4005)"+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where NOT num>5 AND "
				+ "num<3900 OR NOT (num >4000 AND num < 4905) OR str not like '%A%'"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num is not null OR"
				+ " str is not null OR timerange is not null" + orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num is null OR"
				+ " str is null OR timerange is null" + orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num=-1"
				+ orderByCustomer);

		assertEqualsQueryQuery("select * from seq_equals_seq_virtual_num_mod_3 where"
				+ " (num_mod_three =1 OR num > 5) AND small> 78 AND num_mod_three=0"
				+ orderBySeqEqualsSeqVirtualNumMod3);
		assertEqualsQueryQuery("select * from customer where seq<=4700 and (small is null OR small >= 1000) and str is not null"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where seq<=4700 and (small = 22 OR small >= 1000)"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where seq<=4700 and (small is null OR small >= 1000)"
				+ orderByCustomer);
		assertEqualsQueryQuery("select * from customer where num >= 1000 and num <=1200"
				+ orderByCustomer);
//		assertDiffrentQueries(
//				"select customer.seq,customer.num,customer.str,customer1.small,customer1.float "
//						+ "from customer join customer1 on customer.seq=customer1.seq order by customer.seq",
//				"select * from seq_equals_seq order by seq");
//		assertDiffrentQueries(
//				"select small,num from small_equals_small where small is not null order by small",
//				"select customer.small,sum(customer.num) as num from customer join customer1 on"
//						+ " customer.small=customer1.small group by customer.small order by customer.small");
//		assertEqualsQueryQuery("select count(*) from seq_equals_seq_all_dimensions where (num < 4992 OR num >= 17) "
//				+ "AND "
//				+ "(str is null OR timerange is null OR small is not null OR float is not null)");

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

	private void assertDiffrentQueries(String query1, String query2) {
		assertEquals(testDefaultProteumQueryRunner.execute(query1),
				testDefaultProteumQueryRunner.execute(query2));
	}

	private void assertEqualsQueryQuery(String actual) {
		assertEqualsQueryQuery(actual, actual);
	}

	private void assertEqualsQueryQuery(String actual, String expected) {
		assertEquals(testDefaultProteumQueryRunner.execute(actual),
				testNoFilterProteumQueryRunner.execute(expected));
	}
}
