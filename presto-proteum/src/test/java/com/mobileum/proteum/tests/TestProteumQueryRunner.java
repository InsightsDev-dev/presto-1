package com.mobileum.proteum.tests;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.util.Locale.ENGLISH;

import com.facebook.presto.Session;
import com.facebook.presto.connector.proteum.ProteumPlugin;
import com.facebook.presto.tests.DistributedQueryRunner;

/**
 * 
 * @author Dilip Kasana
 * @Date 27 Mar 2015
 */
public final class TestProteumQueryRunner {
	public TestProteumQueryRunner() {
	}

	public static final Session PROTEUM_SESSION = Session.builder()
			.setUser("user").setSource("source").setCatalog("proteum")
			.setSchema("default").setTimeZoneKey(UTC_KEY).setLocale(ENGLISH)
			.setRemoteUserAddress("address").setUserAgent("agent").build();
	private static DistributedQueryRunner defaultQueryRunner;
	private static DistributedQueryRunner noFilterQueryRunner;

	public static DistributedQueryRunner createDefaultQueryRunner()
			throws Exception {
		if (defaultQueryRunner != null) {
			return defaultQueryRunner;
		}
		try {
			defaultQueryRunner = new DistributedQueryRunner(createSession(), 1);
			Map<String, String> properties = new HashMap<String, String>();
			properties.put("proteum.host", "localhost");
			properties.put("proteum.port", "8359");
			properties.put("proteum-plugin.port", "8365");
			properties.put("proteum-plugin.start.port", "13001");
			defaultQueryRunner.installPlugin(new ProteumPlugin());
			defaultQueryRunner.createCatalog("proteum", "proteum", properties);
			return defaultQueryRunner;
		} catch (Throwable e) {
			closeAllSuppress(e, defaultQueryRunner);
			throw e;
		}
	}

	public static Session createSession() {
		return PROTEUM_SESSION;
	}

	public static DistributedQueryRunner createNoFilterQueryRunner()
			throws Exception {
		if (noFilterQueryRunner != null) {
			return noFilterQueryRunner;
		}
		try {
			noFilterQueryRunner = new DistributedQueryRunner(createSession(), 1);
			Map<String, String> properties = new HashMap<String, String>();
			properties.put("proteum.host", "localhost");
			properties.put("proteum.port", "8359");
			properties.put("proteum-plugin.port", "8366");
			properties.put("proteum.apply.filter", "false");
			properties.put("proteum-plugin.start.port", "12001");
			noFilterQueryRunner.installPlugin(new ProteumPlugin());
			noFilterQueryRunner.createCatalog("proteum", "proteum", properties);
			return noFilterQueryRunner;
		} catch (Throwable e) {
			closeAllSuppress(e, noFilterQueryRunner);
			throw e;
		}
	}
}
