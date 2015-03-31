package com.mobileum.proteum.tests;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

import com.facebook.presto.Session;

/**
 * 
 * @author Dilip Kasana
 * @Date 27 Mar 2015
 */
public final class SessionTestUtils {
	public static final Session PROTEUM_SESSION = Session.builder()
			.setUser("user").setSource("source").setCatalog("proteum")
			.setSchema("default").setTimeZoneKey(UTC_KEY).setLocale(ENGLISH)
			.setRemoteUserAddress("address").setUserAgent("agent").build();
	public static final Session PROTEUM_TEST_SESSION = Session.builder()
			.setUser("user").setSource("source").setCatalog("proteum-test")
			.setSchema("default").setTimeZoneKey(UTC_KEY).setLocale(ENGLISH)
			.setRemoteUserAddress("address").setUserAgent("agent").build();

	private SessionTestUtils() {
	}
}
