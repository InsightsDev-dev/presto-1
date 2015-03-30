package com.facebook.presto.connector.proteum;

import io.airlift.configuration.Config;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.base.Splitter;

/**
 * 
 * @author Dilip Kasana
 * @Date 27 Mar 2015
 */
public class ProteumConfig {
	private static final int SECOND = 1000;
	private int pluginListerPort = 8360;
	private int proteumServerPort = 8359;
	private boolean notApplyFilter = false;
	private String baseURL = null;
	private List<String> proteumServerURIs;
	private boolean isBaseURLIntializationRequired = true;
	private static final Splitter SPLITTER = Splitter.on(',').trimResults()
			.omitEmptyStrings();

	@Max(65536)
	@Min(0)
	public int getProteumServerPort() {
		return proteumServerPort;
	}

	@Config("proteum.port")
	public void setProteumServerPort(int proteumServerPort) {
		this.proteumServerPort = proteumServerPort;
	}

	@Max(65536)
	@Min(0)
	public Integer getPluginListerPort() {
		return pluginListerPort;
	}

	@Config("proteum-plugin.port")
	public void setPluginListerPort(Integer pluginListerPort) {
		this.pluginListerPort = pluginListerPort;
	}

	@NotNull
	public boolean getNotApplyFilter() {
		return notApplyFilter;
	}

	@Config("proteum.not.apply.filter")
	public void setNotApplyFilter(Boolean notApplyFilter) {
		this.notApplyFilter = notApplyFilter;
	}

	public List<String> getBaseURL() {
		return proteumServerURIs;
	}

	public String intializeAndGetProteumServerURL() {
		if (baseURL == null) {
			baseURL = initilizeURL();
		}
		if (baseURL == null) {
			throw new RuntimeException("proteum host not defined");
		}
		return baseURL;
	}

	@Config("proteum.host")
	public void setBaseURL(String baseURL) {
		proteumServerURIs = SPLITTER.splitToList(baseURL);
		if (proteumServerURIs == null || proteumServerURIs.isEmpty()) {
			throw new RuntimeException(
					"No URL for proteum.host in proteum.properties");
		}
	}

	private String initilizeURL() {
		if (isBaseURLIntializationRequired) {
			isBaseURLIntializationRequired = false;
			if (proteumServerURIs.size() == 1) {
				return "http://" + proteumServerURIs.get(0) + ":" + proteumServerPort;
			}
			for (String host : proteumServerURIs) {
				String tempURL = "http://" + host + ":" + proteumServerPort;
				HttpURLConnection connection;
				try {
					URL myurl = new URL(tempURL);
					connection = (HttpURLConnection) myurl.openConnection();
					connection.setConnectTimeout(4 * SECOND);
					connection.setReadTimeout(SECOND);
					connection.setRequestMethod("HEAD");
					int code = connection.getResponseCode();
					if (code == HttpURLConnection.HTTP_OK) {
						System.out.println("Proteum Server URL is " + tempURL);
						return tempURL;
					}
				} catch (UnknownHostException e) {
					throw new RuntimeException(e);
				} catch (MalformedURLException e) {
					throw new RuntimeException(e);
				} catch (Exception e) {
					System.out.println(e.getMessage() + " for " + tempURL);
				}
			}
		}
		return null;
	}
}
