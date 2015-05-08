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
	private boolean applyFilter = true;
	private boolean applyGroupBy = false;
	private String proteumConnectionString;
	private String zooKeeperConnectionString;
	private String proteumHost;
	private Boolean useZooKeeper = false;
	private int startPort = 10001;

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
	public boolean getApplyFilter() {
		return applyFilter;
	}

	@Config("proteum.apply.filter")
	public void setApplyFilter(boolean applyFilter) {
		this.applyFilter = applyFilter;
	}

	@NotNull
	public boolean getApplyGroupBy() {
		return applyGroupBy;
	}

	@Config("proteum.apply.groupby")
	public void setApplyGroupBy(boolean applyGroupBy) {
		this.applyGroupBy = applyGroupBy;
	}

	@Config("proteum.host")
	public void setProteumHost(String baseURL) {
		this.proteumHost = baseURL;
		if (proteumHost == null || proteumHost.isEmpty()) {
			throw new RuntimeException(
					"No URL for proteum.host in proteum.properties");
		}
	}

	public String getProteumHost() {
		return proteumHost;
	}

	public void setProteumConnectionString(String proteumConnectionString) {
		this.proteumConnectionString = proteumConnectionString;
		if (proteumConnectionString == null
				|| proteumConnectionString.isEmpty()) {
			throw new RuntimeException(
					"No URL for proteum.connection.string in proteum.properties");
		}
	}

	public String getProteumConnectionString() {
		return proteumConnectionString;
	}

	@Config("use.zookeeper")
	public void setUseZooKeeper(Boolean useZooKeepr) {
		if (useZooKeepr == null) {
			return;
		}
		this.useZooKeeper = useZooKeepr;
	}

	public boolean getUseZooKeeper() {
		return useZooKeeper;
	}

	@Config("zookeeper.connection.string")
	public void setZooKeeperConnectionString(String zooKeeperConnectionString) {
		this.zooKeeperConnectionString = zooKeeperConnectionString;
		if (zooKeeperConnectionString == null
				|| zooKeeperConnectionString.isEmpty()) {
			System.err
					.println("zookeeper.connection.string is not correctly specified in proteum.properties");
		}
	}

	public String getZooKeeperConnectionString() {
		return this.zooKeeperConnectionString;
	}

	public String getProteumUrl() {
		// TODO Auto-generated method stub
		if (useZooKeeper)
			return "http://" + this.proteumConnectionString;
		else {
			return "http://" + this.proteumHost + ":" + this.proteumServerPort;
		}
	}

	@Max(65536)
	@Min(0)
	public int getStartPort() {
		return startPort;
	}

	@Config("proteum-plugin.start.port")
	public void setStartPort(int startPort) {
		this.startPort = startPort;
	}

}
