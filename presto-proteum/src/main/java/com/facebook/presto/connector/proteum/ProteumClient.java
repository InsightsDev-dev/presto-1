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
package com.facebook.presto.connector.proteum;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.mobileum.range.presto.TSRangeType;

public class ProteumClient implements Watcher, Runnable, DataMonitor.DataMonitorListener {
	private String baseURL;
	private ProteumConfig config;
	
	DataMonitor dm;

	ZooKeeper zk;

	private static final String znode = "/proteum_driver_hostname";
	@Inject
	public ProteumClient(ProteumConfig config) {
		this.config = config;
		if(this.config.getUseZooKeeper()){
			try {
				zk = new ZooKeeper(this.config.getZooKeeperConnectionString(), 3000, this, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
	        dm = new DataMonitor(zk, znode, null, this);
		}else{
			initializeClient();
		}
	}
	
	public ProteumConfig getConfig() {
		return config;
	}
	public void initializeClient(){
	    tables = new HashMap<String, Map<String, ProteumTable>>();
        String baseURL = config.getProteumUrl();
        this.baseURL = baseURL;
        System.out.println("initlaizing client using ="+this.baseURL);
        try {
            URL url = new URL(baseURL + "/list");
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(4 * 1000);
            connection.connect();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
            	System.out.println(inputLine);
                addTable(inputLine);
            }
        } catch (Exception e) {
            System.out.println("Unable to connect to Proteum at : "+baseURL);
            System.out.println(e.getMessage());
        }
	}
	
	public String getBaseURL() {
		return this.baseURL;
	}

	private Map<String, Map<String, ProteumTable>> tables;

	public Set<ProteumTable> getTables() {
		Set<ProteumTable> result = new HashSet<ProteumTable>();
		for (Entry<String, Map<String, ProteumTable>> entry : tables.entrySet()) {
			result.addAll(entry.getValue().values());
		}
		return result;
	}

	public Set<String> getSchemas() {
		return tables.keySet();
	}

	public ProteumTable getTable(String schemaName, String tableName) {
		Map<String, ProteumTable> schemaTables = tables.get(schemaName);
		if (schemaTables == null)
			return null;
		return schemaTables.get(tableName);
	}

	public Set<String> getTableNames(String schemaName) {
		Set<String> visibleTables = new HashSet<String>();
		Map<String, ProteumTable> schemaTables = tables.get(schemaName);
		if (schemaTables == null)
			return null;
		for (Entry<String, ProteumTable> entry : schemaTables.entrySet()) {
			if (entry.getValue().isVisible())
				visibleTables.add(entry.getKey());
		}
		return visibleTables;
	}

	private Type getTypeFromString(String type) {
		if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("long"))
			return BigintType.BIGINT;
		if (type.equalsIgnoreCase("double"))
			return DoubleType.DOUBLE;
		if (type.equalsIgnoreCase("tsrange")) {
			return TSRangeType.TS_RANGE_TYPE;
		} else
			return VarcharType.VARCHAR;
	}

	public void addTable(String schema) throws MalformedURLException {
		String[] toks = schema.split("\\$");
		String database = toks[0];
		String tableName = toks[1];
		boolean visible = Boolean.parseBoolean(toks[2]);
		String[] splits = toks[3].split("\\|");
		String[] tableSchema = toks[4].split("\\|");

		List<ProteumColumn> columns = new ArrayList<ProteumColumn>();
		for (int i = 0; i < tableSchema.length; i++) {
			String[] nameType = tableSchema[i].split(":");
			Type type = getTypeFromString(nameType[1]);
			columns.add(new ProteumColumn(nameType[0], type));
		}

		List<URL> urls = new ArrayList<URL>();
		for (String split : splits) {
			urls.add(new URL(baseURL + "/print/" + database + "/" + tableName
					+ "/" + split));
		}
		ProteumTable pTable = new ProteumTable(tableName, columns, urls,
				database, baseURL, visible);
		if (tables.get(database) == null) {
			tables.put(database, new HashMap<String, ProteumTable>());
		}
		tables.get(database).put(tableName, pTable);
	}

	  @Override
		public void process(WatchedEvent event) {
	    	dm.process(event);
	    }

	    @Override
		public void run() {
	        try {
	            synchronized (this) {
	                while (!dm.dead) {
	                    wait();
	                }
	            }
	        } catch (InterruptedException e) {
	        }
	    }

	    @Override
		public void closing(int rc) {
	        synchronized (this) {
	            notifyAll();
	        }
	    }

	    @Override
		public void exists(byte[] data) {
	        if (data == null) {
	            System.out.println("data is null");
	        } else {
	        	String connectionString = new String(data);
	        	System.out.println("connectionString " + connectionString);
	        	this.config.setProteumConnectionString(new String(data));
	        	initializeClient();
	        }
	    }
}
