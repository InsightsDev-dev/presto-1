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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.Lists;
public class ProteumClient {
    private String baseURL;
    public ProteumClient(String host, String port){
        List<String> schemas = new ArrayList<String>();
        tables = new HashMap<String, Map<String,ProteumTable>>();
        String baseURL = "http://"+host+":"+port;
        this.baseURL = baseURL;
        try{
            URL url = new URL(baseURL+"/list");
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                addTable(inputLine);
            }
        }
        catch(Exception e){}
       
//        try{
//            URL url = new URL(baseURL+"/schemas");
//            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
//            connection.setRequestMethod("GET");
//            connection.connect();
//            BufferedReader in = new BufferedReader(new InputStreamReader(
//                    connection.getInputStream()));
//            String inputLine;
//            while ((inputLine = in.readLine()) != null) {
//                tables.put(inputLine, null);
//                schemas.add(inputLine);
//            }
//                        
//            in.close();
//            for(String schema : schemas){
//                url = new URL(baseURL+"/tables/"+schema);
//                connection = (HttpURLConnection)url.openConnection();
//                connection.setRequestMethod("GET");
//                connection.connect();
//                in = new BufferedReader(new InputStreamReader(
//                        connection.getInputStream()));
//                while ((inputLine = in.readLine()) != null) {
//                    String tableName = inputLine;
//                    Map<String, ProteumTable> tempMap = tables.get(schema);
//                    if(tempMap == null){
//                        tempMap = new HashMap<String, ProteumTable>();
//                        tables.put(schema, tempMap);
//                    }
//                    tempMap.put(tableName, null);
//                }
//            }
//            
//            for(String schema : schemas){
//                List<String> tempTables = Lists.newArrayList(tables.get(schema).keySet());
//                for(String tableName : tempTables){
//                    List<ProteumColumn> columns = new ArrayList<ProteumColumn>();
//                    url = new URL(baseURL+"/describe/"+schema+"/"+tableName);
//                    connection = (HttpURLConnection)url.openConnection();
//                    connection.setRequestMethod("GET");
//                    connection.connect();
//                    in = new BufferedReader(new InputStreamReader(
//                            connection.getInputStream()));
//                    while ((inputLine = in.readLine()) != null) {
//                        String[]nameType = inputLine.split(":");
//                        Type type=getTypeFromString(nameType[1]);
//                        columns.add(new ProteumColumn(nameType[0], type));
//                    }
//                    url = new URL(baseURL+"/splits/"+schema+"/"+tableName);
//                    connection = (HttpURLConnection)url.openConnection();
//                    connection.setRequestMethod("GET");
//                    connection.connect();
//                    in = new BufferedReader(new InputStreamReader(
//                            connection.getInputStream()));
//                    String[] splits = in.readLine().split("\\|");
//                    List<URL> urls = new ArrayList<URL>();
//                    for(String split : splits){
//                        urls.add(new URL(baseURL+"/print/"+schema+"/"+tableName+"/"+split));
//                    }
//                    ProteumTable pTable = new ProteumTable(tableName, columns, urls, schema, baseURL,true);
//                    tables.get(schema).put(tableName, pTable);
//                }
//            }
//            
//        }
//        catch(Exception e){
//            
//        }
        
    }
    public String getBaseURL(){
        return this.baseURL;
    }
    private Map<String, Map<String, ProteumTable>> tables;
    public Set<ProteumTable> getTables(){
        Set<ProteumTable> result = new HashSet<ProteumTable>();
        for(Entry<String, Map<String,ProteumTable>> entry : tables.entrySet()){
            result.addAll(entry.getValue().values());
        }
        return result;
    }
    public Set<String> getSchemas(){
        return tables.keySet();
    }
    
    public ProteumTable getTable(String schemaName, String tableName){
        Map<String, ProteumTable> schemaTables = tables.get(schemaName);
        if(schemaTables == null) return null;
        return schemaTables.get(tableName);
    }
    
    public Set<String> getTableNames(String schemaName){
        Set<String> visibleTables = new HashSet<String>();
        Map<String, ProteumTable> schemaTables = tables.get(schemaName);
        if(schemaTables == null) return null;
        for(Entry<String, ProteumTable> entry : schemaTables.entrySet()){
            if(entry.getValue().isVisible()) visibleTables.add(entry.getKey());
        }
        return visibleTables;
    }
    
    private Type getTypeFromString(String type){
        if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("long")) return BigintType.BIGINT;
        if(type.equalsIgnoreCase("double"))return DoubleType.DOUBLE;
        else return VarcharType.VARCHAR;
    }
    
    public void addTable(String schema) throws MalformedURLException{
        String[] toks = schema.split("\\$");
        String database = toks[0];
        String tableName = toks[1];
        boolean visible = Boolean.parseBoolean(toks[2]);
        String[] splits = toks[3].split("\\|");
        String[] tableSchema = toks[4].split("\\|");
        
        List<ProteumColumn> columns = new ArrayList<ProteumColumn>();
        for(int i = 0 ; i < tableSchema.length ; i++){
            String[]nameType = tableSchema[i].split(":");
            Type type=getTypeFromString(nameType[1]);
            columns.add(new ProteumColumn(nameType[0], type));
        }
        
        List<URL> urls = new ArrayList<URL>();
        for(String split : splits){
            urls.add(new URL(baseURL+"/print/"+database+"/"+tableName+"/"+split));
        }
        ProteumTable pTable = new ProteumTable(tableName, columns, urls, database, baseURL,visible);
        if(tables.get(database)==null){
            tables.put(database, new HashMap<String,ProteumTable>());
        }
        tables.get(database).put(tableName, pTable);
    }

}
