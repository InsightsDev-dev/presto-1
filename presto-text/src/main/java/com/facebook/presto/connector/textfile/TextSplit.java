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
package com.facebook.presto.connector.textfile;

import java.io.InputStream;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TextSplit implements ConnectorSplit{
    private String stream;
    
    @JsonCreator
    public TextSplit(@JsonProperty("stream")String stream) {
        super();
        this.stream = stream;
    }
    @JsonProperty
    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    @Override
    public boolean isRemotelyAccessible() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<HostAddress> getAddresses() {
        // TODO Auto-generated method stub
        URI uli;
        try {
            String address = Inet4Address.getLocalHost().getHostAddress();
            uli = new URI(null, address, null, null);
            HostAddress add = HostAddress.fromUri(uli);
            List<HostAddress> result = new ArrayList<HostAddress>();
            result.add(add);
            return result;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Object getInfo() {
        // TODO Auto-generated method stub
        return this;
    }

}
