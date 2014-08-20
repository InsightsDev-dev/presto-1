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

import io.airlift.log.Logger;

import org.eclipse.jetty.server.Server;

public class PrestoProteumService {
	private static Logger log = Logger.get(PrestoProteumService.class);	
    public static void start(ProteumClient client){
        Server server = new Server(8360);
        server.setHandler(new PrestoProteumServiceHandler(client));
        try {
        	log.info("Starting PrestoProteumServer");
            server.start();
            log.info("Started PrestoProteumServer");
            server.join();
        } catch (Exception e) {
            // TODO Auto-generated catch block
        	log.error(e);
        }

    }
}
