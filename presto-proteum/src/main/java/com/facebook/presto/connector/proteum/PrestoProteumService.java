package com.facebook.presto.connector.proteum;

import org.eclipse.jetty.server.Server;

public class PrestoProteumService {
    
    public static void start(ProteumClient client){
        Server server = new Server(8360);
        server.setHandler(new PrestoProteumServiceHandler(client));
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
