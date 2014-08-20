package com.facebook.presto.connector.proteum;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class PrestoProteumServiceHandler extends AbstractHandler {
    private ProteumClient client;
    public PrestoProteumServiceHandler(ProteumClient client){
        this.client = client;
    }
    @Override
    public void handle(String target, Request arg1, HttpServletRequest arg2,
            HttpServletResponse arg3) throws IOException, ServletException {
        if(target.equals("updatesplits")){
            for(ProteumTable table : client.getTables()){
                try {
                    table.updateSources();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        
    }

}
