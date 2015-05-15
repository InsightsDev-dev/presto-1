package com.facebook.presto.connector.proteum;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import javax.management.RuntimeErrorException;

public class ProteumScanThread extends Thread{
    private int listenPort;
    private List<UnsafeMemory> data = null;
    private int size = -1;
    private boolean isFinished;
    private ServerSocket serverSocket;
    List<SocketScanThread> threads;
    private boolean socketAcceptStarted;
    private boolean isThreadStarted = false;
    public ProteumScanThread(int listenPort){
        this.listenPort = listenPort;
        isFinished = false;
        threads = new ArrayList<SocketScanThread>();
        socketAcceptStarted = false;
    }
    public boolean isSocketAccepting(){
        return socketAcceptStarted;
    }
    public void setFinished(boolean isFinished){
        this.isFinished = isFinished;
    }
    
    public void closeServerSocket(){
        try{
            serverSocket.close();
            ProteumClient.addFreePort(listenPort);
            ProteumClient.triggerPortMaintainance();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    private void waitForSocketThreadsToTerminate(){
        System.out.println("size of scan thread is "+threads.size());
        for(SocketScanThread thread : threads){
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        data = new ArrayList<UnsafeMemory>();
        size = 0;
        for(SocketScanThread thread : threads){
            UnsafeMemory memory = new UnsafeMemory(thread.getData());
            data.add(memory);
            size+=thread.getCount();
        }
        closeServerSocket();
    }
    public List<UnsafeMemory> getData(){
        if(data == null){
            waitForSocketThreadsToTerminate();
        }
        return data;
    }
    
    public int getSize(){
        if(size == -1){
            waitForSocketThreadsToTerminate();
        }
        return size;
    }
    
    @Override
    public void run() {
        isThreadStarted = true;
        try {
            serverSocket = ProteumClient.getServerSocket(listenPort);
            //serverSocket.setSoTimeout(500);
            while(!isFinished){
                try{
                    socketAcceptStarted = true;
                    Socket socket = serverSocket.accept();
                    SocketScanThread thread = new SocketScanThread(socket);
                    threads.add(thread);
                    thread.setPriority(10);
                    thread.start();
                    BufferedWriter out =
                            new BufferedWriter(new PrintWriter(socket.getOutputStream()));
                    out.write("start");
                    out.newLine();
                    out.flush();
                    socket.getOutputStream().flush();
                }
                catch(Exception e){}
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}

class SocketScanThread extends Thread{
    private byte[] data;
    private int count;
    private Socket socket;
    public SocketScanThread(Socket socket){
        this.socket = socket;
        count = 0;
    }
    
    @Override
    public void run() {
        try{
            
            InputStream in = socket.getInputStream();
            int ch1 = in.read();
            int ch2 = in.read();
            int ch3 = in.read();
            int ch4 = in.read();
            
            count =  ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
            data = new byte[count];
            int offset = 0;
            while(offset < count){
                offset+= in.read(data, offset, count-offset);
            }
            socket.close();
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }
    
    public byte[] getData(){
        return this.data;
    }
    
    public int getCount(){
        return this.count;
    }
}
