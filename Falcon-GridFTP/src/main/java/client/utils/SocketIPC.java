package client.utils;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SocketIPC {

    public PrintWriter out;
    public BufferedReader in;
    Socket socket = null;
    ServerSocket serverSocket = null;
    ConnectionListener connlisten = null;
    DataListener datalisten = null;
    Thread connlisten_thread = null;
    Thread datalisten_thread = null;
    CommandObject ipc_event_cmd = null;

    // Server thread accepts incoming client connections
    class ConnectionListener extends Thread {

        private int port;

        ConnectionListener(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try {
                serverSocket = new ServerSocket(port);
                socket = serverSocket.accept();
                out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                //datalisten = new DataListener();
               // datalisten_thread = new Thread(datalisten);
               // datalisten_thread.start();
            } catch (Exception e) {
                System.err.println("SocketIPC creation error: " + e.getMessage());
            }
        }
    }

    // Server thread accepts incoming client connections
    class DataListener extends Thread {

        String data_str = null;

        DataListener() {
        }

        @Override
        public void run() {
            try {
                while(true) {
                    data_str = recv();
                    ipc_event_cmd.buffer.add(data_str);
                    ipc_event_cmd.execute();
                }
            } catch (Exception e) {
                System.err.println("SocketIPC reading error: " + e.getMessage());
            }
        }
        public String read() {
            String ret_string = null;
            if(!ipc_event_cmd.buffer.isEmpty()) {
                ret_string = ipc_event_cmd.buffer.remove(0);
            }
            return ret_string;
        }
    }

    public SocketIPC(int port) {
        ipc_event_cmd = new CommandObject();
        connlisten = new ConnectionListener(port);
        connlisten_thread = new Thread(connlisten);
        connlisten_thread.start();
    }

    public void send(String msg) {
        if (out != null) {
            out.println(msg);
        }
    }

    public void flush() {
        if (out != null) {
            out.flush();
        }
    }

    public void close() {
        if (out != null) {
            out.flush();
            out.close();
            try {
                in.close();
                socket.close();
                serverSocket.close();
            } catch (Exception e) {
                System.err.println("SocketIPC closing error: " + e.getMessage());
            }
        }
    }

    public String recv() throws Exception {
        if (in != null) {
            return in.readLine();
        } else {
            return "";
        }
    }

    public void set_cmd(CommandObject event_cmd) {
        if (event_cmd != null) {
            this.ipc_event_cmd = event_cmd;
        }
    }
    public class CommandObject {

        List<String> buffer;

        public CommandObject() {
            this.buffer = new ArrayList<String>();
        }

        public void execute() {
        }

    }
}