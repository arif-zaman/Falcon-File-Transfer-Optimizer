package client.model;

import client.FileCluster;
import client.utils.TunableParameters;

import java.io.*;
import java.net.Socket;

public abstract class Model {
    protected Socket socket;
    protected PrintWriter out = null;
    protected BufferedReader in = null;
    long lastUpdateTime;

    public Model() {
        try {
            socket = new Socket("localhost", 32000);
            out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public abstract TunableParameters runModel(FileCluster chunk, TunableParameters tunableParameters,
                                               double sampleThroughput, double[] relaxation_rates);
    public  abstract TunableParameters requestNextParameters();

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
}
