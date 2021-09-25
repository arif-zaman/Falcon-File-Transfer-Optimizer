package client.model.online;

import client.FileCluster;
import client.model.Model;
import client.utils.SocketIPC;
import client.utils.TunableParameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.Socket;

public class Gradient extends Model {

    public Gradient() {
        super();
    }

    private static final Log LOG = LogFactory.getLog(Gradient.class);
    public TunableParameters runModel (FileCluster chunk, TunableParameters tunableParameters, double throughput,
                                       double[] relaxation_rates) {
        int nextCC = -1;
        int nextPP = 1;
        int nextPL = 1;
        try {
            String fullMessage = Double.toString(throughput/(1024*1024));
            out.println(fullMessage);
            String recv_msg = in.readLine();
            String[] params = recv_msg.split(",");

            if (params.length == 1) {
                nextCC = Integer.parseInt(params[0]);
            }

            if (params.length == 3) {
                nextCC = Integer.parseInt(params[0]);
                nextPP = Integer.parseInt(params[1]);
                nextPL = Integer.parseInt(params[2]);
            }

            System.out.println("Received: " + recv_msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new TunableParameters.Builder().setConcurrency(nextCC).setParallelism(nextPP).setPipelining(nextPL).build();
    }

    public TunableParameters requestNextParameters() {
        int nextCC = -1;
        int nextPP = 1;
        int nextPL = 1;
        try {
            String recv_msg = in.readLine();
            String[] params = recv_msg.split(",");

            if (params.length == 1) {
                nextCC = Integer.parseInt(params[0]);
            }

            if (params.length == 3) {
                nextCC = Integer.parseInt(params[0]);
                nextPP = Integer.parseInt(params[1]);
                nextPL = Integer.parseInt(params[2]);
            }

            System.out.println("Received: " + recv_msg);
	    return new TunableParameters.Builder().setConcurrency(nextCC).setParallelism(nextPP).setPipelining(nextPL).build();
        }
        catch (IOException e) {
            e.printStackTrace();
            return  null;
        }

    }
}

