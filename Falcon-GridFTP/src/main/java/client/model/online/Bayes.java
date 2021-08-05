package client.model.online;

import client.FileCluster;
import client.model.Model;
import client.utils.SocketIPC;
import client.utils.TunableParameters;

public class Bayes extends Model {

    public TunableParameters runModel (FileCluster chunk, TunableParameters tunableParameters, double throughput,
                                       double[] relaxation_rates) {
        SocketIPC socketIPC = new SocketIPC(32000);
        String fullMessage  = Double.toString(throughput);
        socketIPC.send(fullMessage);
        try {
            tunableParameters.setConcurrency(Integer.parseInt(socketIPC.recv()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tunableParameters;
    }
    public TunableParameters requestNextParameters() {
        return new TunableParameters.Builder().setConcurrency(1).build();
    }

}
