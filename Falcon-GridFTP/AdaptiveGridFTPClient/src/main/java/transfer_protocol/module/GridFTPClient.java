package transfer_protocol.module;

import client.AdaptiveGridFTPClient;
import client.ConfigurationParams;
import client.FileCluster;
import client.model.Model;
import client.model.hysteresis.Hysteresis;
import client.model.online.Bayes;
import client.model.online.Gradient;
import client.utils.HostResolution;
import client.utils.TunableParameters;
import client.utils.Utils;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.HostPort;
import org.globus.ftp.HostPortList;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import transfer_protocol.util.XferList;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;

import static client.utils.Utils.getChannels;

public class GridFTPClient implements Runnable {
    public static FTPClient ftpClient;
    public static ExecutorService executor;
    public static InetAddress[] sourceIpList, destinationIpList;
    public static int srcIPIndex = 0, destinationIPIndex = 0;
    public static ModellingThread modellingThread = null;



    static int fastChunkId = -1, slowChunkId = -1, period = 0;
    URI usu = null, udu = null;
    static FTPURI su = null, du = null;

    Thread connectionThread, transferMonitorThread;
    HostResolution sourceHostResolutionThread, destinationHostResolutionThread;
    GSSCredential srcCred =null, dstCred =null, cred = null;

    volatile int rv = -1;
    static int perfFreq = 3;
    public boolean useDynamicScheduling = false;
    public boolean useOnlineTuning =  false;

    private static final Log LOG = LogFactory.getLog(GridFTPClient.class);

    public GridFTPClient(String source, String dest) {
        try {
            usu = new URI(source).normalize();
            udu = new URI(dest).normalize();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        executor = Executors.newFixedThreadPool(64);
    }

    public void setPerfFreq (int perfFreq) {
        this.perfFreq = perfFreq;
    }


    public static boolean configureChannel(ChannelModule.ChannelPair channelPair,
                                           int channelId,
                                           FileCluster chunk) {
        TunableParameters params = chunk.getTunableParameters();
        channelPair.chunk = chunk;
        try {
            channelPair.setID(channelId);
            if (params.getParallelism() > 1)
                channelPair.setParallelism(params.getParallelism());
            channelPair.setPipelining(params.getPipelining());
            channelPair.setBufferSize(params.getBufferSize());
            channelPair.setPerfFreq(perfFreq);
            channelPair.setDataChannelAuthentication(DataChannelAuthentication.NONE);
            if (!channelPair.isDataChannelReady()) {
                // Use extended mode to be able to reuse the channelPair for multi-file transfers
                channelPair.setTypeAndMode('I', 'E');
                if (channelPair.isStripingEnabled()) {
                    HostPortList hpl = channelPair.setStripedPassive();
                    channelPair.setStripedActive(hpl);
                } else {
                    HostPort hp = channelPair.setPassive();
                    channelPair.setActive(hp);
                }
            }

        } catch (Exception ex) {
            System.out.println("Failed to setup channelPair");
            ex.printStackTrace();
            return false;
        }
        return true;
    }




    public void process() throws Exception {

        // Check if we were provided a proxy. If so, load it.
        if (usu.getScheme().compareTo("gsiftp") == 0) {
            if (ConfigurationParams.proxyFile != null) {
                cred = readCredential(ConfigurationParams.proxyFile);
                srcCred = dstCred = cred;
            }
            if (ConfigurationParams.srcCred != null) {
                srcCred = readCredential(ConfigurationParams.srcCred);
            }
            if (ConfigurationParams.dstCred != null) {
                dstCred = readCredential(ConfigurationParams.dstCred);
            }
        }

        // Attempt to connect to hosts.
        // TODO: Differentiate between temporary errors and fatal errors.
        try {
            su = new FTPURI(usu, srcCred);
            du = new FTPURI(udu, dstCred);
        } catch (Exception e) {
            fatal("couldn't connect to server: " + e.getMessage());
        }
        // Attempt to connect to hosts.
        // TODO: Differentiate between temporary errors and fatal errors.
        try {
            ftpClient = new FTPClient(su, du);
        } catch (Exception e) {
            e.printStackTrace();
            fatal("error connecting: " + e);
        }
        // Check that src and dest match.
        if (su.path.endsWith("/") && du.path.compareTo("/dev/null") == 0) {  //File to memory transfer

        } else if (su.path.endsWith("/") && !du.path.endsWith("/")) {
            fatal("src is a directory, but dest is not");
        }
        ftpClient.fileClusters = new LinkedList<>();
    }

    GSSCredential readCredential (String credPath) throws Exception {
        GSSCredential gssCredential = null;
        try {
            File cred_file = new File(credPath);
            FileInputStream fis = new FileInputStream(cred_file);
            byte[] cred_bytes = new byte[(int) cred_file.length()];
            fis.read(cred_bytes);
            System.out.println("Setting parameters");
            //GSSManager manager = ExtendedGSSManager.getInstance();
            ExtendedGSSManager gm = (ExtendedGSSManager) ExtendedGSSManager.getInstance();
            gssCredential = gm.createCredential(cred_bytes,
                    ExtendedGSSCredential.IMPEXP_OPAQUE,
                    GSSCredential.DEFAULT_LIFETIME, null,
                    GSSCredential.INITIATE_AND_ACCEPT);
            fis.close();
        } catch (Exception e) {
            fatal("error loading x509 proxy: " + e.getMessage());
        }
        return gssCredential;
    }

    private void abort() {
        if (ftpClient != null) {
            try {
                ftpClient.abort();
            } catch (Exception e) {
            }
        }

        close();
    }

    private void close() {
        try {
            for (ChannelModule.ChannelPair channelPair : ftpClient.channelList) {
                channelPair.close();
            }
        } catch (Exception e) {
        }
    }

    public void run() {
        try {
            process();
            rv = 0;
        } catch (Exception e) {
            LOG.warn("Client could not be establieshed. Exiting...");
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public void fatal(String m) throws Exception {
        rv = 255;
        throw new Exception(m);
    }

    public void error(String m) throws Exception {
        rv = 1;
        throw new Exception(m);
    }

    public void start() {
        connectionThread = new Thread(this);
        connectionThread.start();
        // Check if there are multiple hosts behind given hostname
        sourceHostResolutionThread = new HostResolution(usu.getHost());
        destinationHostResolutionThread = new HostResolution(udu.getHost());
        sourceHostResolutionThread.start();
        destinationHostResolutionThread.start();
    }

    public void stop() {
        abort();
        //sink.close();
        close();
    }

    public int waitFor() {
        if (connectionThread != null) {
            try {
                connectionThread.join();
            } catch (Exception e) {
            }
        }
        // Make sure hostname resolution operations are completed before starting to a transfer
        try {
            sourceHostResolutionThread.join();
            destinationHostResolutionThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        InetAddress[] inetAddresses = sourceHostResolutionThread.getAllIPs();
        sourceIpList = new InetAddress[inetAddresses.length];
        for (int i = 0; i <inetAddresses.length ; i++) {
            sourceIpList [i] = inetAddresses[i];
        }

        inetAddresses = destinationHostResolutionThread.getAllIPs();
        destinationIpList = new InetAddress[inetAddresses.length];
        for (int i = 0; i <inetAddresses.length ; i++) {
            destinationIpList [i] = inetAddresses[i];
        }

        return (rv >= 0) ? rv : 255;
    }

    public XferList getListofFiles() throws Exception {
        return ftpClient.getListofFiles(usu.getPath(), udu.getPath());
    }

    public void runTransfer(final FileCluster fileCluster) {
        XferList fileList = fileCluster.getRecords();
        TunableParameters tunableParameters = fileCluster.getTunableParameters();
        LOG.info("Transferring chunk " + fileCluster.getDensity().name() +
                " params:" + tunableParameters.toString() + " " + tunableParameters.getBufferSize() +
                " file count:" + fileList.count() +
                " size:" + (fileList.size() / (1024.0 * 1024)));

        int concurrency = tunableParameters.getConcurrency();

        fileList.channels = new LinkedList<>();


        List<Future<ChannelModule.ChannelPair>> futures = new LinkedList<>();
        for (int i = 0; i < concurrency; i++) {
            ChannelPairBuilder channelPairBuilder = new ChannelPairBuilder(fileCluster, i);
            futures.add(executor.submit(channelPairBuilder));
        }
        List<ChannelModule.ChannelPair> channelPairList = new LinkedList<>();
        for (Future<ChannelModule.ChannelPair> future : futures) {
            try {
                ChannelModule.ChannelPair channelPair = future.get();
                if (channelPair == null) {
                    System.out.println("ChannelPair is null, exiting...");
                    System.exit(-1);
                }
                channelPairList.add(channelPair);
                synchronized (fileList.channels) {
                    fileList.channels.add(channelPair);
                }
                synchronized (ftpClient.channelList) {
                    ftpClient.channelList.add(channelPair);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        for (ChannelModule.ChannelPair channelPair: channelPairList) {
            executor.execute(new FTPClient.TransferList(fileCluster.getRecords(), channelPair));
        }
    }

/*
    public double runSampleTransfer(FileCluster fileCluster, TunableParameters tunableParameters,
                                    List<ChannelPairBuilder> transferChannelList, SocketIPC socketIPC,
                                    String algorithm) throws Exception {
        XferList fileList = fileCluster.getRecords();
        fileList.channels = new LinkedList<>();
        ftpClient.channelList = new LinkedList<>();
        for (int i = 0; i < tunableParameters.getConcurrency(); i++) {
            ChannelPairBuilder transferChannel = transferChannelList.remove(0);
            ChannelModule.ChannelPair channelPair = transferChannel.channelPair;
            if (channelPair  == null) {
                System.out.println("Channel pair is null " + channelPair.getId());
                transferChannelList.add(transferChannel);
                i--;
                continue;
            }
            synchronized (fileCluster.getRecords().channels) {
                fileList.channels.add(channelPair);
            }
            synchronized (ftpClient.channelList) {
                ftpClient.channelList.add(channelPair);
            }
            FTPClient.TransferList transferList = new FTPClient.TransferList(fileCluster.getRecords(), channelPair);
            executor.submit(transferList);
            //ftpClient.transferList(transferChannelList.get(i).channelPair);
        }


        XferList xl = fileCluster.getRecords();
        double remainingClusterSize = xl.totalTransferredSize;
        LinkedList<String> instantaneousThroughput = new LinkedList<>();
        int counter = 0;
        System.out.println("CC: " +  tunableParameters.getConcurrency() + " starting with "
                + fileCluster.getRecords().channels.size() + " channels " +
        Utils.printSize(xl.totalTransferredSize, true) + "/" +
        Utils.printSize(xl.initialSize, true));
        while (true) {
            Thread.sleep(1000);
            counter++;
            double throughputInMbps = 8 * (xl.totalTransferredSize - remainingClusterSize);
            //System.out.println("Throughput:" + Utils.printSize(throughputInMbps, false) + "\t");
            if (throughputInMbps == 0 && instantaneousThroughput.isEmpty())
                continue;

            if (throughputInMbps > 0) {
                int lastZeros = 0;
                for (int i = instantaneousThroughput.size()-1; i >0 ; i--) {
                    if(Double.parseDouble(instantaneousThroughput.get(i)) == 0)
                        lastZeros++;
                    else
                        break;
                }
                double throughputShare = throughputInMbps/(lastZeros+1);
                for (int i = 0; i < lastZeros; i++) {
                    instantaneousThroughput.removeLast();
                }
                for (int i = 0; i < lastZeros + 1; i++) {
                    instantaneousThroughput.add(Double.toString(throughputShare / (1000.0 * 1000)));
                }
            } else {
                    instantaneousThroughput.add(Double.toString(throughputInMbps / (1000.0 * 1000)));
                }


            instantaneousThroughput.add(Double.toString(throughputInMbps / (1000.0 * 1000)));

            //instantaneousThroughput.add(Double.toString(throughputInMbps / (1000.0 * 1000)));
            //|| algorithm.compareTo("three")
            if (instantaneousThroughput.size() > 1) {
                if (instantaneousThroughput.size() == 2 &
                        (algorithm.compareTo("dnn") == 0)) {
                    continue;
                }
                String message = String.join(" ", instantaneousThroughput);
                String fullMessage  = algorithm + " " + message;
                //System.out.println("Sending python " + fullMessage);
                socketIPC.send(fullMessage);
                double predictedThroughput = Double.parseDouble(socketIPC.recv());
                if (predictedThroughput > 0 ) {
                    //System.out.println("Closing: " + fileCluster.getRecords().channels.size() + "  channels");
                    for (ChannelModule.ChannelPair channelPair : fileCluster.getRecords().channels) {
                        //System.out.println("intransit size:" + channelPair.inTransitFiles.size());
                        if (channelPair == null) {
                            System.out.println("channel pair is null, unexpected");
                        }
                        while (!channelPair.inTransitFiles.isEmpty()) {
                            XferList.MlsxEntry entry = channelPair.inTransitFiles.poll();
                            synchronized (fileCluster.getRecords()) {
                                fileCluster.getRecords().addEntry(entry);
                                //System.out.println("Adding file " + entry.fileName + " off:" + entry.off);
                            }
                        }
                        //System.out.println("Aborting channelPair " + channelPair.getId());
                        channelPair.isAborted = true;
                        channelPair.abort();
                        GridFTPClient.ChannelPairBuilder transferChannel = new GridFTPClient.ChannelPairBuilder(fileCluster,
                                transferChannelList.get(transferChannelList.size() - 1).channelId + 1);
                        transferChannelList.add(transferChannel);
                        executor.submit(transferChannel);
                    }
                    fileCluster.getRecords().channels = new LinkedList<>();
                    ftpClient.channelList = new LinkedList<>();
                    fileList.onAir = 0;
                    System.out.println("CC: "+ tunableParameters.getConcurrency() +
                            " Throughput:" + predictedThroughput + " Mbps " +
                            " Time:" + counter +
                            " curTime:" + (System.currentTimeMillis()-AdaptiveGridFTPClient.start));
                    return predictedThroughput;
                }   else {
                    //System.out.println("Not converged yet: " +  tunableParameters.getConcurrency() + " starting with "
                    //        + fileCluster.getRecords().channels.size() + " channels " +
                    //        xl.totalTransferredSize + "/" +
                    //        xl.initialSize);
                    if (xl.totalTransferredSize >= xl.initialSize) {
                        System.out.println("Exiting...");
                        fileCluster.getRecords().channels = new LinkedList<>();
                        ftpClient.channelList = new LinkedList<>();
                        return Double.parseDouble(instantaneousThroughput.getLast());
                    }

                }
                remainingClusterSize = xl.totalTransferredSize;
            }
        }
    }
*/

    public static double[] runModelling(FileCluster chunk, TunableParameters tunableParameters, double sampleThroughput,
                                        double[] relaxation_rates) {
        double []resultValues = new double[4];
        try {
            double sampleThroughputinMb = sampleThroughput / Math.pow(10, 6);
            ProcessBuilder pb = new ProcessBuilder("python2.7", "src/main/python/optimizer.py",
                    "-f", "chunk_"+chunk.getDensity()+".txt",
                    "-c", "" + tunableParameters.getConcurrency(),
                    "-p", "" + tunableParameters.getParallelism(),
                    "-q", ""+ tunableParameters.getPipelining(),
                    "-t", "" + sampleThroughputinMb,
                    "--basicClientControlChannel-rate" , ""+ relaxation_rates[0],
                    "--p-rate" , ""+ relaxation_rates[1],
                    "--ppq-rate" , ""+ relaxation_rates[2],
                    "--maxcc", "" + AdaptiveGridFTPClient.transferTask.getMaxConcurrency());
            String formatedString = pb.command().toString()
                    .replace(",", "")  //remove the commas
                    .replace("[", "")  //remove the right bracket
                    .replace("]", "")  //remove the left bracket
                    .trim();           //remove trailing spaces from partially initialized arrays
            System.out.println("input:" + formatedString);
            Process p = pb.start();

            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String output, line;
            if ((output = in.readLine()) == null) {
                in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                while(( output = in.readLine()) !=  null){
                    System.out.println("Output:" + output);
                }
            }
            while ((line = in.readLine()) != null){ // Ignore intermediate log messages
                output = line;
            }
            String []values = output.trim().split("\\s+");
            for (int i = 0; i < values.length; i++) {
                resultValues[i] = Double.parseDouble(values[i]);
            }
        } catch(Exception e) {
            System.out.println(e);
        }
        return resultValues;
    }

    public XferList.MlsxEntry synchronizedPop(List<XferList.MlsxEntry> fileList) {
        synchronized (fileList) {
            return fileList.remove(0);
        }
    }

    public void waitForTransferCompletion() {
        // Check if all the files in all chunks are transferred
        for (FileCluster fileCluster: ftpClient.fileClusters)
            try {
                while (fileCluster.getRecords().totalTransferredSize < fileCluster.getRecords().initialSize) {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        //Close all channels before exiting
        for (int i = 0; i < ftpClient.channelList.size(); i++) {
            ftpClient.channelList.get(i).close();
        }
        ftpClient.channelList.clear();
    }

    public static class ChannelPairBuilder implements Callable {
        final int doStriping;
        int channelId;
        FileCluster fileCluster;
        private boolean connectionSuccessful = false;
        public ChannelPairBuilder(FileCluster fileCluster, int channelId) {
            this.channelId = channelId;
            this.doStriping = 0;
            this.fileCluster = fileCluster;
        }

        @Override
        public ChannelModule.ChannelPair call() {
            int trial = 0;
            //System.out.println("Starting to establish new channel " + channelId);
            ChannelModule.ChannelPair channelPair = null;
            while (++trial <= 3) {
                // Distribute channels to available transfer nodes to balance load on them
                InetAddress srcIP = sourceIpList[srcIPIndex % sourceIpList.length];
                InetAddress dstIP = destinationIpList[destinationIPIndex % destinationIpList.length];
                srcIPIndex++; destinationIPIndex++;
                System.out.println("Making connection between " +srcIP.getHostAddress() + " and " +dstIP.getHostAddress());
                URI srcUri = null, dstUri = null;
                try {
                    srcUri = new URI(su.uri.getScheme(), su.uri.getUserInfo(), srcIP.getHostAddress(),
                            su.uri.getPort(), su.uri.getPath(), su.uri.getQuery(), su.uri.getFragment());
                    dstUri = new URI(du.uri.getScheme(), du.uri.getUserInfo(), dstIP.getHostAddress(),
                            du.uri.getPort(), du.uri.getPath(), du.uri.getQuery(), du.uri.getFragment());
                } catch (URISyntaxException e) {
                    LOG.error("Updating URI host failed:", e);
                    System.exit(-1);
                }

                try {
                    FTPURI srcFTPUri = new FTPURI(srcUri, su.cred);
                    FTPURI dstFTPUri = new FTPURI(dstUri, du.cred);
                    channelPair = new ChannelModule.ChannelPair(srcFTPUri, dstFTPUri);
                    connectionSuccessful = configureChannel(channelPair, channelId, fileCluster);
                    if (connectionSuccessful) {
                        System.out.println("Created channel " + channelId + " successfully");
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Channel configuration failed, will try " + trial + " more times");

            }
            return channelPair;
        }
        public boolean isConnectionSuccessful(){
            return isConnectionSuccessful();
        }
    }

    public class TransferMonitor implements Runnable {
        final int interval = 3000;
        int timer = 0;
        Writer writer;
        List <Double> throughputLogs = new LinkedList<>();

        @Override
        public void run() {
            try {
                writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("inst-throughput.txt"), "utf-8"));
                initializeMonitoring();
                Thread.sleep(interval);

                while (!AdaptiveGridFTPClient.isTransferCompleted) {
                    timer += interval / 1000;
                    long currentTime = System.currentTimeMillis();
                    monitorChannels(interval / 1000, writer, timer);
                    Thread.sleep(interval);
                }
                System.out.println("Leaving monitoring...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void initializeMonitoring() {
        for (FileCluster fileCluster : ftpClient.fileClusters) {
            if (fileCluster.isReadyToTransfer) {
                XferList fileList = fileCluster.getRecords();
                LOG.info("Chunk:" + fileCluster.getDensity().name() +
                        " count:" + fileList.count() +
                        " size:" + Utils.printSize(fileList.size(), true) +
                        " parameters: " + fileCluster.getTunableParameters().toString());
                fileList.instantTransferredSize = fileList.totalTransferredSize;
            }
        }
    }

    public void startTransferMonitor() {
        if (transferMonitorThread == null || !transferMonitorThread.isAlive()) {
            transferMonitorThread = new Thread(new TransferMonitor());
            transferMonitorThread.start();
        }
    }

    private double monitorChannels(int interval, Writer writer, int timer) throws IOException {
        DecimalFormat df = new DecimalFormat("###.##");
        double[] estimatedCompletionTimes = new double[ftpClient.fileClusters.size()];
        double aggregateThroughputInMbps = 0;
        for (int i = 0; i < ftpClient.fileClusters.size(); i++) {
            double estimatedCompletionTime = -1;
            FileCluster fileCluster = ftpClient.fileClusters.get(i);
            XferList xl = fileCluster.getRecords();
            double throughputInMbps = 8 * (xl.totalTransferredSize - xl.instantTransferredSize) / (xl.interval + interval);
            aggregateThroughputInMbps += throughputInMbps;
            if (throughputInMbps == 0) {
                if (xl.totalTransferredSize == xl.initialSize) { // This chunk has finished
                    xl.weighted_throughput = 0;
                } else if (xl.weighted_throughput != 0) { // This chunk is running but current file has not been transferred
                    //xl.instant_throughput = 0;
                    estimatedCompletionTime = ((xl.initialSize - xl.totalTransferredSize) / xl.weighted_throughput) - xl.interval;
                    xl.interval += interval;
                    System.out.println("Chunk " + i +
                            "\t threads:" + xl.channels.size() +
                            "\t count:" + xl.count() +
                            "\t total:" + Utils.printSize(xl.size(), true) +
                            "\t interval:" + xl.interval +
                            "\t onAir:" + xl.onAir +
                            "\t time:" + (System.currentTimeMillis()-AdaptiveGridFTPClient.start/1000.0));
                } else { // This chunk is active but has not transferred any data yet
                    System.out.println("Chunk " + i +
                            "\t threads:" + xl.channels.size() +
                            "\t count:" + xl.count() +
                            "\t total:" + Utils.printSize(xl.size(), true)
                            + "\t onAir:" + xl.onAir);
                    if (xl.channels.size() == 0) {
                        estimatedCompletionTime = Double.POSITIVE_INFINITY;
                    } else {
                        xl.interval += interval;
                    }
                }
            } else {
                xl.instant_throughput = throughputInMbps;
                xl.interval = 0;
                if (xl.weighted_throughput == 0) {
                    xl.weighted_throughput = throughputInMbps;
                } else {
                    xl.weighted_throughput = xl.weighted_throughput * 0.6 + xl.instant_throughput * 0.4;
                }
                estimatedCompletionTime = 8 * (xl.initialSize - xl.totalTransferredSize) / xl.weighted_throughput;
                xl.estimatedFinishTime = estimatedCompletionTime;
                System.out.println("Chunk " + i +
                        "\t threads:" + xl.channels.size() +
                        "\t count:" + xl.count() +
                        "\t time:" + ((System.currentTimeMillis()-AdaptiveGridFTPClient.start)/1000.0) +
                        "\t transferred:" + Utils.printSize(xl.totalTransferredSize, true) +
                        "/" + Utils.printSize(xl.initialSize, true) +
                        "\t throughput:" +  Utils.printSize(xl.instant_throughput, false) +
                        "/" + Utils.printSize(xl.weighted_throughput, true) +
                        "\testimated time:" + df.format(estimatedCompletionTime) +
                        "\t onAir:" + xl.onAir);
                xl.instantTransferredSize = xl.totalTransferredSize;
            }
            estimatedCompletionTimes[i] = estimatedCompletionTime;
            writer.write(timer + "\t" + xl.channels.size() + "\t" + (throughputInMbps)/(1000*1000.0) + "\n");
            writer.flush();
        }
        System.out.println("*******************");
        if (ftpClient.fileClusters.size() > 1 && useDynamicScheduling) {
            checkIfChannelReallocationRequired(estimatedCompletionTimes);
        }
        return aggregateThroughputInMbps;
    }

    // This function implements dynamic scheduling. Dynamic scheduling is used to re-assign channels from fast
    // fileClusters to slow fileClusters to make all run as similar pace
    public void checkIfChannelReallocationRequired(double[] estimatedCompletionTimes) {

        // if any channelPair reallocation is ongoing, then don't go for another!
        for (ChannelModule.ChannelPair cp : ftpClient.channelList) {
            if (cp.isConfigurationChanged) {
                return;
            }
        }
        List<Integer> blacklist = Lists.newArrayListWithCapacity(ftpClient.fileClusters.size());
        int curSlowChunkId, curFastChunkId;
        while (true) {
            double maxDuration = Double.NEGATIVE_INFINITY;
            double minDuration = Double.POSITIVE_INFINITY;
            curSlowChunkId = -1;
            curFastChunkId = -1;
            for (int i = 0; i < estimatedCompletionTimes.length; i++) {
                XferList fileList = ftpClient.fileClusters.get(i).getRecords();
                if (estimatedCompletionTimes[i] == -1 || blacklist.contains(i)) {
                    continue;
                }
                if (estimatedCompletionTimes[i] > maxDuration && fileList.count() > 0) {
                    maxDuration = estimatedCompletionTimes[i];
                    curSlowChunkId = i;
                }
                if (estimatedCompletionTimes[i] < minDuration && fileList.channels.size() > 1) {
                    minDuration = estimatedCompletionTimes[i];
                    curFastChunkId = i;
                }
            }
            System.out.println("CurrentSlow:" + curSlowChunkId + " CurrentFast:" + curFastChunkId +
                    " PrevSlow:" + slowChunkId + " PrevFast:" + fastChunkId + " Period:" + (period + 1));
            if (curSlowChunkId == -1 || curFastChunkId == -1 || curSlowChunkId == curFastChunkId) {
                for (int i = 0; i < estimatedCompletionTimes.length; i++) {
                    System.out.println("Estimated time of :" + i + " " + estimatedCompletionTimes[i]);
                }
                break;
            }
            XferList slowChunk = ftpClient.fileClusters.get(curSlowChunkId).getRecords();
            XferList fastChunk = ftpClient.fileClusters.get(curFastChunkId).getRecords();
            double slowChunkFinTime = Double.MAX_VALUE, fastChunkFinTime;
            period++;
            if (slowChunk.channels.size() > 0) {
                slowChunkFinTime = slowChunk.estimatedFinishTime * slowChunk.channels.size() / (slowChunk.channels.size() + 1);
            }
            fastChunkFinTime = fastChunk.estimatedFinishTime * fastChunk.channels.size() / (fastChunk.channels.size() - 1);
            if (period >= 3 && (curSlowChunkId == slowChunkId || curFastChunkId == fastChunkId)) {
                if (slowChunkFinTime >= fastChunkFinTime * 2) {
                    //System.out.println("total fileClusters  " + ftpClient.ccs.size());
                    synchronized (fastChunk) {
                        ChannelModule.ChannelPair transferringChannel = fastChunk.channels.get(fastChunk.channels.size() - 1);
                        transferringChannel.newChunk = ftpClient.fileClusters.get(curSlowChunkId);
                        transferringChannel.isConfigurationChanged = true;
                        System.out.println("Chunk " + curFastChunkId + "*" + getChannels(fastChunk) +  " is giving channelPair " +
                                transferringChannel.getId() + " to chunk " + curSlowChunkId + "*" + getChannels(slowChunk));
                    }
                    period = 0;
                    break;
                } else {
                    if (slowChunk.channels.size() > fastChunk.channels.size()) {
                        blacklist.add(curFastChunkId);
                    } else {
                        blacklist.add(curSlowChunkId);
                    }
                    System.out.println("Blacklisted chunk " + blacklist.get(blacklist.size() - 1));
                }
            } else if (curSlowChunkId != slowChunkId && curFastChunkId != fastChunkId) {
                period = 1;
                break;
            } else if (period < 3) {
                break;
            }
        }
        fastChunkId = curFastChunkId;
        slowChunkId = curSlowChunkId;

    }


    public static class ModellingThread implements Runnable {
        //public static Queue<ModellingThread.ModellingJob> jobQueue;
        private final int pastLimit = 4;
        private Model model;
        FileCluster fileCluster;
        private final int sampleTimeSeconds = 10;

        public ModellingThread(FileCluster fileCluster, String modelType) {
            //jobQueue = new ConcurrentLinkedQueue<>();
            switch (modelType) {
                case "hysteresis":
                    model = new Hysteresis();
                    //model.findOptimalParameters(chunks, transferTask);
                    break;
                case "gradient":
                    model = new Gradient();
                    break;
                case "bayes":
                    model = new Bayes();
                    break;
            }
            this.fileCluster = fileCluster;
        }

        public Model getModel() {
            return model;
        }

        @Override
        public void run() {
            XferList xl = fileCluster.getRecords();
            // If chunk is almost finished, don't update parameters as no gain will be achieved
            long lastBytesTransferred = 0;
            long lastUpdateTime = System.currentTimeMillis();
            System.out.println("Starting modeling thread");
            while (xl.totalTransferredSize < 0.9 * xl.initialSize) {
                try {
                    Thread.sleep(sampleTimeSeconds*1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                for (ChannelModule.ChannelPair channelPair: xl.channels) {
                    double channelThroughput = 8* (channelPair.dataTransferred- channelPair.lastDataTransferred) /
                            ((System.currentTimeMillis() - lastUpdateTime)/1000.0);
                    if (channelThroughput == 0) {
                        System.out.println("Channel " + channelPair.getId() + " throughput:" + channelThroughput +
                                " between " + channelPair.sc.ftpControlChannel.getHost() + " and " + channelPair.dc.ftpControlChannel.getHost());
                    }
                    channelPair.lastDataTransferred = channelPair.dataTransferred;
                }

                double throughput = 8* (xl.totalTransferredSize - lastBytesTransferred) /
                        ((System.currentTimeMillis() - lastUpdateTime)/1000.0);
                System.out.println("Channels:" + xl.channels.size() +
                        "\t throughput:" +  Utils.printSize(throughput, false) +
                        "\t time:" + ((System.currentTimeMillis()-AdaptiveGridFTPClient.start)/1000.0) +
                        "\t transferred:" + Utils.printSize(xl.totalTransferredSize, true) +
                        "/" + Utils.printSize(xl.initialSize, true) +
                        "\t remFiles:" + xl.count());
                TunableParameters tunableParametersUsed = fileCluster.getTunableParameters();
                TunableParameters tunableParametersEstimated = model.runModel(fileCluster, tunableParametersUsed,
                        throughput, ConfigurationParams.relaxation_ratios);
                //System.out.println("New CC received from model:" + tunableParametersEstimated.getConcurrency());
                checkForParameterUpdate(fileCluster, tunableParametersEstimated);
                lastBytesTransferred = xl.totalTransferredSize;
                lastUpdateTime = System.currentTimeMillis();
            }
            System.out.println("Leaving modelling connectionThread...");
        }

        void checkForParameterUpdate(FileCluster fileCluster, TunableParameters newTunableParameters){

            TunableParameters currentTunableParameters = fileCluster.getTunableParameters();
            int currentConcurrency = currentTunableParameters.getConcurrency();
            int currentParallelism = currentTunableParameters.getParallelism();
            int currentPipelining = currentTunableParameters.getPipelining();
            int newConcurrency = newTunableParameters.getConcurrency();
            int newParallelism = newTunableParameters.getParallelism();
            int newPipelining = newTunableParameters.getPipelining();

            //System.out.println("New parameters estimated:\t" + newConcurrency + "-" + newParallelism + "-" + newPipelining );


            if (newPipelining != currentPipelining) {
                System.out.println("New pipelining " + newPipelining );
                fileCluster.getRecords().channels.forEach(channel -> channel.setPipelining(newPipelining));
                fileCluster.getTunableParameters().setPipelining(newPipelining);
            }

            if (Math.abs(newParallelism - currentParallelism) >= 2 ||
                    Math.max(newParallelism, currentParallelism) >= 2 * Math.min(newParallelism, currentParallelism))  {
                System.out.println("New parallelism " + newParallelism );
                for (ChannelModule.ChannelPair channel : fileCluster.getRecords().channels) {
                    channel.isConfigurationChanged = true;
                    channel.newChunk = fileCluster;
                    System.out.println("Marked channel " + channel.getId() + " for parallelism update");
                }
                fileCluster.getTunableParameters().setParallelism(newParallelism);
                //fileCluster.clearTimeSeries();
            }

            XferList fileList = fileCluster.getRecords();
            if (newConcurrency > currentConcurrency) {
                System.out.println("Current CC:" + currentConcurrency+ "\t NewCC:" + newConcurrency);
                List<Future<ChannelModule.ChannelPair>> futures = new LinkedList<>();
                List<ChannelModule.ChannelPair> channelPairList= new LinkedList<>();
                for (int i = 0; i <newConcurrency - currentConcurrency; i++) {
                    ChannelPairBuilder channelPairBuilder = new ChannelPairBuilder(fileCluster,
                            ftpClient.channelList.size() + i);
                    futures.add(executor.submit(channelPairBuilder));
                }
                for (Future<ChannelModule.ChannelPair> future : futures) {
                    try {
                        ChannelModule.ChannelPair channelPair = future.get();
                        if (channelPair == null) {
                            System.out.println("ChannelPair is null, exiting...");
                            System.exit(-1);
                        }
                        channelPairList.add(channelPair);
                        synchronized (fileList.channels) {
                            fileList.channels.add(channelPair);
                        }
                        synchronized (ftpClient.channelList) {
                            ftpClient.channelList.add(channelPair);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                for (ChannelModule.ChannelPair channelPair: channelPairList) {
                    executor.execute(new FTPClient.TransferList(fileCluster.getRecords(), channelPair));
                }
                System.out.println("New concurrency level became " + newConcurrency);
            }
            else if (newConcurrency < currentConcurrency) {
                System.out.println("Closing channel " + (currentConcurrency - newConcurrency) + " channels");
                int lastIndex  = fileList.channels.size() - 1;
                for (int i = 0; i < currentConcurrency - newConcurrency; i++) {
                    ChannelModule.ChannelPair channelPair = fileList.channels.get(lastIndex - i);
                    channelPair.isAborted = true;
                    System.out.println("Marked channel " + channelPair.getId() + " for closing");
                }

            }
            fileCluster.getTunableParameters().setConcurrency(newConcurrency);
            //fileCluster.clearTimeSeries();
            model.setLastUpdateTime(System.currentTimeMillis());

        }

        int getUpdatedParameterValue (int []pastValues, int currentValue) {
            // System.out.println("Past values " + currentValue + ", "+ Arrays.toString(pastValues));

            boolean isLarger = pastValues[0] > currentValue;
            boolean isAllLargeOrSmall = true;
            for (int i = 0; i < pastValues.length; i++) {
                if ((isLarger && pastValues[i] <= currentValue) ||
                        (!isLarger && pastValues[i] >= currentValue)) {
                    isAllLargeOrSmall = false;
                    break;
                }
            }

            if (isAllLargeOrSmall) {
                int sum = 0;
                for (int i = 0; i< pastValues.length; i++) {
                    sum += pastValues[i];
                }
                System.out.println("Sum: " + sum + " length " + pastValues.length);
                return (int)Math.round(sum/(1.0 * pastValues.length));
            }
            return currentValue;
        }

        public static class ModellingJob {
            private final FileCluster chunk;
            private final TunableParameters tunableParameters;
            private final double sampleThroughput;

            public ModellingJob (FileCluster chunk, TunableParameters tunableParameters, double sampleThroughput) {
                this.chunk = chunk;
                this.tunableParameters = tunableParameters;
                this.sampleThroughput = sampleThroughput;
            }
        }
    }


}
