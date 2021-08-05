package client;

import client.model.hysteresis.Entry;
import client.model.hysteresis.Hysteresis;
import client.utils.TunableParameters;
import client.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.GridFTPClient;
import transfer_protocol.util.XferList;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

public class AdaptiveGridFTPClient {

  public static Entry transferTask;
  public static boolean isTransferCompleted = false;
  public static long start;
  private GridFTPClient gridFTPClient;
  ConfigurationParams conf;
  private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);


  public AdaptiveGridFTPClient() {
    conf = new ConfigurationParams();
  }

  @VisibleForTesting
  public AdaptiveGridFTPClient(GridFTPClient gridFTPClient) {
    this.gridFTPClient = gridFTPClient;
  }

  public static void main(String[] args) throws Exception {
    AdaptiveGridFTPClient adaptiveGridFTPClient = new AdaptiveGridFTPClient();
    adaptiveGridFTPClient.parseArguments(args);
    adaptiveGridFTPClient.transfer();

  }

  private void parseArguments(String[] arguments) {
    conf.parseArguments(arguments, transferTask);

  }

  @VisibleForTesting
  void transfer() throws Exception {
    // Setup new transfer task based on user arguments
    transferTask = new Entry();
    transferTask.setSource(conf.source);
    transferTask.setDestination(conf.destination);
    transferTask.setBandwidth(conf.bandwidth);
    transferTask.setRtt(conf.rtt);
    transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
    transferTask.setBufferSize(conf.bufferSize);
    transferTask.setMaxConcurrency(conf.maxConcurrency);



    if (gridFTPClient == null) {
      gridFTPClient = new GridFTPClient(conf.source, conf.destination);
      gridFTPClient.start();
      gridFTPClient.waitFor();
    }

    if (gridFTPClient == null || GridFTPClient.ftpClient == null) {
      LOG.info("Could not establish GridFTP connection. Exiting...");
      System.exit(-1);
    }
    //Additional transfer configurations
    gridFTPClient.useDynamicScheduling = conf.useDynamicScheduling;
    gridFTPClient.useOnlineTuning = conf.useOnlineTuning;
    gridFTPClient.setPerfFreq(conf.perfFreq);
    GridFTPClient.ftpClient.setEnableIntegrityVerification(conf.enableIntegrityVerification);

    //First fetch the list of files to be transferred
    XferList dataset = gridFTPClient.getListofFiles();
    long datasetSize = dataset.size();

    ArrayList<FileCluster> chunks = Utils.createFileClusters(dataset, conf.bandwidth, conf.rtt, conf.maximumChunks);

    int initialConcurrency = conf.maxConcurrency;
    if (conf.model != null) {
      GridFTPClient.modellingThread = new GridFTPClient.ModellingThread(chunks.get(0),"gradient");
      initialConcurrency = GridFTPClient.modellingThread.getModel().requestNextParameters().getConcurrency();
    }


    GridFTPClient.ftpClient.fileClusters.add(chunks.get(0));
    start = System.currentTimeMillis();

    int[][] estimatedParamsForChunks = new int[chunks.size()][4];
    long timeSpent = 0;


    switch (conf.algorithm) {
      case SINGLECHUNK:
        chunks.forEach(chunk->chunk.setTunableParameters(Utils.getBestParams(chunk.getRecords(), conf.maximumChunks)));
        if (conf.useMaxCC) {
          chunks.forEach(chunk -> chunk.getTunableParameters().setConcurrency(
              Math.min(transferTask.getMaxConcurrency(), chunk.getRecords().count())));
        }
        //GridFTPClient.executor.submit(new GridFTPClient.ModellingThread());
        chunks.forEach(chunk -> gridFTPClient.runTransfer(chunk));
        break;
      default:
        // Make sure total channels count does not exceed total file count
        int totalChannelCount = Math.min(conf.maxConcurrency, dataset.count());
        //totalChannelCount = Math.min(chunks.get(0).getRecords().count(),bestCC);
        if (conf.model != null) {
          for (FileCluster fileCluster : chunks) {
            fileCluster.setTunableParameters(Utils.getBestParams(fileCluster.getRecords(), conf.maximumChunks));
          }
          totalChannelCount = initialConcurrency;
        } else {
          for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            System.out.println("Calculated CC:" + Utils.getBestParams(chunks.get(i).getRecords(), conf.maxConcurrency).getConcurrency());
            chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), conf.maxConcurrency));
            //chunks.get(i).getTunableParameters().setConcurrency(bestCC);
          }
        }
        if (totalChannelCount == 0) {
          break;
        }
        LOG.info(" Running MC with :" + totalChannelCount + " channels.");
        Utils.allocateChannelsToChunks(chunks, totalChannelCount, conf.channelDistPolicy);
        for (FileCluster chunk : chunks) {
          LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                  " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }
        for (FileCluster fileCluster: chunks) {
          gridFTPClient.runTransfer(fileCluster);
        }
        //start = System.currentTimeMillis();
        //gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);
        break;
    }
    if (conf.model != null) {
      GridFTPClient.executor.submit(GridFTPClient.modellingThread);
    }
    //gridFTPClient.startTransferMonitor();
    gridFTPClient.waitForTransferCompletion();
    timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
    LOG.info("Total Time\t" + timeSpent +
            " Throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
    System.out.println("Total Time\t" + timeSpent +
            " Throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));

    isTransferCompleted = true;
    GridFTPClient.executor.shutdown();
    while (!GridFTPClient.executor.isTerminated()) {
    }
    gridFTPClient.stop();
  }

}
