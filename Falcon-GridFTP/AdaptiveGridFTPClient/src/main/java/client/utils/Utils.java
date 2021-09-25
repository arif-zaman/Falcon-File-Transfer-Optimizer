package client.utils;

import client.AdaptiveGridFTPClient;
import client.ConfigurationParams;
import client.FileCluster;
import client.model.hysteresis.Entry;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.ChannelModule;
import transfer_protocol.util.XferList;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Utils {
  private static final Log LOG = LogFactory.getLog(Utils.class);

  public static String printSize(double random, boolean inByte) {
    char sizeUnit = 'b';
    if (inByte) {
      sizeUnit = 'B';
    }
    DecimalFormat df = new DecimalFormat("###.##");
    if (random < 1024.0) {
      return df.format(random) + " " + sizeUnit;
    } else if (random < 1024.0 * 1024) {
      return df.format(random / 1024.0) + " K" + sizeUnit;
    } else if (random < 1024.0 * 1024 * 1024) {
      return df.format(random / (1024.0 * 1024)) + " M" + sizeUnit;
    } else if (random < (1024 * 1024 * 1024 * 1024.0)) {
      return df.format(random / (1024 * 1024.0 * 1024)) + " G" + sizeUnit;
    } else {
      return df.format(random / (1024 * 1024 * 1024.0 * 1024)) + " T" + sizeUnit;
    }
  }

  public static TunableParameters getBestParams(XferList xl, int maximumChunks) {
    Density density = findDensityOfFile((long) xl.avgFileSize(), AdaptiveGridFTPClient.transferTask.getBandwidth(),
        maximumChunks);
    xl.density = density;
    double avgFileSize = xl.avgFileSize();
    int fileCountToFillThePipe = (int) Math.round(AdaptiveGridFTPClient.transferTask.getBDP() / avgFileSize);
    int pLevelToFillPipe = (int) Math.ceil(AdaptiveGridFTPClient.transferTask.getBDP() /
        AdaptiveGridFTPClient.transferTask.getBufferSize());
    int pLevelToFillBuffer = (int) Math.ceil(avgFileSize / AdaptiveGridFTPClient.transferTask.getBufferSize());
    //int basicClientControlChannel = Math.min(Math.min(Math.max(fileCountToFillThePipe, 2), xl.count()),
    //    AdaptiveGridFTPClient.transferTask.getMaxConcurrency());
    int cc = Math.min(xl.count(), maximumChunks);
    int ppq = Math.min(fileCountToFillThePipe, 100);
    int p = Math.max(Math.min(pLevelToFillPipe, pLevelToFillBuffer), 1);
    p = avgFileSize > AdaptiveGridFTPClient.transferTask.getBDP() ? p : p;
    return new TunableParameters.Builder()
        .setConcurrency(cc)
        .setParallelism(p)
        .setPipelining(ppq)
        .setBufferSize((int) AdaptiveGridFTPClient.transferTask.getBufferSize())
        .build();
  }

  public static XferList readInputFilesFromFile(InputStream stream, String src, String dst) throws Exception {
    //FileInputStream fstream = new FileInputStream(new File(fileName));
    BufferedReader br = new BufferedReader(new InputStreamReader(stream));
    // Find number of lines in the file to create an array of files beforehead
    // rather than using linkedlists which is slow.
    XferList fileEntries = new XferList(src, dst);
    String strLine;
    while ((strLine = br.readLine()) != null) {
      // Print the content on the console
      String[] fileEntry = strLine.split(" ");
      String path = fileEntry[0];
      long size = Long.parseLong(fileEntry[1]);
      fileEntries.add(path, size);
    }
    br.close();
    return fileEntries;
  }

  public static List<Integer> getChannels(XferList xl) {
    List<Integer> list = new LinkedList<Integer>();
    for (ChannelModule.ChannelPair channel : xl.channels) {
      list.add(channel.getId());
    }
    return list;
  }

  public static Density findDensityOfFile(long fileSize, double bandwidth, int maximumChunks) {
    double bandwidthInMB = bandwidth / 8.0;
    if (maximumChunks == 1 || fileSize <= bandwidthInMB / 20) {
      return Density.SMALL;
    } else if (maximumChunks > 3 && fileSize > bandwidthInMB * 2) {
      return Density.HUGE;
    } else if (maximumChunks > 2 && fileSize <= bandwidthInMB / 5) {
      return Density.MEDIUM;
    } else {
      return Density.LARGE;
    }
  }

  @VisibleForTesting
  public static ArrayList<FileCluster> createFileClusters(XferList list, double bandwidth, double rtt,
                                                          int maximumChunks) {
    list.shuffle();

    ArrayList<FileCluster> fileClusters = new ArrayList<>();
    for (int i = 0; i < maximumChunks; i++) {
      FileCluster p = new FileCluster();
      fileClusters.add(p);
    }
    for (XferList.MlsxEntry e : list) {
      if (e.dir) {
        continue;
      }
      Density density = Utils.findDensityOfFile(e.size(), bandwidth, maximumChunks);
      fileClusters.get(density.ordinal()).addRecord(e);
    }
    Collections.sort(fileClusters);
    mergePartitions(fileClusters, bandwidth, rtt);

    for (int i = 0; i < fileClusters.size(); i++) {
      FileCluster chunk = fileClusters.get(i);
      chunk.getRecords().sp = list.sp;
      chunk.getRecords().dp = list.dp;
      long avgFileSize = chunk.getRecords().size() / chunk.getRecords().count();
      chunk.setDensity(Entry.findDensityOfList(avgFileSize, bandwidth, maximumChunks));
      LOG.info("Chunk " + i + ":\tfiles:" + fileClusters.get(i).getRecords().count() + "\t avg:" +
              Utils.printSize(fileClusters.get(i).getCentroid(), true)
              + " \t total:" + Utils.printSize(fileClusters.get(i).getRecords().size(), true) + " Density:" +
              chunk.getDensity());
      chunk.getRecords().initialSize = chunk.getRecords().size();
    }
    return fileClusters;
  }

  public static ArrayList<FileCluster> mergePartitions(ArrayList<FileCluster> fileClusters, double bandwidth, double rtt) {
    double bdp = bandwidth * rtt / 8;
    for (int i = 0; i < fileClusters.size(); i++) {
      FileCluster p = fileClusters.get(i);
      //merge small chunk with the the chunk with closest centroid
      if ((p.getRecords().count() < 2 || p.getRecords().size() < 5 * bdp) && fileClusters.size() > 1) {
        int index = -1;
        double diff = Double.POSITIVE_INFINITY;
        for (int j = 0; j < fileClusters.size(); j++) {
          if (j != i && Math.abs(p.getCentroid() - fileClusters.get(j).getCentroid()) < diff) {
            diff = Math.abs(p.getCentroid() - fileClusters.get(j).getCentroid());
            index = j;
          }
        }
        if (index == -1) {
          LOG.fatal("Fatal error: Could not find chunk to merge!");
          System.exit(-1);
        }
        fileClusters.get(index).getRecords().addAll(p.getRecords());
        fileClusters.remove(i);
        i--;
      }
    }
    return fileClusters;
  }

  public static void allocateChannelsToChunks(List<FileCluster> chunks, final int channelCount,
                                       ConfigurationParams.ChannelDistributionPolicy channelDistPolicy) {
    int totalChunks = chunks.size();
    int[] fileCount = new int[totalChunks];
    for (int i = 0; i < totalChunks; i++) {
      fileCount[i] = chunks.get(i).getRecords().count();
    }

    int[] concurrencyLevels = new int[totalChunks];
    if (channelDistPolicy == ConfigurationParams.ChannelDistributionPolicy.ROUND_ROBIN) {
      int modulo = (totalChunks + 1) / 2;
      int count = 0;
      for (int i = 0; count < channelCount; i++) {
        int index = i % modulo;
        if (concurrencyLevels[index] < fileCount[index]) {
          concurrencyLevels[index]++;
          count++;
        }
        if (index < totalChunks - index - 1 && count < channelCount
                && concurrencyLevels[totalChunks - index - 1] < fileCount[totalChunks - index - 1]) {
          concurrencyLevels[totalChunks - index - 1]++;
          count++;
        }
      }

      for (int i = 0; i < totalChunks; i++) {
        FileCluster fileCluster = chunks.get(i);
        fileCluster.getTunableParameters().setConcurrency(concurrencyLevels[i]);
        System.out.println("Chunk " + i + ":" + concurrencyLevels[i] + "channels");
      }
    } else {
      double[] chunkWeights = new double[chunks.size()];
      double totalWeight = 0;

      double[] chunkSize = new double[chunks.size()];
      for (int i = 0; i < chunks.size(); i++) {
        chunkSize[i] = chunks.get(i).getTotalSize();
        Utils.Density densityOfChunk = chunks.get(i).getDensity();
        switch (densityOfChunk) {
          case SMALL:
            chunkWeights[i] = 3 * chunkSize[i];
            break;
          case MEDIUM:
            chunkWeights[i] = 2 * chunkSize[i];
            break;
          case LARGE:
            chunkWeights[i] = 1 * chunkSize[i];
            break;
          case HUGE:
            chunkWeights[i] = 1 * chunkSize[i];
            break;
          default:
            break;
        }
        totalWeight += chunkWeights[i];
      }
    int remainingChannelCount = channelCount;

      for (int i = 0; i < totalChunks; i++) {
        double propChunkWeight = (chunkWeights[i] * 1.0 / totalWeight);
        concurrencyLevels[i] = Math.min(remainingChannelCount, (int) Math.floor(channelCount * propChunkWeight));
        remainingChannelCount -= concurrencyLevels[i];
      }

      // Since we take floor when calculating, total channels might be unassigned.
      // If so, starting from fileClusters with zero channels, assign remaining channels
      // in round robin fashion
      for (int i = 0; i < chunks.size(); i++) {
        if (concurrencyLevels[i] == 0 && remainingChannelCount > 0) {
          concurrencyLevels[i]++;
          remainingChannelCount--;
        }
      }
      //find the fileClusters with minimum assignedChannelCount
      while (remainingChannelCount > 0) {
        int minChannelCount = Integer.MAX_VALUE;
        int chunkIdWithMinChannel = -1;
        for (int i = 0; i < chunks.size(); i++) {
          if (concurrencyLevels[i] < minChannelCount) {
            minChannelCount = concurrencyLevels[i];
            chunkIdWithMinChannel = i;
          }
        }
        concurrencyLevels[chunkIdWithMinChannel]++;
        remainingChannelCount--;
      }
      for (int i = 0; i < totalChunks; i++) {
        FileCluster fileCluster = chunks.get(i);
        fileCluster.getTunableParameters().setConcurrency(concurrencyLevels[i]);
        LOG.info("Chunk " + fileCluster.getDensity() + " weight " + chunkWeights[i] + " basicClientControlChannel: " + concurrencyLevels[i]);
      }
    }
  }


  // Ordering in this enum is important and has to stay as this for Utils.findDensityOfFile to perform as expected
  public
  enum Density {
    SMALL, LARGE, MEDIUM, HUGE
  }
}
