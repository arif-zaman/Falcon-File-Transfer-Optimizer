/**
 *
 */
package client.model.hysteresis;

import client.AdaptiveGridFTPClient;
import client.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.util.List;
import java.util.Vector;

/**
 * @author earslan
 */
public class Entry {

  double similarityValue;
  Vector<Double> specVector;
  private int id;
  private String testbed;
  private String source, destination;
  private double bandwidth;
  private double RTT;
  private double BDP;
  private double bufferSize;
  private long fileSize;
  private double fileCount;
  private Utils.Density density;
  private double totalDatasetSize;
  private Date date;
  private double throughput, duration;
  private int parallellism, pipelining, concurrency;
  private boolean fast;
  private boolean isEmulation;
  private boolean isDedicated;
  private String note;
  // Used to distinguish same dataset runs with different times.
  private int groupNumber;
  private int maxConcurrency;

  private static final Log LOG = LogFactory.getLog(Entry.class);

  public Entry() {
    date = new Date();
    isDedicated = false;
    isEmulation = false;
    fast = false;
  }

  public static Utils.Density findDensityOfList(long averageFileSize, double bandwidth, int maximumChunks) {
    return Utils.findDensityOfFile(averageFileSize, bandwidth, maximumChunks);
  }


  public static Utils.Density findDensityOfList(List<Entry> list) {
    long totalSize = 0;
    for (Entry e : list) {
      totalSize += e.fileSize;
    }
    long averageFileSize = totalSize/list.size();
    double bandwidthInMB = AdaptiveGridFTPClient.transferTask.bandwidth / 8.0;
    if (averageFileSize < bandwidthInMB / 20) {
      return Utils.Density.SMALL;
    } else if (averageFileSize < bandwidthInMB / 5) {
      return Utils.Density.MEDIUM;
    } else if (averageFileSize < bandwidthInMB * 2) {
      return Utils.Density.LARGE;
    }
    return Utils.Density.HUGE;
  }


  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(int id) {
    this.id = id;
  }

  /**
   * @return the testbed
   */
  public String getTestbed() {
    return testbed;
  }

  /**
   * @param testbed the testbed to set
   */
  public void setTestbed(String testbed) {
    this.testbed = testbed;
  }

  /**
   * @return the source
   */
  public String getSource() {
    return source;
  }

  /**
   * @param source the source to set
   */
  public void setSource(String source) {
    this.source = source;
  }

  /**
   * @return the destination
   */
  public String getDestination() {
    return destination;
  }

  /**
   * @param destination the destination to set
   */
  public void setDestination(String destination) {
    this.destination = destination;
  }

  /**
   * @return the bandwidth
   */
  public double getBandwidth() {
    return bandwidth;
  }

  /**
   * @param bandwidth the bandwidth to set
   */
  public void setBandwidth(double bandwidth) {
    this.bandwidth = bandwidth;
  }

  /**
   * @return the rtt
   */
  public double getRtt() {
    return RTT;
  }

  /**
   * @param rtt the rtt to set
   */
  public void setRtt(double rtt) {
    this.RTT = rtt;
  }

  /**
   * @return the bDP
   */
  public double getBDP() {
    return BDP;
  }

  /**
   * @param BDP the bDP to set
   */
  public void setBDP(double BDP) {
    this.BDP = BDP;
  }

  /**
   * @return the bufferSize
   */
  public double getBufferSize() {
    return bufferSize;
  }

  /**
   * @param bufferSize the bufferSize to set
   */
  public void setBufferSize(double bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return the fileSize
   */
  public long getFileSize() {
    return fileSize;
  }

  /**
   * @param fileSize the fileSize to set
   */
  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  /**
   * @return the fileCount
   */
  public double getFileCount() {
    return fileCount;
  }

  /**
   * @param fileCount the fileCount to set
   */
  public void setFileCount(double fileCount) {
    this.fileCount = fileCount;
  }

  /**
   * @return the density
   */
  public Utils.Density getDensity() {
    return density;
  }

  /**
   * @param density the density to set
   */
  public void setDensity(Utils.Density density) {
    this.density = density;
  }

  /**
   * @return the totalDatasetSize
   */
  public double getTotalDatasetSize() {
    return totalDatasetSize;
  }

  /**
   * @param totalDatasetSize the totalDatasetSize to set
   */
  public void setTotalDatasetSize(double totalDatasetSize) {
    this.totalDatasetSize = totalDatasetSize;
  }

  /**
   * @return the date
   */
  public Date getDate() {
    return date;
  }

  /**
   * @param date the date to set
   */
  public void setDate(Date date) {
    this.date = date;
  }

  /**
   * @return the throughput
   */
  public Double getThroughput() {
    return throughput;
  }

  /**
   * @param throughput the throughput to set
   */
  public void setThroughput(Double throughput) {
    this.throughput = throughput;
  }

  /**
   * @return the duration
   */
  public Double getDuration() {
    return duration;
  }

  /**
   * @param duration the duration to set
   */
  public void setDuration(Double duration) {
    this.duration = duration;
  }

  /**
   * @return the parallellism
   */
  public int getParallellism() {
    return parallellism;
  }

  /**
   * @param parallellism the parallellism to set
   */
  public void setParallellism(int parallellism) {
    this.parallellism = parallellism;
  }

  /**
   * @return the pipelining
   */
  public int getPipelining() {
    return pipelining;
  }

  /**
   * @param pipelining the pipelining to set
   */
  public void setPipelining(int pipelining) {
    this.pipelining = pipelining;
  }

  /**
   * @return the concurrency
   */
  public int getConcurrency() {
    return concurrency;
  }

  /**
   * @param concurrency the concurrency to set
   */
  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
  }

  /**
   * @return the fast
   */
  public boolean getFast() {
    return fast;
  }

  /**
   * @param fast the fast to set
   */
  public void setFast(boolean fast) {
    this.fast = fast;
  }

  /**
   * @return the isEmulation
   */
  public boolean isEmulation() {
    return isEmulation;
  }

  /**
   * @param isEmulation the isEmulation to set
   */
  public void setEmulation(boolean isEmulation) {
    this.isEmulation = isEmulation;
  }

  /**
   * @return the isDedicated
   */
  public boolean isDedicated() {
    return isDedicated;
  }

  /**
   * @param isDedicated the isDedicated to set
   */
  public void setDedicated(boolean isDedicated) {
    this.isDedicated = isDedicated;
  }

  /**
   * @return the note
   */
  public String getNote() {
    return note;
  }

  /**
   * @param note the note to set
   */
  public void setNote(String note) {
    this.note = note;
  }

  /**
   * @return the similarityValue
   */
  public double getSimilarityValue() {
    return similarityValue;
  }

  /**
   * @param similarityValue the similarityValue to set
   */
  public void setSimilarityValue(double similarityValue) {
    this.similarityValue = similarityValue;
  }

  /**
   * @return the specVector
   */
  public Vector<Double> getSpecVector() {
    return specVector;
  }

  /**
   * @param specVector the specVector to set
   */
  public void setSpecVector(Vector<Double> specVector) {
    this.specVector = specVector;
  }

  /**
   * @return the maxConcurrency
   */
  public int getMaxConcurrency() {
    return maxConcurrency;
  }

  /**
   * @param maxConcurrency the maxConcurrency to set
   */
  public void setMaxConcurrency(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
  }

  int DensityToValue(Utils.Density density) {
    if (density == Utils.Density.SMALL) {
      return 1;
    }
    if (density == Utils.Density.MEDIUM) {
      return 11;
    }
    if (density == Utils.Density.LARGE) {
      return 21;
    }
    if (density == Utils.Density.HUGE) {
      return 31;
    } else {
      return -1;
    }
  }

  public void calculateSpecVector() {
    specVector = new Vector<Double>();
    //specVector.add(fileSize/(1024*1024*1024));
    //specVector.add(fileSize);
    specVector.add(bandwidth);
    specVector.add(RTT);
    specVector.add((bandwidth * RTT) / (8.0 * bufferSize));
    specVector.add(DensityToValue(density) * 1.0);
    /*
    if(isDedicated)
			specVector.add(0.0);
		else
			specVector.add(1.0);
		*/
    //specVector.add(bufferSize);
    //specVector.add(bufferSize/(1024*1024));
    specVector.add(Math.log10(fileSize / (1024 * 1024)));
    specVector.add(Math.log10(fileCount) + 1);


  }

  String getIdentity() {
    return fileSize + "*" + fileCount + "*" + density.name() + "*" + testbed + "*" + source + "*" +
            destination + "*" + bandwidth + "*" + RTT + "*" + bufferSize + isEmulation + "*" + isDedicated;
  }

  String getParameters() {
    return parallellism + "*" + concurrency + "*" + pipelining + "*" + fast;
  }

  public int getGroupNumber() {
    return groupNumber;
  }

  public void setGroupNumber(int groupNumber) {
    this.groupNumber = groupNumber;
  }

  @Override
  public int hashCode() {
    //System.out.println("HashCode:"+getIdentity().hashCode());
    return getIdentity().hashCode();
  }

  public void printEntry(String... extraInfo) {
    StringBuilder sb = new StringBuilder("");
    for (int i = 0; i < extraInfo.length; i++) {
      sb.append(extraInfo[i] + "\t");
    }
    LOG.info(note + "*" + fileSize + "*" + fileCount + "*" + density.name() + "*" + testbed + "*" + source + "*" +
            destination + "*" + bandwidth + "*" + RTT + "*" + bufferSize + "*p:" + parallellism + "*basicClientControlChannel:" +
            concurrency + "*ppq:" + pipelining + "*" + fast + "*" + throughput + "*" + isEmulation + "*" +
            isDedicated + " date:" + date.toString() + " Extra:" + sb.toString());
  }

  public String printSpecVector() {
    return specVector.toString();
  }
}

