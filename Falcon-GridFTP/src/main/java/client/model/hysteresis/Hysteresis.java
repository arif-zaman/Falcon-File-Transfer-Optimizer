package client.model.hysteresis;

import client.AdaptiveGridFTPClient;
import client.ConfigurationParams;
import client.FileCluster;
import client.model.Model;
import client.utils.TunableParameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Hysteresis extends Model {

  private static final Log LOG = LogFactory.getLog(Hysteresis.class);
  private static List<List<Entry>> entries;
  private double[] estimatedThroughputs;
  private int[][] estimatedParamsForChunks;

  public TunableParameters runModel(FileCluster chunk, TunableParameters tunableParameters, double sampleThroughput,
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
      tunableParameters.setConcurrency(Integer.parseInt(values[0]));
      tunableParameters.setParallelism(Integer.parseInt(values[1]));
      tunableParameters.setPipelining(Integer.parseInt(values[2]));
    } catch(Exception e) {
      System.out.println(e);
    }
    return tunableParameters;
  }
  public TunableParameters requestNextParameters() {
    return new TunableParameters.Builder().setConcurrency(1).build();
  }

  private void parseInputFiles() {
    File folder = new File(ConfigurationParams.INPUT_DIR);
    if (!folder.exists()) {
      LOG.error("Cannot access to " + folder.getAbsoluteFile());
      System.exit(-1);
    }
    File[] listOfFiles = folder.listFiles();
    if (listOfFiles.length == 0) {
      LOG.error("No historical data found at " + folder.getAbsoluteFile());
      System.exit(-1);
    }
    List<String> historicalDataset = new ArrayList<>(listOfFiles.length);
    entries = new ArrayList<>();
    for (File listOfFile : listOfFiles) {
      if (listOfFile.isFile()) {
        historicalDataset.add(ConfigurationParams.INPUT_DIR + listOfFile.getName());
      }
    }
    for (String fileName : historicalDataset) {
      List<Entry> fileEntries = Similarity.readFile(fileName);
      if (!fileEntries.isEmpty()) {
        entries.add(fileEntries);
      }
    }
  }

  public void findOptimalParameters(List<FileCluster> chunks, Entry transferTask) throws Exception {
    parseInputFiles();
    if (entries.isEmpty()) {  // Make sure there are log files to run hysterisis
      LOG.fatal("No input entries found to run hysterisis analysis. Exiting...");
    }
    double[] maxValues = Similarity.normalizeDataset(entries);
    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      Entry chunkSpecs = new Entry();
      chunkSpecs.setBandwidth(transferTask.getBandwidth());
      chunkSpecs.setRtt(transferTask.getRtt());
      chunkSpecs.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
      chunkSpecs.setBufferSize(transferTask.getBufferSize());
      chunkSpecs.setFileCount(chunks.get(chunkNumber).getRecords().count());
      chunkSpecs.setFileSize((new Double(chunks.get(chunkNumber).getRecords().avgFileSize()).longValue()));
      chunkSpecs.setDensity(Entry.findDensityOfList(chunkSpecs.getFileSize(), chunkSpecs.getBandwidth(),
             transferTask.getMaxConcurrency()));
      chunkSpecs.calculateSpecVector();
      //chunkSpecs.getIdentity()
      Similarity.normalizeEntry(maxValues, chunkSpecs);
      List<Entry> similarEntries = Similarity.findSimilarEntries(entries, chunkSpecs);
      //Categorize selected entries based on log date
      Similarity.categorizeEntries(similarEntries, chunks.get(chunkNumber).getDensity().name());
      LOG.info("Chunk "+chunkNumber + " entries are categorized and written to disk at ");
    }
  }

  public double[] getEstimatedThroughputs() {
    return estimatedThroughputs;
  }

  public int[][] getEstimatedParams() {
    return estimatedParamsForChunks;
  }

}
