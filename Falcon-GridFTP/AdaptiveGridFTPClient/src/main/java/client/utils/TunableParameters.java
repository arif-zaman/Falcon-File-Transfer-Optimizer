package client.utils;

import client.AdaptiveGridFTPClient;

/**
 * Created by earslan on 12/6/16.
 */
public class TunableParameters {
  private int concurrency;
  private int parallelism;
  private int pipelining;
  private int bufferSize;

  public TunableParameters (int concurrency, int parallelism, int pipelining, int bufferSize) {
    this.concurrency = concurrency;
    this.parallelism = parallelism;
    this.pipelining = pipelining;
    this.bufferSize = bufferSize;
  }

  public TunableParameters (double [] parameters) {
    this.concurrency = (int) parameters[0];
    this.parallelism = (int) parameters[1];
    this.pipelining = (int) parameters[2];
    //this.bufferSize = (int) AdaptiveGridFTPClient.transferTask.getBufferSize();
  }

  public TunableParameters (Builder builder) {
    this.concurrency = builder.concurrency;
    this.parallelism = builder.parallelism;
    this.pipelining = builder.pipelining;
    this.bufferSize = builder.bufferSize;
  }

  public int getConcurrency () {
    return concurrency;
  }

  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public int getPipelining() {
    return pipelining;
  }

  public void setPipelining(int pipelining) {
    this.pipelining = pipelining;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Concurrency:" + concurrency + "\t");
    sb.append("Paralellism:" + parallelism + "\t");
    sb.append("Pipelining:" + pipelining);
    return sb.toString();
  }

  public static class Builder {
    int concurrency;
    int parallelism;
    int pipelining;
    int bufferSize;

    public Builder setConcurrency(int concurrency) {
      this.concurrency = concurrency;
      return this;
    }

    public Builder setParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder setPipelining(int pipelining) {
      this.pipelining = pipelining;
      return this;
    }

    public Builder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public TunableParameters build() {
      return new TunableParameters(this);
    }
  }
}
