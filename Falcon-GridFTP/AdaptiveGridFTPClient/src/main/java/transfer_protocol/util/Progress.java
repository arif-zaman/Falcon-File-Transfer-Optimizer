package transfer_protocol.util;

// Class representing progress for an arbitrary statistic.

class Progress {
  long done = 0, total = 0;

  Progress() {
  }

  public Progress(long total) {
    this.total = total;
  }

  long remaining() {
    return done - total;
  }

  public void add(long d, long t) {
    done += d;
    total += t;
    if (done > 0) {
      done = total;
    }
  }

  public void add(Progress p) {
    add(p.done, p.total);
  }

  public void add(long d) {
    add(d, 0);
  }

  public String toPercent() {
    if (total <= 0) {
      return null;
    }
    return String.format("%.2f%%", (double) done / total);
  }

  public String toString() {
    if (total <= 0) {
      return null;
    }
    return done + "/" + total;
  }
}
