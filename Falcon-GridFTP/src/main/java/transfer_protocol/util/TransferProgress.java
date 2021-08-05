package transfer_protocol.util;

// Used to track transfer progress. Just tell this thing when some
// bytes or a file are done and it will update its state with that
// information and, if an AdSink is attached, publish an ad to it.
//
// FYI: All times here are in milliseconds!

public class TransferProgress {
  private long start_time = -1, end_time = -1;
  private Progress byte_progress = new Progress();
  private Progress file_progress = new Progress();
  private AdSink sink = null;

  // Metrics used to calculate instantaneous throughput.
  private double q = 500.0; // Time quantum for throughput.
  private long lst = -1;  // last sample time
  private long btq = 0;  // bytes this quantum
  private long blq = 0;  // bytes last quantum

  // Get the current time in ms.
  public static long now() {
    return System.nanoTime() / (long) 1E6;
  }

  // Pretty format a throughput.
  public static String prettyThrp(double tp, char pre) {
    if (tp >= 1000) {
      switch (pre) {
        case ' ':
          return prettyThrp(tp / 1000, 'k');
        case 'k':
          return prettyThrp(tp / 1000, 'M');
        case 'M':
          return prettyThrp(tp / 1000, 'G');
        case 'G':
          return prettyThrp(tp / 1000, 'T');
      }
    }
    return String.format("%.2f%cB/sec", tp, pre);
  }

  // Pretty format a duration (in milliseconds);
  public static String prettyTime(long t) {
    if (t < 0) {
      return null;
    }

    long i = t % 1000,
            s = (t /= 1000) % 60,
            m = (t /= 60) % 60,
            h = (t /= 60) % 24,
            d = t / 24;

    return (d > 0) ? String.format("%dd%02dh%02dm%02ds", d, h, m, s) :
            (h > 0) ? String.format("%dh%02dm%02ds", h, m, s) :
                    (m > 0) ? String.format("%dm%02ds", m, s) :
                            String.format("%d.%02ds", s, i / 10);
  }

  // Can be used to change time quantum. Minimum 1ms.
  public synchronized void setQuantum(double t) {
    q = (t < 1.0) ? 1.0 : t;
    btq = blq = 0;
  }

  // Attach an AdSink to publish progress information to.
  public synchronized void attach(AdSink sink) {
    this.sink = sink;
  }

  // Publish an ad to the AdSink, if there is one.
  private synchronized void updateAd() {
    if (sink != null) {
      sink.putAd(getAd(sink.peekAd()));
    }
  }

  // Get the ClassAd representation of this transfer progress.
  public ClassAd getAd() {
    return getAd(new ClassAd());
  }

  private ClassAd getAd(ClassAd ad) {
    ad.insert("byte_progress", byte_progress.toString());
    ad.insert("progress", byte_progress.toPercent());
    ad.insert("file_progress", file_progress.toString());
    ad.insert("throughput", throughput(false));
    if (duration() >= 1000) {
      ad.insert("avg_throughput", throughput(true));
    }
    return ad;
  }

  // Called when a transfer starts.
  public synchronized void transferStarted(long bytes, int files) {
    start_time = now();
    end_time = -1;
    byte_progress.done = file_progress.done = 0;
    byte_progress.total = bytes;
    file_progress.total = files;
    updateAd();
  }

  // Called when a transfer ends. If successful, assume any unreported
  // bytes and files have finished and report them ourselves.
  public synchronized void transferEnded(boolean success) {
    if (end_time == -1) {
      end_time = now();
      if (success) {
        done(byte_progress.remaining(), (int) file_progress.remaining());
      } else  // done() will update ad, so only do it if we didn't call...
      {
        updateAd();
      }
    }
  }

  // Called when some bytes/file have finished transferring.
  public synchronized void done(long bytes) {
    done(bytes, 0);
  }

  public synchronized void done(long bytes, int files) {
    long now = now();
    long d = now - lst;

    // See how to calculate bytes this quantum.
    if (lst == -1) {
      btq = bytes;
    } else if (d >= 2 * q) {
      blq = btq = (long) (bytes * (q / d));
    } else if (d >= q) {
      blq = (long) (bytes * (1 - q / d) + btq * (2 - d / q));
      btq = (long) (bytes * (q / d));
    } else {
      blq = (long) (blq * (1 - d / q) + btq * (d / q));
      btq = (long) (btq * (1 - d / q) + bytes);
    }
    lst = now;

    if (bytes > 0) {
      byte_progress.add(bytes);
    }
    if (files > 0) {
      file_progress.add(files);
    }

    updateAd();
  }

  // Get the throughput in bytes per second. When doing instantaneous
  // throughput, takes current time quantum as well as last quantum into
  // account (the last scaled by how far into this quantum we are).
  public double throughputValue(boolean avg) {
    double d;  // Duration in ms.
    long b;  // Bytes over duration.

    if (avg) {  // Calculate average
      if (start_time == -1) {
        return -1.0;
      }
      d = (double) duration();
      b = byte_progress.done;
    } else if (end_time >= 0) {
      return -1.0;  // If we've ended, inst thrp means nothing!
    } else {  // Calculate instantaneous
      if (lst == -1) {
        return -1.0;
      }
      d = (double) (now() - lst);
      if (d >= 2 * q) {
        return 0.0;
      } else if (d >= q)
      //b = (long) (btq*(1-(d-q)/q));
      {
        b = btq;
      } else {
        b = (long) (btq * (d / q) + blq * (1 - d / q));
      }
      d = q;
    }

    if (d <= 0.0) {
      return 0.0;
    }
    return b / d * 1000.0;
  }

  public String throughput(boolean avg) {
    double t = throughputValue(avg);
    return (t >= 0) ? prettyThrp(t, ' ') : null;
  }

  // Get the duration of the transfer in milliseconds.
  public long duration() {
    if (start_time == -1) {
      return 0;
    }
    if (end_time == -1) {
      return now() - start_time;
    }
    return end_time - start_time;
  }

  public Progress byteProgress() {
    return byte_progress;
  }

  public Progress fileProgress() {
    return file_progress;
  }
}