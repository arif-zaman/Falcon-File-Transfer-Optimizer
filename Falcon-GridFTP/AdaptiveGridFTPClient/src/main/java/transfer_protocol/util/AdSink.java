package transfer_protocol.util;

// Ad sink to allow for ads from multiple sources.

public class AdSink {
  private volatile boolean closed = false;
  private volatile boolean more = true;
  private volatile ClassAd ad = null;

  public synchronized void close() {
    closed = true;
    System.out.println("Closing ad sink...");
    notifyAll();
  }

  synchronized void putAd(ClassAd ad) {
    if (closed) {
      return;
    }
    this.ad = ad;
    notifyAll();
  }

  public synchronized void mergeAd(ClassAd a) {
    putAd((ad != null) ? ad.merge(a) : a);
  }

  // Block until an ad has come in, then clear the ad.
  public synchronized ClassAd getAd() {
    if (!closed && more) {
      try {
        wait();
        if (ad == null) {
          return null;
        }
        if (closed) {
          more = false;
        }
        return new ClassAd(ad);
      } catch (Exception e) {
      }
    }
    return null;
  }

  // Get the ad current in the sink, or an empty ad if none.
  public synchronized ClassAd peekAd() {
    return (ad != null) ? new ClassAd(ad) : new ClassAd();
  }
}