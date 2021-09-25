package transfer_protocol.util;

import client.ConfigurationParams;
import client.utils.Utils;
import org.apache.commons.math3.ml.clustering.Clusterable;
import transfer_protocol.module.ChannelModule;

import java.util.*;

public class XferList implements Iterable<XferList.MlsxEntry> {
  private final MlsxEntry root;
  public String sp, dp;
  public long totalTransferredSize = 0, instantTransferredSize = 0;
  public double estimatedFinishTime = Integer.MAX_VALUE;
  public double instant_throughput = 0, weighted_throughput = 0;
  public long initialSize = 0;
  public List<ChannelModule.ChannelPair> channels;
  public int interval = 0;
  public int onAir = 0;
  public Utils.Density density;
  private LinkedList<MlsxEntry> list = new LinkedList<>();
  private long size = 0;
  private int count = 0;  // Number of files (not dirs)

  // Create an XferList for a directory.
  public XferList(String src, String dest) {
    if (!src.endsWith("/")) {
      src += "/";
    }
    if (!dest.endsWith("/")) {
      dest += "/";
    }
    sp = src;
    dp = dest;
    root = new MlsxEntry("");
  }

  // Create an XferList for a file.
  public XferList(String src, String dest, long size) {
    String[] bits = src.split("/");
    String fileName = bits[bits.length - 1];
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < bits.length - 1; i++) {
      builder.append(bits[i]+"/");
    }
    sp = builder.toString();
    dp = dest;
    root = new MlsxEntry(fileName, size);
    list.add(root);
    this.size += size;
    count ++;
  }

  // Add a directory to the list.
  public void add(String fileName) {
    list.add(new MlsxEntry(fileName));
  }

  // Add a file to the list.
  public void add(String path, long size) {
    list.add(new MlsxEntry(path, size));
    this.size += size;
    this.count++;
  }

  public void addEntry(MlsxEntry e) {
    list.add(e);
    if (e.len == -1) {
      size += e.remaining();
    } else {
      size += e.len;
    }
    count++;
  }

  // Add another XferList's entries under this XferList.
  public void addAll(XferList ol) {
    size += ol.size;
    count += ol.count;
    for (MlsxEntry e : ol)
      list.add(e);
    //list.addAll(ol.list);
  }

  public long size() {
    return size;
  }

  public int count() {
    return count;
  }

  public boolean isEmpty() {
    return list.isEmpty();
  }

  // Remove and return the topmost entry.
  public MlsxEntry pop() {
    try {
      MlsxEntry e = list.pop();
      size -= e.size;
      count--;
      return e;
    } catch (Exception e) {
      return null;
    }
  }

  public MlsxEntry getItem(int index) {
    try {
      MlsxEntry e = list.get(index);
      return e;
    } catch (Exception e) {
      return null;
    }
  }

  public LinkedList<MlsxEntry> getFileList() {
    return list;
  }

  public void removeItem(int index) {
    try {
      MlsxEntry e = list.get(index);
      size -= e.size;
      list.remove(index);
    } catch (Exception e) {

    }
  }

  public void removeAll() {
    try {
      while (!list.isEmpty())
        list.removeFirst();
      count = 0;
      size = 0;
    } catch (Exception e) {

    }
  }

  public void shuffle() {
    long seed = System.nanoTime();
    Collections.shuffle(list, new Random(seed));
    Collections.shuffle(list, new Random(seed));
  }

  // Get the progress of the list in terms of bytes.
  public Progress byteProgress() {
    Progress p = new Progress();

    for (MlsxEntry e : list)
      p.add(e.remaining(), e.size);
    return p;
  }

  // Get the progress of the list in terms of files.
  public Progress fileProgress() {
    Progress p = new Progress();

    for (MlsxEntry e : list)
      p.add(e.done ? 1 : 0, 1);
    return p;
  }

  // Split off a sublist from the front of this list which is
  // of a certain byte length.
  public XferList split(long len) {
    XferList nl = new XferList(sp, dp);
    Iterator<MlsxEntry> iter = iterator();

    if (len == -1 || size <= len) {
      // If the request is bigger than the list, empty into new list.
      nl.list = list;
      nl.size = size;
      list = new LinkedList<MlsxEntry>();
      size = 0;
    } else {
      while (len > 0 && iter.hasNext()) {
        MlsxEntry e2, e = iter.next();

        if (e.done) {
          iter.remove();
        } else if (e.dir || e.size() <= len || e.size() - len < (ConfigurationParams.MAXIMUM_SINGLE_FILE_SIZE / 5)) {
          nl.addEntry(new MlsxEntry(e.spath, e));
          len -= e.size();
          iter.remove();
          count--;
          size -= e.size();
          e.done = true;
        } else {  // Need to split file...
          e2 = new MlsxEntry(e.spath, e);
          e2.len = len;
          nl.addEntry(e2);
          size -= e2.len;
          if (e.len == -1) {
            e.len = e.size;
          }
          e.len -= len;
          e.off += len;
          len = 0;
        }
      }
    }
    return nl;
  }

  public XferList sliceLargeFiles(long sliceSize) {
    Iterator<MlsxEntry> iter = iterator();
    XferList nl = new XferList(sp, dp);
    while (iter.hasNext()) {
      MlsxEntry e2, e = iter.next();
      if (e.size() > sliceSize) {
        int pieceCount = (int) Math.ceil(1.0 * e.size / sliceSize);
        long pieceSize = (int) (e.size / pieceCount);
        long offset = 0;
        //System.out.println("Removing "+ e.spath() + " " + e.off + " size:"+ e.remaining() + " " + e.len + " to insert "+ pieceCount + "pieces" + " each" + pieceSize);
        for (int i = 1; i < pieceCount; i++) {
          e2 = new MlsxEntry(e.spath, e);
          e2.len = pieceSize;
          e2.off = offset;
          offset += pieceSize + 1;
          nl.addEntry(e2);
        }
        e2 = new MlsxEntry(e.spath, e);
        e2.len = -1;
        e2.off = offset;
        nl.addEntry(e2);
        //iter.remove();
      } else {
        e2 = new MlsxEntry(e.spath, e);
        nl.addEntry(e2);
      }
    }
    return nl;
  }

  public double avgFileSize() {
    return count == 0 ? 0 : size / count;
  }

  public Iterator<MlsxEntry> iterator() {
    return list.iterator();
  }

  public void updateDestinationPaths() {
    for (MlsxEntry e : list) {
      if (dp.endsWith("/")) {
        e.setdpath(dp + e.spath);
      } else {
        e.setdpath(dp);
      }
    }
  }

  public synchronized void updateTransferredSize (long size) {
    totalTransferredSize += size;
  }

  public synchronized  MlsxEntry getNextFile() {
    if (count > 0) {
      return pop();
    }
    return null;
  }

  public synchronized void updateOnAir (int count) {
    onAir += count;
    return;
  }

  // An entry (file or directory) in the list.
  public class MlsxEntry implements Clusterable {
    public final boolean dir;
    public final long size;
    public String fileName, spath, dpath;
    public boolean done = false;
    public long off = 0, len = -1;  // Start/stop offsets.
    public long interruptedOffset = 0;

    // Create a directory entry.
    MlsxEntry(String spath) {
      if (spath == null) {
        spath = "";
      } else if (!spath.endsWith("/")) {
        spath += "/";
      }
      spath = spath.replaceFirst("^/+", "");
      this.spath = spath;
      size = off = 0;
      dir = true;
    }

    // Create a file entry.
    public MlsxEntry(String spath, long size) {
      String fileName = spath.substring(spath.lastIndexOf('/') + 1);
      this.spath = spath;
      this.fileName = fileName;
      this.size = (size < 0) ? 0 : size;
      off = 0;
      dir = false;
    }

    // Create an entry based off another entry with a new spath.
    MlsxEntry(String spath, MlsxEntry e) {
      this.spath = spath;
      dir = e.dir;
      done = e.done;
      size = e.size;
      off = e.off;
      len = e.len;
    }

    public long remaining() {
      if (done || dir) {
        return 0;
      }
      if (off > size) {
        return 0;
      }
      //if (len == -1 || len > size)
      return size - off;
      //return len - off;
    }

    public long size() {
      if (done || dir) {
        return 0;
      }
      if (off > size) {
        return 0;
      }
      if (len == -1) {
        return size - off;
      }
      return len;
    }

    public String fullPath() {
      return XferList.this.sp + spath;
    }

    public void setdpath(String dp) {
      dpath = dp;
    }

    public String dpath() {
      return dpath;
    }

    public String toString() {
      return (dir ? "Directory: " : "File: ") + spath + " -> " + dpath();
    }

    @Override
    public double[] getPoint() {
      return new double[] {size};
    }
  }
}