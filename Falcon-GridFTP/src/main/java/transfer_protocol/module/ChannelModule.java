package transfer_protocol.module;

import client.FileCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.ftp.*;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.extended.GridFTPControlChannel;
import org.globus.ftp.extended.GridFTPServerFacade;
import org.globus.ftp.vanilla.*;
import org.ietf.jgss.GSSCredential;
import transfer_protocol.util.StorkUtil;
import transfer_protocol.util.TransferProgress;
import transfer_protocol.util.XferList;
import transfer_protocol.util.XferList.MlsxEntry;

import java.io.*;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class ChannelModule {

  private static final Log LOG = LogFactory.getLog(ChannelModule.class);

  private static String MODULE_NAME = "Stork GridFTP Module";
  private static String MODULE_VERSION = "0.1";

  // A sink meant to receive MLSD lists. It contains a list of
  // JGlobus Buffers (byte buffers with offsets) that it reads
  // through sequentially using a BufferedReader to read lines
  // and parse data returned by FTP and GridFTP MLSD commands.


  // A combined sink/source for file I/O.
  static class FileMap implements DataSink, DataSource {
    RandomAccessFile file;
    long rem, total, base;

    public FileMap(String path, long off, long len) throws IOException {
      file = new RandomAccessFile(path, "rw");
      base = off;
      if (off > 0) {
        file.seek(off);
      }
      if (len + off >= file.length()) {
        len = -1;
      }
      total = rem = len;
    }

    public FileMap(String path, long off) throws IOException {
      this(path, off, -1);
    }

    public FileMap(String path) throws IOException {
      this(path, 0, -1);
    }

    public void write(Buffer buffer) throws IOException {
      if (buffer.getOffset() >= 0) {
        file.seek(buffer.getOffset());
      }
      file.write(buffer.getBuffer());
    }

    public Buffer read() throws IOException {
      if (rem == 0) {
        return null;
      }
      int len = (rem > 0x3FFF || rem < 0) ? 0x3FFF : (int) rem;
      byte[] b = new byte[len];
      long off = file.getFilePointer() - base;
      len = file.read(b);
      if (len < 0) {
        return null;
      }
      if (rem > 0) {
        rem -= len;
      }
      return new Buffer(b, len, off);
    }

    public void close() throws IOException {
      file.close();
    }

    public long totalSize() throws IOException {
      return (total < 0) ? file.length() : total;
    }
  }


  static class ListSink extends Reader implements DataSink {
    private String base;
    private LinkedList<Buffer> buf_list;
    private Buffer cur_buf = null;
    private BufferedReader br;
    private int off = 0;

    public ListSink(String base) {
      this.base = base;
      buf_list = new LinkedList<Buffer>();
      br = new BufferedReader(this);
    }

    public void write(Buffer buffer) throws IOException {
      buf_list.add(buffer);
      //System.out.println(new String(buffer.getBuffer()));
    }

    public void close() throws IOException {
    }

    private Buffer nextBuf() {
      try {
        return cur_buf = buf_list.pop();
      } catch (Exception e) {
        return cur_buf = null;
      }
    }

    // Increment reader offset, getting new buffer if needed.
    private void skip(int amt) {
      off += amt;

      // See if we need a new buffer from the list.
      while (cur_buf != null && off >= cur_buf.getLength()) {
        off -= cur_buf.getLength();
        nextBuf();
      }
    }

    // Read some bytes from the reader into a char array.
    public int read(char[] cbuf, int co, int cl) throws IOException {
      if (cur_buf == null && nextBuf() == null) {
        return -1;
      }

      byte[] bbuf = cur_buf.getBuffer();
      int bl = bbuf.length - off;
      int len = (bl < cl) ? bl : cl;

      for (int i = 0; i < len; i++)
        cbuf[co + i] = (char) bbuf[off + i];

      skip(len);

      // If we can write more, write more.
      if (len < cl && cur_buf != null) {
        len += read(cbuf, co + len, cl - len);
      }

      return len;
    }

    // Read a line, updating offset.
    private String readLine() {
      try {
        return br.readLine();
      } catch (Exception e) {
        return null;
      }
    }

    // Get the list from the sink as an XferList.
    public XferList getList(String path) {
      XferList xl = new XferList(base, "");
      String line;

      // Read lines from the buffer list.
      while ((line = readLine()) != null) {
        try {
          org.globus.ftp.MlsxEntry m = new org.globus.ftp.MlsxEntry(line);

          String fileName = m.getFileName();
          String type = m.get("type");
          String size = m.get("size");

          if (type.equals(org.globus.ftp.MlsxEntry.TYPE_FILE)) {
            xl.add(path + fileName, Long.parseLong(size));
          } else if (!fileName.equals(".") && !fileName.equals("..")) {
            xl.add(path + fileName);
          }
        } catch (Exception e) {
          e.printStackTrace();
          continue;  // Weird data I guess!
        }
      }
      return xl;
    }
  }


  static class Block {
    long off, len;
    int para = 0, pipe = 0, conc = 0;
    double tp = 0;  // Throughput - filled out by caller

    Block(long o, long l) {
      off = o;
      len = l;
    }

    public String toString() {
      return String.format("<off=%d, len=%d | sc=%d, tp=%.2f>", off, len, para, tp);
    }
  }


  static class ControlChannel {
    public final boolean local, gridftp;
    public final FTPServerFacade facade;
    public final FTPControlChannel ftpControlChannel;
    public final BasicClientControlChannel basicClientControlChannel;
    GSSCredential cred;
    public int channelID;
    Writer instantThroughputWriter;
    boolean isAborted = false;


    public ControlChannel(FTPURI u) throws Exception {
      if (u.file) {
        throw new Error("making remote connection to invalid URL");
      }
      local = false;
      facade = null;
      gridftp = u.gridftp;
      if (u.gridftp) {
        GridFTPControlChannel gc;
        basicClientControlChannel = ftpControlChannel = gc = new GridFTPControlChannel(u.host, u.port);
        gc.open();
        if (u.cred != null) {
          cred = u.cred;
          try {
            gc.authenticate(u.cred);
          } catch (Exception e) {
            System.out.println("Error in connecting host " + u.host);
            e.printStackTrace();
          }
        } else {
          String user = (u.user == null) ? "anonymous" : u.user;
          String pass = (u.pass == null) ? "" : u.pass;
          Reply r = exchange("USER", user);
          if (Reply.isPositiveIntermediate(r)) {
            try {
              execute("PASS", u.pass);
            } catch (Exception e) {
              throw new Exception("bad password");
            }
          } else if (!Reply.isPositiveCompletion(r)) {
            throw new Exception("bad username");
          }
        }
        exchange("SITE CLIENTINFO appname=" + MODULE_NAME +
            ";appver=" + MODULE_VERSION + ";schema=gsiftp;");

        //server.run();
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        basicClientControlChannel = ftpControlChannel = new FTPControlChannel(u.host, u.port);
        ftpControlChannel.open();

        Reply r = exchange("USER", user);
        if (Reply.isPositiveIntermediate(r)) {
          try {
            execute("PASS", u.pass);
          } catch (Exception e) {
            throw new Exception("bad password");
          }
        } else if (!Reply.isPositiveCompletion(r)) {
          throw new Exception("bad username");
        }
      }
    }

    // Make a local control channelPair connection to a remote control channelPair.
    public ControlChannel(ControlChannel rc) throws Exception {
      if (rc.local) {
        throw new Error("making local facade for local channelPair");
      }
      local = true;
      gridftp = rc.gridftp;
      if (gridftp) {
        facade = new GridFTPServerFacade((GridFTPControlChannel) rc.ftpControlChannel);
        ((GridFTPServerFacade) facade).setCredential(rc.cred);
        //((GridFTPServerFacade) facade).setDataChannelAuthentication(DataChannelAuthentication.NONE);
      } else {
        facade = new FTPServerFacade(rc.ftpControlChannel);
      }
      basicClientControlChannel = facade.getControlChannel();
      ftpControlChannel = null;
    }

    // Dumb thing to convert mode/type chars into JGlobus mode ints...
    private static int modeIntValue(char m) throws Exception {
      switch (m) {
        case 'E':
          return GridFTPSession.MODE_EBLOCK;
        case 'B':
          return GridFTPSession.MODE_BLOCK;
        case 'S':
          return GridFTPSession.MODE_STREAM;
        default:
          throw new Error("bad mode: " + m);
      }
    }

    private static int typeIntValue(char t) throws Exception {
      switch (t) {
        case 'A':
          return Session.TYPE_ASCII;
        case 'I':
          return Session.TYPE_IMAGE;
        default:
          throw new Error("bad type: " + t);
      }
    }

    // Change the mode of this channelPair.
    public void mode(char m) throws Exception {
      if (local) {
        facade.setTransferMode(modeIntValue(m));
      } else {
        execute("MODE", m);
      }
    }

    // Change the data type of this channelPair.
    public void type(char t) throws Exception {
      if (local) {
        facade.setTransferType(typeIntValue(t));
      } else {
        execute("TYPE", t);
      }
    }

    // Pipe a command whose reply will be read later.
    public void write(Object... args) throws Exception {
      if (local) {
        return;
      }
      ftpControlChannel.write(new Command(StorkUtil.join(args)));
    }

    // Read the reply of a piped command.
    public Reply read() throws FTPReplyParseException, ServerException, IOException {
      Reply r;
      try {
        r = basicClientControlChannel.read();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw e;
      }
      return r;
    }

    // Execute command, but don't throw on negative reply.
    public Reply exchange(Object... args) throws Exception {
      if (local) {
        return null;
      }
      return ftpControlChannel.exchange(new Command(StorkUtil.join(args)));
    }

    // Execute command, but DO throw on negative reply.
    public Reply execute(Object... args) throws Exception {
      if (local) {
        return null;
      }
      try {
        return ftpControlChannel.execute(new Command(StorkUtil.join(args)));
      } catch (Exception e) {
        // TODO: handle exception
        e.printStackTrace();
        return null;
      }
    }

    // Close the control channels in the chain.
    public void close() throws Exception {
      if (local) {
        facade.close();
      } else {
        write("QUIT");
      }
    }

    public void abort() throws Exception {
      if (local) {
        facade.abort();
      } else {
        write("ABOR");
      }
      isAborted = true;
    }
  }

  // Class for binding a pair of control channels and performing pairwise
  // operations on them.
  public static class ChannelPair{
    //public final FTPURI su, du;
    public final boolean gridftp;
    // File list this channelPair is transferring
    public FileCluster chunk, newChunk = null;
    public boolean isConfigurationChanged = false;
    public boolean enableCheckSum = false;
    Queue<XferList.MlsxEntry> inTransitFiles = new LinkedList<>();
    private int parallelism = 1, pipelining = 0, trev = 5;
    private char mode = 'S', type = 'A';
    private boolean dataChannelReady = false;
    private int id;
    private boolean stripingEnabled = false;
    long dataTransferred = 0,  lastDataTransferred = 0;

    FTPURI su, du;
    // Remote/other view of control channels.
    // rc is always remote, oc can be either remote or local.
    ControlChannel rc, oc;
    // Source/dest view of control channels.
    // Either one of these may be local (but not both).
    ControlChannel sc, dc;

    boolean isAborted = false;

    // Create a control channelPair pair. TODO: Check if they can talk.
    public ChannelPair(FTPURI su, FTPURI du) throws Exception {
      this.su = su; this.du = du;
      gridftp = !su.ftp && !du.ftp;
      if (su == null || du == null) {
        throw new Error("ChannelPair called with null args");
      }
      if (su.file && du.file) {
        throw new Exception("file-to-file not supported");
      } else if (su.file) {
        rc = dc = new ControlChannel(du);
        oc = sc = new ControlChannel(rc);
      } else if (du.file) {
        rc = sc = new ControlChannel(su);
        oc = dc = new ControlChannel(rc);
      } else {
        rc = dc = new ControlChannel(du);
        oc = sc = new ControlChannel(su);
      }
    }

    // Pair a channelPair with a new local channelPair. Note: don't duplicate().
    public ChannelPair(ControlChannel cc) throws Exception {
      if (cc.local) {
        throw new Error("cannot create local pair for local channelPair");
      }
      //du = null; su = null;
      gridftp = cc.gridftp;
      rc = dc = cc;
      oc = sc = new ControlChannel(cc);
    }

    public void setID (int id) {
      this.id = id;
      sc.channelID = dc.channelID = rc.channelID = oc.channelID = id;
    }

    public void pipePassive() throws Exception {
      rc.write(rc.ftpControlChannel.isIPv6() ? "EPSV" : "PASV");
    }

    // Read and handle the response of a pipelined PASV.
    public HostPort getPasvReply() {
      Reply r = null;
        try {
          r = rc.read();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      String s = r.getMessage().split("[()]")[1];
      return new HostPort(s);
    }

    public HostPort setPassive() throws Exception {
      pipePassive();
      return getPasvReply();
    }

    // Put the other channelPair into active mode.
    void setActive(HostPort hp) throws Exception {
      if (oc.local) {
        //oc.facade
        oc.facade.setActive(hp);
      } else if (oc.ftpControlChannel.isIPv6()) {
        oc.execute("EPRT", hp.toFtpCmdArgument());
      } else {
        oc.execute("PORT", hp.toFtpCmdArgument());
      }
      dataChannelReady = true;
    }

    public HostPortList setStripedPassive()
        throws IOException,
        ServerException {
      // rc.write(rc.ftpControlChannel.isIPv6() ? "EPSV" : "PASV");
      Command cmd = new Command("SPAS",
          (rc.ftpControlChannel.isIPv6()) ? "2" : null);
      HostPortList hpl;
      Reply reply = null;

      try {
        reply = rc.execute(cmd);
      } catch (UnexpectedReplyCodeException urce) {
        throw ServerException.embedUnexpectedReplyCodeException(urce);
      } catch (FTPReplyParseException rpe) {
        throw ServerException.embedFTPReplyParseException(rpe);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      //this.gSession.serverMode = GridFTPSession.SERVER_EPAS;
      if (rc.ftpControlChannel.isIPv6()) {
        hpl = HostPortList.parseIPv6Format(reply.getMessage());
        int size = hpl.size();
        for (int i = 0; i < size; i++) {
          HostPort6 hp = (HostPort6) hpl.get(i);
          if (hp.getHost() == null) {
            hp.setVersion(HostPort6.IPv6);
            hp.setHost(rc.ftpControlChannel.getHost());
          }
        }
      } else {
        hpl = HostPortList.parseIPv4Format(reply.getMessage());
      }
      return hpl;
    }

    /**
     * 366      * Sets remote server to striped active server mode (SPOR).
     **/
    public void setStripedActive(HostPortList hpl)
        throws IOException,
        ServerException {
      Command cmd = new Command("SPOR", hpl.toFtpCmdArgument());

      try {
        oc.execute(cmd);
      } catch (UnexpectedReplyCodeException urce) {
        throw ServerException.embedUnexpectedReplyCodeException(urce);
      } catch (FTPReplyParseException rpe) {
        throw ServerException.embedFTPReplyParseException(rpe);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      //this.gSession.serverMode = GridFTPSession.SERVER_EACT;
    }

    // Set the mode and type for the pair.
    void setTypeAndMode(char t, char m) throws Exception {
      if (t > 0 && type != t) {
        type = t;
        sc.type(t);
        dc.type(t);
      }
      if (m > 0 && mode != m) {
        mode = m;
        sc.mode(m);
        dc.mode(m);
      }
    }

    // Set the parallelism for this pair.
    void setParallelism(int p) throws Exception {
      if (!rc.gridftp || parallelism == p) {
        return;
      }
      parallelism = p = (p < 1) ? 1 : p;
      sc.execute("OPTS RETR Parallelism=" + p + "," + p + "," + p + ";");
    }

    void setDataChannelAuthentication(DataChannelAuthentication type) throws Exception {
      if (!rc.gridftp) {
        return;
      }
      Command cmd = new Command("DCAU", type.toFtpCmdArgument());
      sc.execute(cmd);
      dc.execute(cmd);
    }

    public int getPipelining() {
      return pipelining;
    }

    public void setPipelining(int pipelining) {
      this.pipelining = pipelining;
    }

    // Set the parallelism for this pair.
    void setBufferSize(int bs) throws Exception {
      if (!rc.gridftp) {
        return;
      }
      bs = (bs < 1) ? 16384 : bs;
      Reply reply = sc.exchange("SITE RBUFSZ", String.valueOf(bs));
      boolean succeeded = false;
      if (Reply.isPositiveCompletion(reply)) {
        reply = dc.exchange("SITE SBUFSZ", String.valueOf(bs));
        if (Reply.isPositiveCompletion(reply)) {
          succeeded = true;
        }
      }
      if (!succeeded) {
        reply = sc.exchange("RETRBUFSIZE", String.valueOf(bs));
        if (Reply.isPositiveCompletion(reply)) {
          reply = dc.exchange("STORBUFSIZE", String.valueOf(bs));
          if (Reply.isPositiveCompletion(reply)) {
            succeeded = true;
          }
        }
      }
      if (!succeeded) {
        reply = sc.exchange("SITE RETRBUFSIZE", String.valueOf(bs));
        if (Reply.isPositiveCompletion(reply)) {
          reply = dc.exchange("SITE STORBUFSIZE", String.valueOf(bs));
          if (Reply.isPositiveCompletion(reply)) {
            succeeded = true;
          }
        }
      }
      if (!succeeded) {
        System.out.println("Buffer size set failed!");
      }
    }

    // Set event frequency for this pair.
    void setPerfFreq(int f) throws Exception {
      if (!rc.gridftp || trev == f) return;
      trev = f = (f < 1) ? 1 : f;
      rc.exchange("TREV", "PERF", f);
    }


    public boolean isDataChannelReady() {
      return dataChannelReady;
    }

    public void setDataChannelReady(boolean dataChannelReady) {
      this.dataChannelReady = dataChannelReady;
    }


    public boolean isStripingEnabled() {
      return stripingEnabled;
    }

    public void setStripingEnabled(boolean stripingEnabled) {
      this.stripingEnabled = stripingEnabled;
    }

    // Make a directory on the destination.
    void pipeMkdir(String path) throws Exception {
      if (dc.local) {
        new File(path).mkdir();
      } else {
        dc.write("MKD", path);
      }
    }

    // Prepare the channels to transfer an XferEntry.
    void pipeTransfer(XferList.MlsxEntry e) {
      try {
        if (e.dir) {
          pipeMkdir(e.dpath());
        } else {
          // Reset port if we are using FTP in stream mode, otherwise channelPair will be closed after first file transfer.
          /*if (!gridftp && mode == 'S') {
            HostPort hp = setPassive();
            setActive(hp);
          }*/
          String checksum = null;
          if (enableCheckSum) {
            checksum = pipeGetCheckSum(e.fullPath());
          }
          //System.out.println("Piping file: " + e.dpath() + " offset: " + e.off + " len:" + e.len);
          pipeRetr(e.fullPath(), e.off, e.len);
          if (enableCheckSum && checksum != null)
            pipeStorCheckSum(checksum);
          pipeStor(e.dpath(), e.off, e.len);
        }
      } catch (Exception err) {
        err.printStackTrace();
      }
    }

    // Prepare the source to retrieve a file.
    // FIXME: Check for ERET/REST support.
    void pipeRetr(String path, long off, long len) throws Exception {
      if (sc.local) {
        sc.facade.retrieve(new FileMap(path, off, len));
      } else if (len > -1) {
        sc.write("ERET", "P", off, len, path);
      } else {
        if (off > 0) {
          sc.write("REST", off);
        }
        sc.write("RETR", path);
      }
    }

    // Prepare the destination to store a file.
    // FIXME: Check for ESTO/REST support.
    void pipeStor(String path, long off, long len) throws Exception {
      if (dc.local) {
        dc.facade.store(new FileMap(path, off, len));
      } else if (len > -1) {
        dc.write("ESTO", "A", off, path);
      } else {
        if (off > 0) {
          dc.write("REST", off);
        }
        dc.write("STOR", path);
      }
    }

    String pipeGetCheckSum(String path) throws Exception {
      String parameters = String.format("MD5 %d %d %s", 0,-1,path);
      Reply r = sc.exchange("CKSM", parameters);
      if (!Reply.isPositiveCompletion(r)) {
        throw new Exception("Error:" + r.getMessage());
      }
      return r.getMessage();
    }


    void pipeStorCheckSum(String checksum) throws Exception {
      String parameters = String.format("MD5 %s", checksum);
      Reply cksumReply = dc.exchange("SCKS", parameters);
      if( !Reply.isPositiveCompletion(cksumReply) ) {
        throw new ServerException(ServerException.SERVER_REFUSED,
            cksumReply.getMessage());
      }
      return;
    }

    // Watch a transfer as it takes place, intercepting status messages
    // and reporting any errors. Use this for pipelined transfers.
    // TODO: I'm sure this can be done better...
    void watchTransfer(ProgressListener p, XferList.MlsxEntry e) throws Exception {
      MonitorThread rmt, omt;

      rmt = new MonitorThread(rc, e);
      omt = new MonitorThread(oc, e);

      rmt.pair(omt);
      if (p != null) {
        rmt.pl = p;
        rmt.fileList = chunk.getRecords();
      }

      omt.start();
      rmt.run();
      omt.join();
      if (omt.error != null) {
        throw omt.error;
      }
      if (rmt.error != null) {
        throw rmt.error;
      }
    }

    public void close() {
      try {
        sc.close();
        dc.close();
      } catch (Exception e) { e.printStackTrace(); }
    }

    public void abort() {
      try {
        sc.abort();
        dc.abort();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    public int getId () {
      return id;
    }

    @Override
    public String toString() {
      return String.valueOf(id);
    }

  }

  static class MonitorThread extends Thread {
    public TransferProgress progress = null;
    public Exception error = null;
    ProgressListener pl = null;
    XferList fileList;
    XferList.MlsxEntry e;
    private ControlChannel cc;
    private MonitorThread other = this;

    public MonitorThread(ControlChannel cc, MlsxEntry e) {
      this.cc = cc;
      this.e  = e;
    }

    public void pair(MonitorThread other) {
      this.other = other;
      other.other = this;
    }

    public void process() throws Exception {
      //if (basicClientControlChannel.isAborted)
      //  return;

        if (progress == null) {
          progress = new TransferProgress();
        }

        //ProgressListener pl = new ProgressListener(progress);

        if (other.error != null) {
          throw other.error;
        }
        while (true) {
          try {
            Reply r = cc.read();
            if (r != null) {
              if (Reply.isPositiveIntermediate(r)) {
                r = cc.read();
              }
              if (!Reply.isPositivePreliminary(r) && !Reply.isPositiveCompletion(r)) {
                error = new Exception("failed to start " + r.getCode() + ":" + r.getCategory() + ":" + r.getMessage());
              }
            }
            while (other.error == null) {
              if (cc.isAborted)
                return;
              r = cc.read();
              if (r != null) {
                //if (r.getCode() != 112 && basicClientControlChannel.ftpControlChannel != null)
                //  System.out.println("Reply from " + basicClientControlChannel.ftpControlChannel.getHost() + " \t"  + r.getCode() + "\t" + r.getMessage());
                switch (r.getCode()) {
                  case 111:  // Restart marker
                    break;   // Just ignore for now...
                  case 112:  // Progress marker
                    if (pl != null) {
                      try {
                        long diff = pl._markerArrived(new PerfMarker(r.getMessage()), e);
                        fileList.updateTransferredSize(diff);
                      } catch (Exception e) {
                        e.printStackTrace();
                      }
                    }
                    break;
                  case 125:  // Transfer complete!
                    break;
                  case 226:  // Transfer complete!
                    return;
                  case 227:  // Entering passive mode
                    return;
                  default:
                    if (cc.ftpControlChannel != null)
                      System.out.println("Error:" + cc.ftpControlChannel.getHost() + " id:" + cc.channelID);
                    if (!cc.isAborted)
                      throw new Exception(" unexpected reply: " + r.getCode() + " " + r.getMessage());
                }   // We'd have returned otherwise...
              }
            }
          } catch(SocketTimeoutException e){
            System.out.println("Channel " + cc.channelID + " raised SocketTimeoutException");
            return;
          } catch(Exception e){
            if (cc.isAborted)
              return;
            System.out.println("Channel " + cc.channelID + " raised " + e.getMessage());
            throw e;
          }
      }
    }

    public void run() {
      try {
        process();
      } catch (Exception e) {
        error = e;
        System.out.println("Channel read exception");
        e.printStackTrace();
        System.exit(-1);
      }
    }
  }

  // Listens for markers from GridFTP servers and updates transfer
  // progress statistics accordingly.
  static class ProgressListener implements MarkerListener {
    long last_bytes = 0;
    TransferProgress prog;

    public ProgressListener(TransferProgress prog) {
      this.prog = prog;
    }

    public ProgressListener() {
    }

    // When we've received a marker from the server.
    public void markerArrived(Marker m) {
      if (m instanceof PerfMarker) {
        try {
          PerfMarker pm = (PerfMarker) m;
          long cur_bytes = pm.getStripeBytesTransferred();
          long diff = cur_bytes - last_bytes;

          last_bytes = cur_bytes;
          if (prog != null) {
            prog.done(diff);
          }
        } catch (Exception e) {
          // Couldn't get bytes transferred...
        }
      }
    }

    public long _markerArrived(Marker m, XferList.MlsxEntry mlsxEntry) {
      if (m instanceof PerfMarker) {
        try {
          PerfMarker pm = (PerfMarker) m;
          long cur_bytes = pm.getStripeBytesTransferred();
          long diff = cur_bytes - last_bytes;
          //System.out.println("Progress update from " + hostName + " file :" + mlsxEntry.spath + "\t"  +
          //        Utils.printSize(cur_bytes, true) + "/" +
          //        Utils.printSize(mlsxEntry.size, true));
          mlsxEntry.interruptedOffset = cur_bytes;
          last_bytes = cur_bytes;
          if (prog != null) {
            prog.done(diff);
          }
          return diff;
        } catch (Exception e) {
          // Couldn't get bytes transferred...
          e.printStackTrace();
          return -1;
        }
      }
      return -1;
    }
  }

}