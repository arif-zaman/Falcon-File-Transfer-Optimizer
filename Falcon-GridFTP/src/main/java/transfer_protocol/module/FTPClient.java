package transfer_protocol.module;

import client.FileCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.ftp.HostPort;
import org.globus.ftp.vanilla.Reply;
import transfer_protocol.util.AdSink;
import transfer_protocol.util.StorkUtil;
import transfer_protocol.util.TransferProgress;
import transfer_protocol.util.XferList;

import java.util.LinkedList;
import java.util.List;

// A custom extended GridFTPClient that implements some undocumented
// operations and provides some more responsive transfer methods.
public class FTPClient{

    volatile boolean aborted = false;
    private FTPURI su, du;
    private TransferProgress progress = new TransferProgress();
    //private AdSink sink = null;
    //private FTPServerFacade local;
    private ChannelModule.ChannelPair channelPair;  // Main control channels.
    private boolean enableIntegrityVerification = false;
    public LinkedList<FileCluster> fileClusters;
    public static List<ChannelModule.ChannelPair> channelList;

    private static final Log LOG = LogFactory.getLog(FTPClient.class);

    public FTPClient(FTPURI su, FTPURI du) throws Exception {
        this.su = su;
        this.du = du;
        channelPair = new ChannelModule.ChannelPair(su, du);
        channelList = new LinkedList<>();
    }

    // Set the progress listener for this ftpClient's transfers.
    public void setAdSink(AdSink sink) {
        //this.sink = sink;
        progress.attach(sink);
    }


    public int getChannelCount() {
        return channelList.size();
    }


    void close() {
        channelPair.close();
    }

    // Recursively list directories.
    public XferList mlsr() throws Exception {
        final String MLSR = "MLSR", MLSD = "MLSD";
        final int MAXIMUM_PIPELINING = 200;
        int currentPipelining = 0;
        //String cmd = isFeatureSupported("MLSR") ? MLSR : MLSD;
        String cmd = MLSD;
        XferList list = new XferList(su.path, du.path);
        String path = list.sp;
        // Check if we need to do a local listing.
        if (channelPair.sc.local) {
            return StorkUtil.list(path);
        }
        ChannelModule.ChannelPair cc = new ChannelModule.ChannelPair(this.channelPair.sc);
        //channelPair.setDataChannelAuthentication(DataChannelAuthentication.NONE);

        LinkedList<String> dirs = new LinkedList<String>();
        dirs.add("");

        //Command cmd1 = new Command("DCAU", DataChannelAuthentication.NONE.toFtpCmdArgument());
        //channelPair.rc.execute(cmd1);
        Reply reply;
        reply = cc.rc.exchange("TYPE I");
        //System.out.println(reply.getCode() + "\t" + reply.getMessage());
        reply = cc.rc.exchange("OPTS MLST type;size;");
        //System.out.println(reply.getCode() + "\t" + reply.getMessage());
        reply = cc.rc.exchange("PBSZ 1048576;");
        //System.out.println(reply.getCode() + "\t" + reply.getMessage());
        // Keep listing and building subdirectory lists.

        // TODO: Replace with pipelining structure.
        LinkedList<String> waiting = new LinkedList<String>();
        LinkedList<String> working = new LinkedList<String>();
        while (!dirs.isEmpty() || !waiting.isEmpty()) {
            LinkedList<String> subdirs = new LinkedList<String>();

            while (!dirs.isEmpty())
                waiting.add(dirs.pop());

            // Pipeline commands like a champ.
            while  (currentPipelining < MAXIMUM_PIPELINING && !waiting.isEmpty()) {
                String p = waiting.pop();
                cc.pipePassive();
                cc.rc.write(cmd, path + p);
                working.add(p);
                currentPipelining++;
            }

            // Read the pipelined responses like a champ.
            for (String p : working) {
                ChannelModule.ListSink sink = new ChannelModule.ListSink(path);
                // Interpret the pipelined PASV command.
                try {
                    HostPort hp = cc.getPasvReply();
                    cc.setActive(hp);
                } catch (Exception e) {
                    sink.close();
                    throw new Exception("couldn't set passive mode: " + e);
                }

                // Try to get the listing, ignoring errors unless it was root.
                try {
                    cc.oc.facade.store(sink);
                    cc.watchTransfer(null, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    if (p.isEmpty()) {
                        throw new Exception("couldn't list: " + path + ": " + e);
                    }
                    continue;
                }

                XferList xl = sink.getList(p);

                // If we did mlsr, return the list.
                if (cmd == MLSR) {
                    return xl;
                }
                // Otherwise, add subdirs and repeat.
                for (XferList.MlsxEntry e : xl) {
                    if (e.dir) {
                        subdirs.add(e.spath);
                    }
                }
                list.addAll(xl);

            }
            working.clear();
            currentPipelining = 0;

            // Get ready to repeat with new subdirs.
            dirs.addAll(subdirs);
        }

        return list;
    }

    // Get the size of a file.
    public long size(String path) throws Exception {
        if (channelPair.sc.local) {
            return StorkUtil.size(path);
        }
        Reply r = channelPair.sc.exchange("SIZE", path);
        if (!Reply.isPositiveCompletion(r)) {
            throw new Exception("file does not exist: " + path);
        }
        return Long.parseLong(r.getMessage());
    }

    public void setEnableIntegrityVerification(boolean enableIntegrityVerification) {
        this.enableIntegrityVerification = enableIntegrityVerification;
    }


    // Call this to kill transfer.
    public void abort() {
        for (ChannelModule.ChannelPair cc : channelList)
            cc.abort();
        aborted = true;
    }

    // Check if we're prepared to transfer a file. This means we haven't
    // aborted and destination has been properly set.
    void checkTransfer() throws Exception {
        if (aborted) {
            throw new Exception("transfer aborted");
        }
    }


    //returns list of files to be transferred
    public XferList getListofFiles(String sp, String dp) throws Exception {
        checkTransfer();

        //checkTransfer();
        XferList xl;
        // Some quick sanity checking.
        if (sp == null || sp.isEmpty()) {
            throw new Exception("src spath is empty");
        }
        if (dp == null || dp.isEmpty()) {
            throw new Exception("dest spath is empty");
        }
        // See if we're doing a directory transfer and need to build
        // a directory list.
        if (sp.endsWith("/")) {
            xl = mlsr();
            xl.dp = dp;
        } else {  // Otherwise it's just one file.
            xl = new XferList(sp, dp, size(sp));
        }
        // Pass the list off to the transfer() which handles lists.
        xl.updateDestinationPaths();
        for (XferList.MlsxEntry entry : xl.getFileList()) {
            if (entry.dir) {
                LOG.info("Creating directory " + entry.dpath);
                channelPair.pipeMkdir(entry.dpath);
            }
        }
        return xl;
    }




    void updateOnAir(XferList fileList, int count) {
        synchronized (fileList) {
            fileList.onAir += count;
        }
    }
/*
    synchronized ChannelModule.ChannelPair findChunkInNeed(ChannelModule.ChannelPair cc) throws Exception {
        double max = -1;
        boolean found = false;

        while (!found) {
            int index = -1;
            //System.out.println("total fileClusters:"+fileClusters.size());
            for (int i = 0; i < fileClusters.size(); i++) {
          /* Conditions to pick a chunk to allocate finished channelPair
             1- Chunk is ready to be transferred (non-SingleChunk algo)
             2- Chunk has still files to be transferred
             3- Chunks has the highest estimated finish time
          */
    /*
                if (fileClusters.get(i).isReadyToTransfer && fileClusters.get(i).getRecords().count() > 0 &&
                        fileClusters.get(i).getRecords().estimatedFinishTime > max) {
                    max = fileClusters.get(i).getRecords().estimatedFinishTime;
                    index = i;
                }
            }
            // not found any chunk candidate, returns
            if (index == -1) {
                return null;
            }
            if (fileClusters.get(index).getRecords().count() > 0) {
                cc.newChunk = fileClusters.get(index);
                System.out.println("Channel  " + cc.getId() + " is being transferred from " +
                        cc.chunk.getDensity().name() + " to " + cc.newChunk.getDensity().name() +
                        "\t" + cc.newChunk.getTunableParameters().toString());
                cc = restartChannel(cc);
                System.out.println("Channel  " + cc.getId() + " is transferred current:" +
                        cc.inTransitFiles.size() + " ppq:" + cc.getPipelining());
                if (cc.inTransitFiles.size() > 0) {
                    return cc;
                }
            }
        }
        return null;
    }
*/
    public static class TransferList implements Runnable {
        XferList fileList;
        ChannelModule.ChannelPair channelPair;
        TransferList(XferList fileList, ChannelModule.ChannelPair channelPair) {
            this.fileList = fileList;
            this.channelPair = channelPair;
        }

        @Override
        public void run() {
            XferList.MlsxEntry e;
            while (true) {
                // pipe transfer commands if ppq is enabled
                if (!channelPair.isAborted){
                    for (int i = channelPair.inTransitFiles.size(); i < channelPair.getPipelining() + 1; i++) {
                        if ((e = fileList.getNextFile()) == null) {
                            continue;
                        }
                        channelPair.pipeTransfer(e);
                        channelPair.inTransitFiles.add(e);
                        fileList.updateOnAir(1);
                        System.out.println("Channel " + channelPair.getId() + " piping " + e.fileName);
                    }
                }
                if (channelPair.inTransitFiles.isEmpty()) {
                    break;
                }

                // Read responses to piped commands.
                e = channelPair.inTransitFiles.poll();
                if (e.dir) {
                    continue;
                }
                //System.out.println("Channel " + channelPair.getId() + " is monitoring file " + e.fileName);
                ChannelModule.ProgressListener prog = new ChannelModule.ProgressListener();
                try {
                    channelPair.watchTransfer(prog, e);
                } catch (Exception e1) {
                    System.out.println("Error in the tunnel");
                    e1.printStackTrace();
                }
                //System.out.println("Channel " + channelPair.getId() + " finished file  " + e.fileName);
                if (e.len == -1) {
                    //fileList.updateTransferredSize(e.size - prog.last_bytes);
                    if (e.size > prog.last_bytes) {
                        e.off = prog.last_bytes;
                        e.len = e.size - e.off;
                        synchronized (fileList) {
                            fileList.addEntry(e);
                            //System.out.println(channelPair.getId() + " adding partial file " + e.fileName + " off:" + e.off);
                        }
                    } else {
                        //System.out.println(channelPair.getId() + " completed file " + e.fileName);
                    }
                } else {
                    if (e.size > e.off + prog.last_bytes) {
                        e.off = e.off + prog.last_bytes;
                        e.len = e.size - e.off;
                        synchronized (fileList) {
                            fileList.addEntry(e);
                            //System.out.println(channelPair.getId() + " adding partial file " + e.fileName + " off:" + e.off);
                        }
                    } else {
                        //System.out.println(channelPair.getId() + " completed file " + e.fileName);
                    }

                }
                channelPair.dataTransferred += e.size;

                fileList.updateOnAir(-1);
                if(channelPair.isConfigurationChanged) {
                    channelPair = restartChannel(channelPair);
                }
                /*
                // The transfer of the channelPair's assigned fileClusters is completed.
                // Check if other fileClusters have any outstanding files. If so, help!
                if (channelPair.inTransitFiles.isEmpty()) {
                    //LOG.info(channelPair.id + "--Chunk "+ channelPair.xferListIndex + "finished " +fileClusters.get(channelPair.xferListIndex).count());
                    channelPair = findChunkInNeed(channelPair);
                    if (channelPair == null)
                        return;
                }
                */
            }
            if (channelPair == null) {
                System.out.println("Channel " + channelPair.getId() +  " is null");
            }
            else {
                channelPair.close();
                synchronized (fileList.channels) {
                    fileList.channels.remove(channelPair);
                }
                synchronized (channelList) {
                    channelList.remove(channelPair);
                }
                System.out.println("Closed data channelPair " + channelPair.getId());
            }
        }

        ChannelModule.ChannelPair restartChannel(ChannelModule.ChannelPair oldChannel) {
            System.out.println("Updating channelPair " + oldChannel.getId()+ " parallelism to " +
                    oldChannel.newChunk.getTunableParameters().getParallelism());
            XferList oldFileList = oldChannel.chunk.getRecords();
            XferList newFileList = oldChannel.newChunk.getRecords();
            synchronized (oldFileList) {
                oldFileList.channels.remove(oldChannel);
            }

            ChannelModule.ChannelPair newChannel;
            oldChannel.close();
            try {
                newChannel = new ChannelModule.ChannelPair(oldChannel.su, oldChannel.du);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            boolean success = GridFTPClient.configureChannel(newChannel, oldChannel.getId(), oldChannel.newChunk);
            if (!success)
                System.exit(-1);
            synchronized (newFileList.channels) {
                newFileList.channels.add(newChannel);
            }
            channelPair.isConfigurationChanged = false;
            return newChannel;
        }
    }
}
