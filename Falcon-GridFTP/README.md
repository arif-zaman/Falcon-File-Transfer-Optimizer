#### Modified from following Repository: https://github.com/earslan58/JGlobus

*************

## Installation

### Requirements
* Linux
* Java 1.8
* Python 3.5 and up
* mvn


## Usage

1. For local data transfer nodes: install globus toolkits (https://gridcf.org/gct-docs/latest/admin/install/); then spwan gridftp server on both sender and receiver hosts. For example:
     #### sender: 
     `globus-gridftp-server -aa -p <source_port> -data-interface <source_ip> &`
     #### receiver:
     `globus-gridftp-server -aa -p <dest_port> -data-interface <dest_ip> &`

2. edit configuration file (Falcon-GridFTP/AdaptiveGridFTPClient/src/main/resources/config.cfg):
 
    #### sender: Use gsiftp// for gsi authenticated endpoints and ftp:// for the rest
    `-s ftp://<source_ip>:<source_port>/path/to/transfer/dataset` or `-s gsiftp://source-ip:source-port/path/to/transfer/dataset`
    #### receiver: Use gsiftp// for gsi authenticated endpoints and ftp:// for the rest
    `-d ftp://<dest_ip>:<dest_port>/path/to/transfer/dataset` or `-d gsiftp://destination-ip:destination-port/path/to/transfer/dataset`
    #### RTT between sender and receiver (milliseconds), below one is for 1 ms RTT
    `-rtt 1`
    #### Bandwidth in Gbps, below one is for 40Gbps network bandwidth
    `-bandwidth 40`
    #### Buffer size in MB, below one is for 32 MB maximum TCP buffer size
    `-bs 32`
    #### Maximum number of concurrent threads
    `-maxcc 20`
    #### keep it as it is for gradient optimization
    `-model gradient`

3. cd into `Falcon-GridFTP/AdaptiveGridFTPClient/src/main/python/` folder; then run the optimization server: `python3 socket_gradient.py`

4. open another terminal; cd into `Falcon-GridFTP/AdaptiveGridFTPClient/` folder; then run: `mvn compile; mvn install -DskipTests`

5. if compilation is successful, then run the transfer: `mvn exec:java`
