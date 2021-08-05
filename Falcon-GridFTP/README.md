### Modified from following Repository: https://github.com/earslan58/JGlobus

*************

## Installation

### Requirements
* Linux
* Java 1.8
* Python 3.5 and up
* mvn


## Usage

1. cd into Falcon-GridFTP and run `mvn compile; mvn install` if you face test failures, add `-DskipTests` option to commands
2. run `mvn compile` 
3. cd into src/main/python and run `python3 socket_gradient.py`
4. Add a configuration file (config.cfg) in src/main/resources/ and edit as described below. See the sample config file in  src/main/resources/sample_config.cfg
5. Run `mvn exec:java` to run the code

## Configuration File
  **-s** $Source_GridFTP_Server  
  **-d** $Destination_GridFTP_Server  
  **-proxy** $Proxy_file_path (Default will try to read from /tmp for running user id)  
  **-cc** $maximum_allowed_concurrency (HARP Specific, does not have impact on Falcon)  
  **-rtt** $rtt (round trip time between source and destination [HARP Only]) in ms
  **-bw** $bw (Maximum bandwidth between source and destination [HARP Only]) in Gbps
  **-bs** $buffer_size (TCP buffer size of minimum of source's read and destination's write in MB)
  **-model** $optimization model (put: gradient [Future work]. it connects to the local server for parameters update. Current implementation uses whatever algorithms runinng on that server.)   
  **[-single-chunk]** (Will use Single Chunk [SC](http://dl.acm.org/citation.cfm?id=2529904) approach to schedule transfer. Will transfer one chunk at a time)  
  **[-useHysterisis]** (Will use historical data to run modelling and estimate transfer parameters. [HARP]. Requires python to be installed with scipy and sklearn packages)  
  **[-use-dynamic-scheduling]** (Provides dynamic channel reallocation between chunks while transfer is running [ProMC](http://dl.acm.org/citation.cfm?id=2529904))  
  **[-use-online-tuning]** (Provides continous tuning capability to historical data based modeling [HARP](https://ieeexplore.ieee.org/abstract/document/8249824))  
