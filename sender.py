import socket
import os
import numpy as np
import time
import warnings
import logging as log
from sendfile import sendfile
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from config import configurations
from search import bayes_opt, random_opt, probe_test_config

warnings.filterwarnings("ignore", category=FutureWarning)
configurations["cpu_count"] = mp.cpu_count()


FORMAT = '%(asctime)s -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    log.basicConfig(format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=log.DEBUG)
    mp.log_to_stderr(log.DEBUG)
else:
    log.basicConfig(format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=log.INFO)


root = configurations["data_dir"]["sender"]
probing_time = configurations["probing_sec"]
files_name = os.listdir(root) * configurations["multiplier"]

score = mp.Value("d", 0.0)
process_done = mp.Value("i", 0)
transfer_status = mp.Array("i", [0 for i in range(len(files_name))])
file_offsets = mp.Array("d", [0.0 for i in range(len(files_name))])
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]


def worker(buffer_size, indx, num_workers, sample_transfer):
    start = time.time()
    sock = socket.socket()
    sock.connect((HOST, PORT))
    
    for i in range(indx, len(files_name), num_workers):
        duration = time.time() - start
        if sample_transfer and (duration > probing_time):
            break
        
        if transfer_status[i] == 0:
            filename = root + files_name[i]
            file = open(filename, "rb")
            offset = file_offsets[i]

            log.debug("sending {u}".format(u=filename))
            total_sent = 0
            while True:
                sent = sendfile(sock.fileno(), file.fileno(), offset, buffer_size)
                offset += sent
                total_sent += sent
                
                duration = time.time() - start
                if sample_transfer and (duration > probing_time):
                    score.value = score.value + (total_sent/duration)
                    
                    if sent == 0:
                        transfer_status[i] = 1
                        log.debug("finished {u}".format(u=filename))
                        
                    break
                
                if sent == 0:
                    transfer_status[i] = 1
                    log.debug("finished {u}".format(u=filename)) 
                    break
                
            file_offsets[i] = offset
    
    process_done.value = process_done.value + 1
    sock.close()
    return True 


def get_buffer_size(unit):
    return (2 ** (unit-1)) * 1024


def get_retransmitted_packet_count():
    try:
        data = os.popen("netstat -s | grep retransmitted").read().split()
        return int(data[0])
    except:
        return -1
    

def do_transfer(params, sample_transfer=True):
    score.value = 0.0
    process_done.value = 0
    num_workers = params[0]
    buffer_size = get_buffer_size(params[1])
    log.info("Current Parameters: {0}".format(params))
    before_rc = get_retransmitted_packet_count()
    
    if len(files_name) < num_workers:
        num_workers = len(files_name)

    # workers = [mp.Process(target=worker, args=(buffer_size,i,num_workers, sample_transfer)) for i in range(num_workers)]
    # for p in workers:
    #     p.daemon = True
    #     p.start()
        
    for i in range(num_workers):
        thread_pool.submit(worker, buffer_size, i, num_workers, sample_transfer,)
    
    while process_done.value < num_workers:
            time.sleep(0.01)

    after_rc = get_retransmitted_packet_count()
    rt_count = after_rc - before_rc
    thrpt = score.value / (1024*1024*(1/8))
    log.info("Throughput: {0}, Packet Retransmitted: {1}".format(np.round(thrpt), rt_count))
    
    if rt_count == 0:
        rt_count = 1
    
    if sample_transfer:
        score.value = np.log2(thrpt) / np.log10(rt_count) if np.log10(rt_count) > 0 else np.log2(thrpt)
        return np.round(score.value * (-1), 4)


thread_pool = ThreadPoolExecutor(configurations["cpu_count"] * 2)


if __name__ == '__main__':
    start = time.time()
    
    if configurations["method"].lower() == "random":
        random_opt(do_transfer)
    
    elif configurations["method"].lower() == "probe":
        params = [configurations["probe_config"]["thread"], configurations["probe_config"]["bsize"]]
        probe_test_config(do_transfer, params)
    else:
        bayes_opt(configurations, do_transfer, log)
    
    end = time.time()
    time_sec = np.round(end-start, 3)
    total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
    thrpt = np.round((total*8*1024)/time_sec,2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(total, time_sec, thrpt))
        