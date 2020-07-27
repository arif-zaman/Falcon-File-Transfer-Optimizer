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
sent_till_now = mp.Value("d", 0.0)
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]


def worker(buffer_size, indx, num_workers, sample_transfer):
    start = time.time()
    sock = socket.socket()
    sock.connect((HOST, PORT))
    
    total_sent = 0
    for i in range(indx, len(files_name), num_workers):
        duration = time.time() - start
        if sample_transfer and (duration > probing_time):
            break
        
        if transfer_status[i] == 0:
            filename = root + files_name[i]
            file = open(filename, "rb")
            offset = file_offsets[i]

            log.debug("sending {u}".format(u=filename))
            while True:
                # log.info(str(sock.fileno()) + ", " + str(file.fileno()))
                sent = sendfile(sock.fileno(), file.fileno(), offset, buffer_size)
                offset += sent
                total_sent += sent
                sent_till_now.value += sent
                
                duration = time.time() - start
                if sample_transfer and (duration > probing_time):
                    if sent == 0:
                        transfer_status[i] = 1
                        log.debug("finished {u}".format(u=filename))
                        
                    break
                
                if sent == 0:
                    transfer_status[i] = 1
                    log.debug("finished {u}".format(u=filename)) 
                    break
                
            file_offsets[i] = offset
    
    score.value = score.value + (total_sent/duration)
    # log.info(duration)
    process_done.value = process_done.value + 1
    sock.close()
    return True 


def get_buffer_size(unit):
    return (2 ** (unit-1)) * 1024


def get_retransmitted_packet_count():
    try:
        # if configurations["testbed"] == "xsede":
        #     data = os.popen("netstat -s | grep retransmited").read().split()
        # else:
        #     data = os.popen("netstat -s | grep retransmitted").read().split()
        
        data = os.popen("netstat -s | grep segments").read().split()
        return int(data[3]), int(data[7])

    except:
        return -1, -1
    

def do_transfer(params, sample_transfer=True):
    start_time = time.time()
    score.value = 0.0
    process_done.value = 0
    num_workers = params[0]
    buffer_size = get_buffer_size(params[1])
    log.info("Current Probing Parameters: {0}".format(params))
    
    if sample_transfer:
        before_sc, before_rc = get_retransmitted_packet_count()
    
    if len(files_name) < num_workers:
        num_workers = len(files_name)

    workers = [mp.Process(target=worker, args=(buffer_size,i,num_workers, sample_transfer)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()
        
    # for i in range(num_workers):
    #     process_pool.apply_async(worker, args=(buffer_size, i, num_workers, sample_transfer,))
        
    # for i in range(num_workers):
    #     thread_pool.submit(worker, buffer_size, i, num_workers, sample_transfer,)
    
    duration = time.time() - start_time
    while process_done.value < num_workers:
        if sample_transfer:
            duration = time.time() - start_time
            if duration > 2*probing_time:
                log.info("Probing Taking unusually long time, EXITING! (Process Done: {0})".format(process_done.value))
                break
        
        time.sleep(0.01)
    
    if process_done.value != num_workers:
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join()
    
    if sample_transfer:
        after_sc, after_rc = get_retransmitted_packet_count()
        sc, rc = after_sc-before_sc, after_rc-before_rc
        
        lr = 0
        if sc != 0:
            lr = rc/sc if sc>rc else 0.99
            
        thrpt = score.value / (1024*1024*(1/8))
        log.info("Throughput: {0}, Packet Sent: {1}, Packet Retransmitted: {2}".format(np.round(thrpt), sc, rc))
        
        score.value = thrpt * (1 - ((1/(1-lr))-1))# 2 * np.log10(thrpt) - np.log10(rt_count)
        thread_limit = configurations['limits']["thread"]
        score.value = thrpt * (1 + (thread_limit-num_workers)/(2*thread_limit))
        return np.round(score.value * (-1), 4)


def report_retransmission_count(start_time):
    previous_sc, previous_rc = get_retransmitted_packet_count()
    previous_time = 0
    
    time.sleep(1)
    while len(transfer_status) > sum(transfer_status):
        curr_time = time.time()
        time_sec = np.round(curr_time-start_time)
        after_sc, after_rc = get_retransmitted_packet_count()
        curr_rc = after_rc - previous_rc
        previous_time, previous_sc, previous_rc = time_sec, after_sc, after_rc
        log.info("Retransmission Count @{0}s: {1}".format(time_sec, curr_rc))
        time.sleep(0.999)

def report_throughput(start_time):
    previous_total = 0
    previous_time = 0
    
    time.sleep(1)
    while len(transfer_status) > sum(transfer_status):
        curr_time = time.time()
        time_sec = np.round(curr_time-start_time)
        total = np.round(sent_till_now.value / (1024*1024*1024), 3)
        thrpt = np.round((total*8*1024)/time_sec)
        
        curr_total = total - previous_total
        curr_time_sec = time_sec - previous_time
        curr_thrpt = np.round((curr_total*8*1024)/curr_time_sec)
        previous_time, previous_total = time_sec, total
        log.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(time_sec, curr_thrpt, thrpt))
        time.sleep(0.998)


if __name__ == '__main__':
    thread_pool = ThreadPoolExecutor(2)
    
    start = time.time()
    thread_pool.submit(report_throughput, start,)
    thread_pool.submit(report_retransmission_count, start,)
    # process_pool = mp.Pool(configurations["cpu_count"])
    
    if configurations["method"].lower() == "random":
        random_opt(do_transfer)
    
    elif configurations["method"].lower() == "probe":
        params = [configurations["probe_config"]["thread"], configurations["probe_config"]["bsize"]]
        probe_test_config(do_transfer, params)
        
    else:
        bayes_opt(configurations, do_transfer, log)
    
    end = time.time()
    # process_pool.close()
    # process_pool.join()
    
    time_sec = np.round(end-start, 3)
    total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
    thrpt = np.round((total*8*1024)/time_sec,2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(total, time_sec, thrpt))
        