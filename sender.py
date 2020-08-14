import socket
import os
import numpy as np
import time
import warnings
import logging as log
import multiprocessing as mp
from threading import Thread
from config import configurations
from search import bayes_opt, random_opt, probe_test_config

warnings.filterwarnings("ignore", category=FutureWarning)
configurations["cpu_count"] = mp.cpu_count()
configurations["thread_limit"] = configurations['limits']["thread"]
configurations["chunk_limit"] = configurations['limits']["bsize"]
    
if configurations["thread_limit"] == -1:
    configurations["thread_limit"] = configurations["cpu_count"]
    
if configurations["chunk_limit"] == -1:
    configurations["chunk_limit"] = 21


FORMAT = '%(asctime)s -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    log.basicConfig(format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=log.DEBUG)
    mp.log_to_stderr(log.DEBUG)
else:
    log.basicConfig(format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=log.INFO)

emulab_test = False
root = configurations["data_dir"]["sender"]
probing_time = configurations["probing_sec"]
file_names = os.listdir(root) * configurations["multiplier"]
probe_again = False

buffer_size = mp.Value("i", 0)
num_workers = mp.Value("i", 0)
sample_phase = mp.Value("i", 0)
kill_transfer = mp.Value("i", 0)
process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
transfer_status = mp.Array("i", [0 for i in range(len(file_names))])
file_offsets = mp.Array("d", [0.0 for i in range(len(file_names))])

HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]


def worker(indx):
    while kill_transfer.value == 0:
        if process_status[indx] == 0:
            if (len(transfer_status) == np.sum(transfer_status)):
                kill_transfer.value = 1
        else:
            start = time.time()
            
            try:
                sock = socket.socket()
                sock.connect((HOST, PORT))
                
                if emulab_test:
                    max_speed = (50 * 1024 * 1024)/8 # 50k * 1024 = bytes
                    data_count = 0
                    time_next = time.time() + 1

                if sample_phase.value == 0:
                    log.info((indx, len(file_names), num_workers.value,))
                    
                for i in range(indx, len(file_names), num_workers.value):
                    duration = time.time() - start
                    if (sample_phase.value == 1 and (duration > probing_time)) or (process_status[indx] == 0):
                        break
                    
                    if transfer_status[i] == 0:
                        filename = root + file_names[i]
                        file = open(filename, "rb")
                        offset = file_offsets[i]

                        log.debug("starting {0}, {1}, {2}".format(indx, i, filename))
                        while process_status[indx] == 1:
                            sent = sock.sendfile(file=file, offset=int(offset), count=buffer_size.value)
                            # data = os.preadv(file,buffer_size.value,offset)
                            offset += sent
                            file_offsets[i] = offset
                            
                            if emulab_test:
                                data_count += sent
                                if data_count >= max_speed:
                                    data_count = 0
                                    
                                    sleep_for = time_next - time.time()
                                    if sleep_for > 0:
                                        time.sleep(sleep_for)
                                    
                                    time_next = time.time() + 1
                            
                            duration = time.time() - start
                            if (sample_phase.value == 1 and (duration > probing_time)) or (process_status[indx] == 0):
                                if sent == 0:
                                    transfer_status[i] = 1
                                    log.debug("finished {0}, {1}, {2}".format(indx, i, filename))
                                    
                                break
                            
                            if sent == 0:
                                transfer_status[i] = 1
                                log.debug("finished {0}, {1}, {2}".format(indx, i, filename)) 
                                break
                
                process_status[indx] = 0
                sock.close()
            except Exception as e:
                process_status[indx] = 0
                log.error(str(e))
    
    process_status[indx] == 0
    return True 


def get_buffer_size(unit):
    return (2 ** (unit-1)) * 1024


def get_retransmitted_packet_count():
    try:        
        data = os.popen("netstat -s | grep segments").read().split()
        return int(data[3]), int(data[7])
    except:
        return -1, -1
    

def sample_transfer(params):
    if kill_transfer.value == 1:
        return 10 ** 10
        
    start_time = time.time()
    score_before = np.sum(file_offsets)
    num_workers.value = params[0]
    buffer_size.value = get_buffer_size(params[1])
    log.info("Current Probing Parameters: {0}".format(params))
    
    before_sc, before_rc = get_retransmitted_packet_count()
    if len(file_names) < num_workers.value:
        num_workers.value = len(file_names)
    
    for i in range(num_workers.value):
        process_status[i] = 1
        
    while np.sum(process_status)>0:
        if (time.time()-start_time) > probing_time:
            for i in range(num_workers.value):
                process_status[i] = 0
            
        time.sleep(0.01)
    
    after_sc, after_rc = get_retransmitted_packet_count()
    sc, rc = after_sc - before_sc, after_rc - before_rc
    
    score_after = np.sum(file_offsets)
    score = score_after - score_before
    duration = time.time() - start_time         
    thrpt = (score * 8) / (duration*1024*1024)
    log.info("Throughput: {0}, Packet Retransmitted: {2}".format(np.round(thrpt), sc, rc))
        
    lr, C = 0, 10
    if sc != 0:
        lr = rc/sc if sc>rc else 0.99
        
    if rc < 128:
        rc = 128
    
    score_value = thrpt / np.log2(rc)
    # 2 * np.log10(thrpt) - np.log10(rc)
    # thrpt / np.log2(rc) 
    # thrpt * (1 - C * ((1/(1-lr))-1)) 
    # thrpt * (1 + (configurations["thread_limit"] - num_workers.value)/(2*configurations["thread_limit"]))
    
    return np.round(score_value * (-1), 4)


def normal_transfer(params):
    global probe_again
    num_workers.value = params[0]
    buffer_size.value = get_buffer_size(params[1])
    log.info("Normal Transfer -- Probing Parameters: {0}".format(params))
    
    files_left = len(transfer_status) - np.sum(transfer_status)
    if files_left < num_workers.value:
        num_workers.value = files_left

    for i in range(num_workers.value):
        process_status[i] = 1
    
    while np.sum(process_status) > 0 and kill_transfer.value == 0:
        time.sleep(0.01)
                
    # if probe_again and (len(transfer_status) > np.sum(transfer_status)):
    #     run_transfer()

    
def run_transfer():
    global probe_again
    sample_phase.value = 1
    probe_again = False
    
    if configurations["method"].lower() == "random":
        params = random_opt(configurations, sample_transfer, log)
    
    elif configurations["method"].lower() == "probe":
        params = [configurations["probe_config"]["thread"], configurations["probe_config"]["bsize"]]
        
    else:
        params = bayes_opt(configurations, sample_transfer, log)
    
    sample_phase.value = 0
    
    if kill_transfer.value == 0:
        normal_transfer(params)
    
    
def report_retransmission_count(start_time):
    previous_sc, previous_rc = get_retransmitted_packet_count()
    previous_time = 0
    
    time.sleep(1)
    while (len(transfer_status) > sum(transfer_status)) and (kill_transfer.value == 0):
        curr_time = time.time()
        time_sec = np.round(curr_time-start_time)
        after_sc, after_rc = get_retransmitted_packet_count()
        curr_rc = after_rc - previous_rc
        previous_time, previous_sc, previous_rc = time_sec, after_sc, after_rc
        log.info("Retransmission Count @{0}s: {1}".format(time_sec, curr_rc))
        time.sleep(0.999)


def report_throughput(start_time):
    global probe_again
    previous_total = 0
    previous_time = 0
    throughput_logs = []
    max_mean_thrpt = 0
    sampling_ended = 0 
    
    time.sleep(1)
    while (len(transfer_status) > sum(transfer_status)) and (kill_transfer.value == 0):
        curr_time = time.time()
        time_sec = np.round(curr_time-start_time)
        total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
        thrpt = np.round((total*8*1024)/time_sec,2)
        
        curr_total = total - previous_total
        curr_time_sec = time_sec - previous_time
        curr_thrpt = np.round((curr_total*8*1024)/curr_time_sec)
        previous_time, previous_total = time_sec, total
        throughput_logs.append(curr_thrpt)
        log.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(time_sec, curr_thrpt, thrpt))

        if sample_phase.value == 0:
            if sampling_ended == 0:
                sampling_ended = time_sec
            
            if (time_sec - sampling_ended > 10) and (np.mean(throughput_logs[-10:]) < 1.0):
                log.info("Alas! Transfer is Stuck. Killing it!")
                kill_transfer.value = 1
                
            if configurations["multiple_probe"] and time_sec - sampling_ended > 10:
                max_mean_thrpt = max(max_mean_thrpt, np.mean(throughput_logs[-10:]))
                lower_limit = max_mean_thrpt * configurations["probing_threshold"]
                # print(np.mean(throughput_logs[-10:]), max_mean_thrpt)
                if np.mean(throughput_logs[-10:]) < lower_limit:
                    log.info("It Seems We Need to Probe Again!")
                    probe_again = True
        else:
            max_mean_thrpt = 0
            sampling_ended = 0 
                
        time.sleep(0.998)


workers = [mp.Process(target=worker, 
                      args=(i,)) for i in range(configurations["thread_limit"])]


if __name__ == '__main__':
    for p in workers:
        p.daemon = True
        p.start()
    
    start = time.time()
    throughput_thread = Thread(target=report_throughput, args=(start,), daemon=True)
    throughput_thread.start()
    
    if configurations["loglevel"] == "debug":
        retransmission_thread = Thread(target=report_retransmission_count, args=(start,), daemon=True)
        
    run_transfer()
    end = time.time()
            
    time_sec = np.round(end-start, 3)
    total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
    thrpt = np.round((total*8*1024)/time_sec,2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(total, time_sec, thrpt))
    
    for p in workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)
