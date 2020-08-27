import socket
import os
import numpy as np
import time
import warnings
import logging as log
from sendfile import sendfile
import multiprocessing as mp
from threading import Thread
from config import configurations
from search import initial_probe, repetitive_probe, random_opt, brute_force

warnings.filterwarnings("ignore", category=FutureWarning)
configurations["cpu_count"] = mp.cpu_count()
configurations["thread_limit"] = configurations['limits']["thread"]
configurations["chunk_limit"] = configurations['limits']["bsize"]
    
if configurations["thread_limit"] == -1:
    configurations["thread_limit"] = configurations["cpu_count"]
    
if configurations["chunk_limit"] == -1:
    configurations["chunk_limit"] = 21
    
configurations["thread"] = {
    "min": 1,
    "max": configurations["thread_limit"],
    "iteration": configurations["bayes"]["num_of_exp"],
    "random_probe": configurations["bayes"]["initial_run"]
}


FORMAT = '%(asctime)s -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    log.basicConfig(format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=log.DEBUG)
    mp.log_to_stderr(log.DEBUG)
else:
    log.basicConfig(format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p', level=log.INFO)

emulab_test = False
if configurations["emulab_test"] is not None:
    emulab_test = configurations["emulab_test"]
else:
    configurations["emulab_test"] = False
    
root = configurations["data_dir"]["sender"]
probing_time = configurations["probing_sec"]
file_names = os.listdir(root) * configurations["multiplier"]
probe_again = False
sample_phase_number = 0

segments_sent = mp.Value("i", 0)
segments_retransmitted = mp.Value("i", 0)
chunk_size = mp.Value("i", 0)
num_workers = mp.Value("i", 0)
timeout_count = mp.Value("i", 0)
sample_phase = mp.Value("i", 0)
kill_transfer = mp.Value("i", 0)
process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
transfer_status = mp.Array("i", [0 for i in range(len(file_names))])
file_offsets = mp.Array("d", [0.0 for i in range(len(file_names))])

HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]


def tcp_stats(addr):
    sent, retm = 0, 0
    
    try:
        data = os.popen("ss -ti").read().split("\n")

        for i in range(1,len(data)):
            if addr in data[i-1]:
                parse_data = data[i].split(" ")
                for entry in parse_data:
                    if "data_segs_out" in entry:
                        sent = int(entry.split(":")[-1])
                        
                    if "retrans" in entry:
                        retm = int(entry.split("/")[-1])
                
                break
    except:
        pass
    
    return sent, retm


def worker(indx):
    while kill_transfer.value == 0:
        if process_status[indx] == 0:
            if (len(transfer_status) == np.sum(transfer_status)):
                kill_transfer.value = 1
        else:
            start = time.time()
            
            try:
                sock = socket.socket()
                sock.settimeout(configurations["timeout"])
                sock.connect((HOST, PORT))
                
                if emulab_test:
                    target = 16
                    max_speed = (target * 1000 * 1000)/8
                    second_target = int(max_speed/1)
                    second_data_count = 0
                    data_count = 0
                    time_next = time.time() + 1

                for i in range(indx, len(file_names), num_workers.value):
                    duration = time.time() - start
                    if (sample_phase.value == 1 and (duration > probing_time)) or (process_status[indx] == 0):
                        break
                    
                    if transfer_status[i] == 0:
                        filename = root + file_names[i]
                        file = open(filename, "rb")
                        offset = file_offsets[i]

                        log.debug("starting {0}, {1}, {2}".format(indx, i, filename))
                        timer100ms = time.time();
                       
                        while process_status[indx] == 1:
                            chunk_size.value = min(chunk_size.value, second_target-second_data_count)
                            data_to_send = bytearray(chunk_size.value )
                            sent = sock.send(data_to_send)
                            offset += sent
                            file_offsets[i] = offset

                            if emulab_test:
                                second_data_count += sent
                                if second_data_count >= second_target:
                                    log.info("took {0} ms to send data".format((time.time()-timer100ms)*1000))
                                    second_data_count = 0
                                    current_time = time.time()
                                    if current_time > timer100ms + 1:
                                        log.error("It took more than 1000ms to transfer data, unexpected condition!!! cur_time:{0}, timer100ms: {1}".format(current_time, timer100ms+1))
                                        process_status[indx] = 0
                                        kill_transfer.value = 1
                                    time.sleep(timer100ms + 1 - time.time())
                                    timer100ms = time.time();
                                data_count += sent
                                
                                if data_count >= max_speed:
                                    if data_count > max_speed:
                                        log.error("Data count is {0} is more than maximum data size {1}".format(data_count, max_speed))
                                    data_count = 0
                                    sleep_for = time_next - time.time()
                                    if sleep_for > 0:
                                        time.sleep(sleep_for)
                                    
                                    time_next = time.time() + 1
                
                own_addr = sock.getsockname()
                addr = str(own_addr[0]) + ":" + str(own_addr[1])
                sc, rc = tcp_stats(addr)
                segments_sent.value += sc
                segments_retransmitted.value += rc
                lr = rc/sc if sc>0 else 0
                log.info("Process: {0}, Loss Rate: {1}".format(indx+1, np.round(lr, 4)))
                process_status[indx] = 0
                sock.close()
            
            except socket.timeout as e:
                if sample_phase.value == 1:
                    process_status[indx] = 0
                    
                timeout_count.value = timeout_count.value + 1
                # log.error("{0}, {1}".format(indx, str(e)))
                
            except Exception as e:
                log.error("{0}, {1}".format(indx, str(e)))
    
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
    global sample_phase_number
    if kill_transfer.value == 1:
        return 10 ** 10
        
    start_time = time.time()
    timeout_count.value = 0
    score_before = np.sum(file_offsets)
    num_workers.value = params[0]
    
    if sample_phase_number == 1:
        chunk_size.value = get_buffer_size(params[1])
        
    log.info("Sample Transfer -- Probing Parameters: {0}".format([num_workers.value, chunk_size.value]))
    
    before_sc, before_rc = segments_sent.value, segments_retransmitted.value
    if len(file_names) < num_workers.value:
        num_workers.value = len(file_names)
    
    for i in range(num_workers.value):
        process_status[i] = 1
        
    # time.sleep(probing_time)
        
    while np.sum(process_status)>0:
        if (time.time() - start_time) > 2*probing_time:
            for i in range(num_workers.value):
                process_status[i] = 0
            
        time.sleep(0.01)
    
    after_sc, after_rc = segments_sent.value, segments_retransmitted.value
    sc, rc = after_sc - before_sc, after_rc - before_rc
    
    score_after = np.sum(file_offsets)
    score = score_after - score_before
    duration = time.time() - start_time         
    thrpt = (score * 8) / (duration*1000*1000)
    
    rc = int(rc * (1+(timeout_count.value/num_workers.value)))
    lr, C = 0, 10
    if sc != 0:
        lr = rc/sc if sc>rc else 0.99
        
    log.info("Sample Transfer -- Throughput: {0}, Loss Rate: {1}, Packet Retransmitted: {2}".format(
        np.round(thrpt), np.round(lr, 4), rc))
    
    score_value = thrpt * (1 - C * ((1/(1-lr))-1)) 
    # score_value = score_value * (
    #     1 + (configurations["thread_limit"] - num_workers.value)/(2*configurations["thread_limit"]))
    
    return np.round(score_value * (-1), 4)


def normal_transfer(params):
    global probe_again
    timeout_count.value = 0
    num_workers.value = params[0]
    if sample_phase_number == 1:
        chunk_size.value = get_buffer_size(params[1])
        
    log.info("Normal Transfer -- Probing Parameters: {0}".format([num_workers.value, chunk_size.value]))
    
    files_left = len(transfer_status) - np.sum(transfer_status)
    if files_left < num_workers.value:
        num_workers.value = files_left

    for i in range(num_workers.value):
        process_status[i] = 1
    
    while (np.sum(process_status) > 0) and (kill_transfer.value == 0):
        if probe_again:
            files_left = len(transfer_status) - np.sum(transfer_status)
            if files_left < configurations["thread"]["min"]:
                log.info("Not much transfer is left. Further probing request ignored!")
            else:
                break
            
        time.sleep(0.01)

    if probe_again and (len(transfer_status) > np.sum(transfer_status)):
        run_transfer()
    
    
def run_transfer():
    global probe_again, sample_phase_number
    probe_again = False
    
    if configurations["method"].lower() == "random":
        sample_phase.value = 1
        sample_phase_number += 1
        params = random_opt(configurations, sample_transfer, log)
    
    elif configurations["method"].lower() == "brute":
        sample_phase.value = 1
        sample_phase_number += 1
        params = brute_force(configurations, sample_transfer, log)
    
    elif configurations["method"].lower() == "probe":
        params = [configurations["probe_config"]["thread"], configurations["probe_config"]["bsize"]]
        chunk_size.value = get_buffer_size(params[1])
        
    else:
        sample_phase.value = 1
        sample_phase_number += 1
        if sample_phase_number == 1:
            params = initial_probe(configurations, sample_transfer, log)
        else:
            params = repetitive_probe(configurations, sample_transfer, log)
    
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
        time_sec = curr_time-start_time
        total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
        total_bytes = np.sum(file_offsets)
        thrpt = (total_bytes*8)/time_sec
        
        curr_total = total_bytes - previous_total
        curr_time_sec = time_sec - previous_time
        curr_thrpt = (curr_total*8)/curr_time_sec
        previous_time, previous_total = time_sec, total_bytes
        throughput_logs.append(curr_thrpt)
        log.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(time_sec, np.round(curr_thrpt/(1000*1000),2), np.round(thrpt/(1000*1000),2)))

        if sample_phase.value == 0:
            if sampling_ended == 0:
                sampling_ended = time_sec
            
            if (time_sec - sampling_ended > 10) and (np.mean(throughput_logs[-10:]) < 1.0):
                log.info("Alas! Transfer is Stuck. Killing it!")
                kill_transfer.value = 1
                
            if configurations["multiple_probe"] and configurations["method"].lower() == "bayes" and (time_sec - sampling_ended > 20):
                n = 10
                last_n_sec_thrpt = np.mean(throughput_logs[-n:])
                max_mean_thrpt = max(max_mean_thrpt, last_n_sec_thrpt)
                min_mean_thrpt = min(min_mean_thrpt, last_n_sec_thrpt)
                lower_limit = max_mean_thrpt * configurations["probing_threshold"]
                upper_limit = min_mean_thrpt * (2-configurations["probing_threshold"])
                
                if last_n_sec_thrpt < lower_limit or last_n_sec_thrpt > upper_limit:
                    log.info("It Seems We Need to Probe Again!")
                    probe_again = True
                    configurations["thread"]["min"] =max(int(num_workers.value/2), 1)
                    configurations["thread"]["max"] =min (num_workers.value + int(num_workers.value/2), configurations["thread_limit"])
                    
                    if configurations["thread"]["max"] == configurations["thread"]["min"]:
                        probe_again = False
                    
                    if configurations["thread"]["max"] > configurations["thread"]["min"]:  
                        configurations["thread"]["iteration"] = min(configurations["thread"]["max"] - configurations["thread"]["min"], 8)
                        configurations["thread"]["random_probe"] = max(1, int(configurations["thread"]["iteration"]/2))
        else:
            max_mean_thrpt = 0
            min_mean_thrpt = 100000
            sampling_ended = 0
                
        time.sleep(0.998)


workers = [mp.Process(target=worker, args=(i,)) for i in range(configurations["thread_limit"])]


if __name__ == '__main__':
    sock = socket.socket()
    sock.settimeout(configurations["timeout"])
    sock.connect((HOST, PORT))
    
    while True:
        start = time.time()
        data_to_send = bytearray(get_buffer_size(11))
        sent = sock.send(data_to_send)
        end = time.time()
        time_taken = np.round((end-start)*1000)
        
        if time_taken < 1000:
            sleep_time = 1000 - time_taken
            log.info("sent data: {0}, time spent: {1}ms, sleep time: {2}ms".format(sent, time_taken, sleep_time))
            time.sleep(sleep_time/1000)
        else:
            log.info("sent data: {0}, time spent: {1}ms. Exiting ...".format(sent, time_taken))
            break