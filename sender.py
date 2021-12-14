## Only supports Concurrency optimization

import datetime
import logging as log
import multiprocessing as mp
import os
import socket
import time
import warnings

import numpy as np

from FileState import Fs
from config_sender import configurations
from search import base_optimizer, dummy, brute_force, hill_climb, cg_opt, lbfgs_opt, gradient_opt_fast
from multiprocessing.managers import BaseManager

warnings.filterwarnings("ignore", category=FutureWarning)
log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
log_file = "logs/" + datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + ".log"

if configurations["loglevel"] == "debug":
    log.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log.DEBUG,
        # filename=log_file,
        # filemode="w"
        handlers=[
            log.FileHandler(log_file),
            log.StreamHandler()
        ]
    )

    mp.log_to_stderr(log.DEBUG)
else:
    log.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log.INFO,
        # filename=log_file,
        # filemode="w"
        handlers=[
            log.FileHandler(log_file),
            log.StreamHandler()
        ]
    )


def tcp_stats():
    global RCVR_ADDR
    start = time.time()
    sent, retm = 0, 0

    try:
        data = os.popen("ss -ti").read().split("\n")
        for i in range(1, len(data)):
            if RCVR_ADDR in data[i - 1]:
                parse_data = data[i].split(" ")
                for entry in parse_data:
                    if "data_segs_out" in entry:
                        sent += int(entry.split(":")[-1])

                    if "bytes_retrans" in entry:
                        pass

                    elif "retrans" in entry:
                        retm += int(entry.split("/")[-1])

    except Exception as e:
        print(e)

    end = time.time()
    log.debug("Time taken to collect tcp stats: {0}ms".format(np.round((end - start) * 1000)))
    return sent, retm


def worker(process_id, fs):
    log.debug("incomplete files  :: {0}".format(fs.filesIn.get_file_incomplete()))
    while fs.filesIn.get_file_incomplete() > 0:
        if fs.process_status[process_id] == 0:
            pass
        else:
            while fs.num_workers.value < 1:
                pass

            log.debug("Start Process :: {0}".format(process_id))
            try:
                sock = socket.socket()
                sock.settimeout(3)
                sock.connect((fs.HOST, fs.PORT))

                if fs.emulab_test:
                    target, factor = 20, 10
                    max_speed = (target * 1000 * 1000) / 8
                    second_target, second_data_count = int(max_speed / factor), 0

                while (not fs.q.empty()) and (fs.process_status[process_id] == 1):
                    log.debug("getting  :: {0}".format(process_id))
                    try:
                        file_id = fs.q.get()
                        log.debug("getting {0}  from  {1}".format(file_id, process_id))
                    except:
                        fs.process_status[process_id] = 0
                        break

                    offset = fs.filesIn.get_file_offsets(file_id)
                    to_send = fs.filesIn.get_file_sizes(file_id) - offset
                    log.debug("for processid {0},offset  {1},to_send {2}".format(process_id, offset, to_send))
                    if (to_send > 0) and (fs.process_status[process_id] == 1):
                        filename = fs.filesIn.get_root()[file_id] + fs.filesIn.get_file_names()[file_id]
                        file = open(filename, "rb")
                        msg = fs.filesIn.get_file_names()[file_id] + "," + str(int(offset))
                        msg += "," + str(int(to_send)) + "\n"
                        sock.send(msg.encode())

                        log.debug("starting {0}, {1}, {2}".format(process_id, file_id, filename))
                        timer100ms = time.time()

                        while (to_send > 0) and (fs.process_status[process_id] == 1):
                            log.debug("in {0}, {1}, {2}".format(process_id, file_id, filename))
                            if fs.emulab_test:
                                block_size = min(fs.chunk_size, second_target - second_data_count)

                                data_to_send = bytearray(block_size)
                                sent = sock.send(data_to_send)
                            else:
                                block_size = min(fs.chunk_size, to_send)
                                log.debug("count must be integer {0}".format(int(block_size)))
                                if fs.file_transfer:
                                    sent = sock.sendfile(file=file, offset=int(offset), count=int(block_size))
                                    # data = os.preadv(file, block_size, offset)
                                else:
                                    data_to_send = bytearray(block_size)
                                    sent = sock.send(data_to_send)

                            offset += sent
                            to_send -= sent
                            fs.filesIn.set_file_offsets(file_id, offset)
                            # fs.file_offsets[file_id] = offset
                            log.debug("in {0}, {1}, {2}".format(process_id, file_id, filename))
                            if fs.emulab_test:
                                second_data_count += sent
                                if second_data_count >= second_target:
                                    second_data_count = 0
                                    while timer100ms + (1 / factor) > time.time():
                                        pass

                                    timer100ms = time.time()
                    # log.debug("finished {0}, {1}, {2}".format(process_id, file_id, filename))
                    if to_send > 0:
                        fs.q.put(file_id)
                    else:
                        filCo = fs.filesIn.get_file_incomplete() - 1
                        log.debug("new filecount {0} ".format(filCo))
                        fs.filesIn.set_file_incomplete(filCo)
                        # fs.file_incomplete.value = fs.file_incomplete.value - 1

                sock.close()

            except socket.timeout as e:
                pass

            # except Exception as e:
            #     fs.process_status[process_id] = 0
            #     log.error("Process: {0}, Error: {1}".format(process_id, str(e)))

            log.debug("End Process :: {0}".format(process_id))

    fs.process_status[process_id] = 0


def sample_transfer(params, fs):
    global throughput_logs
    if fs.file_incomplete.value == 0:
        return 10 ** 10

    params = [int(np.round(x)) for x in params]
    params = [1 if x < 1 else x for x in params]

    log.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    fs.num_workers.value = params[0]

    current_cc = np.sum(fs.process_status)
    for i in range(configurations["thread_limit"]):
        if i < params[0]:
            if i >= current_cc:
                fs.process_status[i] = 1
        else:
            fs.process_status[i] = 0

    log.debug("Active CC: {0}".format(np.sum(fs.process_status)))

    time.sleep(1)
    before_sc, before_rc = tcp_stats()
    n_time = time.time() + fs.probing_time - 1.1
    # time.sleep(n_time)
    while (time.time() < n_time) and (fs.file_incomplete.value > 0):
        time.sleep(0.1)

    after_sc, after_rc = tcp_stats()
    sc, rc = after_sc - before_sc, after_rc - before_rc

    log.info("SC: {0}, RC: {1}".format(sc, rc))
    thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 2 else 0

    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])
    if sc != 0:
        lr = rc / sc if sc > rc else 0

    cc_impact_nl = K ** fs.num_workers.value
    # cc_impact_lin = (K-1) * num_workers.value
    plr_impact = B * lr
    # score = thrpt
    score = (thrpt / cc_impact_nl) - (thrpt * plr_impact)
    # score = thrpt * (1- plr_impact - cc_impact_lin)
    score_value = np.round(score * (-1))

    log.info("Sample Transfer -- Throughput: {0}Mbps, Loss Rate: {1}%, Score: {2}".format(
        np.round(thrpt), np.round(lr * 100, 2), score_value))

    if fs.file_incomplete.value == 0:
        return 10 ** 10
    else:
        return score_value


def normal_transfer(params, fs):
    fs.num_workers.value = max(1, int(np.round(params[0])))
    log.info("Normal Transfer -- Probing Parameters: {0}".format([fs.num_workers.value]))

    for i in range(fs.num_workers.value):
        fs.process_status[i] = 1

    while (np.sum(fs.process_status) > 0) and (fs.filesIn.get_file_incomplete() > 0):
        pass


def run_transfer(fs):
    params = []
    if configurations["method"].lower() == "random":
        log.info("Running Random Optimization .... ")
        params = dummy(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "brute":
        log.info("Running Brute Force Optimization .... ")
        params = brute_force(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "hill_climb":
        log.info("Running Hill Climb Optimization .... ")
        params = hill_climb(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "gradient":
        log.info("Running Gradient Optimization .... ")
        params = gradient_opt_fast(configurations, sample_transfer, log)

    elif configurations["method"].lower() == "cg":
        log.info("Running Conjugate Optimization .... ")
        params = cg_opt(configurations, sample_transfer)

    elif configurations["method"].lower() == "lbfgs":
        log.info("Running LBFGS Optimization .... ")
        params = lbfgs_opt(configurations, sample_transfer)

    elif configurations["method"].lower() == "probe":
        log.info("Running a fixed configurations Probing .... ")
        params = [configurations["fixed_probing"]["thread"]]

    else:
        log.info("Running Bayesian Optimization .... ")
        params = base_optimizer(configurations, sample_transfer, log)

    if fs.filesIn.get_file_incomplete() > 0:
        normal_transfer(params, fs)


def report_throughput(start_time, fs):
    # global throughput_logs
    previous_total = 0
    previous_time = 0
    log.debug("incomplete files  :: {0}".format(fs.filesIn.get_file_incomplete()))
    while fs.filesIn.get_file_incomplete() > 0:
        t1 = time.time()
        time_since_begining = np.round(t1 - start_time, 1)

        if time_since_begining >= 1:
            total_bytes = fs.filesIn.get_total_file_offsets()
            thrpt = np.round((total_bytes * 8) / (time_since_begining * 1000 * 1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total * 8) / (curr_time_sec * 1000 * 1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            fs.throughput_logs.append(curr_thrpt)
            m_avg = np.round(np.mean(fs.throughput_logs[-60:]), 2)
            log.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps, 60Sec_Average: {3}Mbps".format(
                time_since_begining, curr_thrpt, thrpt, m_avg))
            t2 = time.time()
            time.sleep(max(0, 1 - (t2 - t1)))


class Workers:
    def __init__(self):
        self.workers = []

    def start_workers(self, worker, fs, theads_count=1):
        self.workers = [mp.Process(name="FalconWorkers", target=worker, args=(i, fs)) for i in range(configurations["thread_limit"])]
        for p in self.workers:
            p.daemon = True
            p.start()

    def stop_workers(self):
        for p in self.workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)

    def running_workers(self):
        count = 0
        for p in self.workers:
            if p.is_alive():
                count = count + 1
        return count


def initialize_transfer(fs,logger = 0):
    # workers = [mp.Process(target=worker, args=(i, fs)) for i in range(configurations["thread_limit"])]
    # for p in workers:
    #     p.daemon = True
    #     p.start()

    if (logger != 0):
        logger.info(
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX initialize_transfer started XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    jb = Workers()

    jb.start_workers(worker, fs)
    start = time.time()
    reporting_process = mp.Process(name="FalconReports",target=report_throughput, args=(start, fs))
    reporting_process.daemon = True
    reporting_process.start()
    run_transfer(fs)
    end = time.time()

    time_since_begining = np.round(end - start, 3)
    total = np.round(fs.filesIn.get_total_file_offsets() / (1024 * 1024 * 1024), 3)
    thrpt = np.round((total * 8 * 1024) / time_since_begining, 2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
        total, time_since_begining, thrpt))
    reporting_process.terminate()

    jb.stop_workers()
    # for p in workers:
    #     if p.is_alive():
    #         p.terminate()
    #         p.join(timeout=0.1)

    print("initialize_transfer ",fs.q.qsize())
    if (logger != 0):
        logger.info(
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX initialize_transfer completed XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

fs = Fs()

def get_fs():

    return fs

ls =[]

def get_ls():
    return ls
def check_way(fil):
    time.sleep(3)
    ls.append(fil)
    print(ls)

if __name__ == '__main__':

    ds= get_fs()
    ds.set_connection(configurations["receiver"]["host"], configurations["receiver"]["port"])
    print("q-------",ds.q.qsize())
    ds.add_to_queue(configurations["data_dir"])
    print("q-------", ds.q.qsize())
    initialize_transfer(ds)
    ds.filesIn.print_state()
    print("q-------",ds.q.qsize())
