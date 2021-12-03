## Only supports Concurrency optimization

import datetime
import logging as log
import multiprocessing as mp
import os
import socket
import time
import warnings

import numpy as np

from config_sender import configurations
from search import base_optimizer, dummy, brute_force, hill_climb, cg_opt, lbfgs_opt, gradient_opt_fast

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


# emulab_test = False
# if "emulab_test" in configurations and configurations["emulab_test"] is not None:
#     emulab_test = configurations["emulab_test"]
#
# file_transfer = True
# if "file_transfer" in configurations and configurations["file_transfer"] is not None:
#     file_transfer = configurations["file_transfer"]


# configurations["thread_limit"] = configurations["max_cc"]
# configurations["cpu_count"] = mp.cpu_count()
# if configurations["thread_limit"] == -1:
#     configurations["thread_limit"] = configurations["cpu_count"]
# manager = mp.Manager()
# probing_time = configurations["probing_sec"]
# throughput_logs = manager.list()
# chunk_size = 1 * 1024 * 1024


# num_workers = mp.Value("i", 0)
# process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

# HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
# RCVR_ADDR = str(HOST) + ":" + str(PORT)


# root = configurations["data_dir"]
# file_names = os.listdir(root) * configurations["multiplier"]
# file_sizes = [os.path.getsize(root + filename) for filename in file_names]
# file_count = len(file_names)
# file_incomplete = mp.Value("i", file_count)
# file_offsets = mp.Array("d", [0.0 for i in range(file_count)])
class Fs:
    def __init__(self, vhost, vport):
        # self.root = vroot
        #
        # self.file_names = os.listdir(self.root) * multiplier
        # self.file_sizes = [os.path.getsize(self.root + filename) for filename in self.file_names]
        # self.file_count = len(self.file_names)
        # self.file_incomplete = mp.Value("i", self.file_count)
        # self.file_offsets = mp.Array("d", [0.0 for i in range(self.file_count)])

        self.root = []
        self.file_names = []
        self.file_sizes = []
        self.file_count = 2
        self.file_incomplete = mp.Value("i", self.file_count)
        self.file_offsets = mp.Array("d", [0.0 for i in range(self.file_count)])

        self.HOST, self.PORT = vhost, vport
        self.RCVR_ADDR = str(self.HOST) + ":" + str(self.PORT)

        configurations["thread_limit"] = configurations["max_cc"]
        configurations["cpu_count"] = mp.cpu_count()
        if configurations["thread_limit"] == -1:
            configurations["thread_limit"] = configurations["cpu_count"]
        self.manager = mp.Manager()
        self.probing_time = configurations["probing_sec"]
        self.throughput_logs = self.manager.list()
        self.chunk_size = 1 * 1024 * 1024
        self.num_workers = mp.Value("i", 0)
        self.process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

        self.emulab_test = False
        if "emulab_test" in configurations and configurations["emulab_test"] is not None:
            self.emulab_test = configurations["emulab_test"]

        self.file_transfer = True
        if "file_transfer" in configurations and configurations["file_transfer"] is not None:
            self.file_transfer = configurations["file_transfer"]
        self.q = self.manager.Queue()

    def add_to_queue(self, vroot, multiplier=1):
        # time.sleep(10)
        print(vroot)
        file_names = os.listdir(vroot) * multiplier
        file_sizes = [os.path.getsize(vroot + filename) for filename in file_names]
        file_count = len(os.listdir(vroot) * multiplier)

        for i in range(self.file_count, self.file_count + file_count):
            self.q.put(i)

        self.file_incomplete = mp.Value("i", self.file_incomplete.value + file_count)
        self.file_offsets = mp.Array("d", [i for i in self.file_offsets] + [0.0 for i in range(file_count)])
        self.file_names = self.file_names + file_names
        self.file_sizes = self.file_sizes + file_sizes
        self.file_count = self.file_count + file_count
        self.root = self.root + [vroot for i in range(file_count)]
        print("file_incomplete",self.file_incomplete)
        self.is_finish()
        print("intital queue size", self.q.qsize(), self.file_count)
        self.q.task_done()


    def append_queue(self):

        for i in range(self.file_count):
            self.q.put(i)
        print(self.q.qsize(), self.file_count)

    def is_finish(self):
        print("finidhed check",time.time())
        print("queue size",self.q.qsize())

        print("file_names", self.file_incomplete)

        return True if self.q.qsize() == 0 else False


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
    while fs.file_incomplete.value > 0:
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

                    offset = fs.file_offsets[file_id]
                    to_send = fs.file_sizes[file_id] - offset
                    if (to_send > 0) and (fs.process_status[process_id] == 1):
                        filename = fs.root[file_id] + fs.file_names[file_id]
                        file = open(filename, "rb")
                        msg = fs.file_names[file_id] + "," + str(int(offset))
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
                            fs.file_offsets[file_id] = offset
                            log.debug("in {0}, {1}, {2}".format(process_id, file_id, filename))
                            if fs.emulab_test:
                                second_data_count += sent
                                if second_data_count >= second_target:
                                    second_data_count = 0
                                    while timer100ms + (1 / factor) > time.time():
                                        pass

                                    timer100ms = time.time()
                    log.debug("finished {0}, {1}, {2}".format(process_id, file_id, filename))
                    if to_send > 0:
                        fs.q.put(file_id)
                    else:
                        fs.file_incomplete.value = fs.file_incomplete.value - 1

                sock.close()

            except socket.timeout as e:
                pass

            except Exception as e:
                fs.process_status[process_id] = 0
                log.error("Process: {0}, Error: {1}".format(process_id, str(e)))

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

    while (np.sum(fs.process_status) > 0) and (fs.file_incomplete.value > 0):
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

    if fs.file_incomplete.value > 0:
        normal_transfer(params, fs)


def report_throughput(start_time, fs):
    # global throughput_logs
    previous_total = 0
    previous_time = 0

    while fs.file_incomplete.value > 0:
        t1 = time.time()
        time_since_begining = np.round(t1 - start_time, 1)

        if time_since_begining >= 1:
            total_bytes = np.sum(fs.file_offsets)
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
        self.workers = [mp.Process(target=worker, args=(i, fs)) for i in range(configurations["thread_limit"])]
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


def initialize_transfer(fs):
    # fs.initialoze_queue()
    fs.is_finish()
    # workers = [mp.Process(target=worker, args=(i, fs)) for i in range(configurations["thread_limit"])]
    # for p in workers:
    #     p.daemon = True
    #     p.start()
    jb = Workers()

    jb.start_workers(worker, fs)
    print("running workers", jb.running_workers())
    start = time.time()
    reporting_process = mp.Process(target=report_throughput, args=(start, fs))
    reporting_process.daemon = True
    reporting_process.start()
    run_transfer(fs)
    end = time.time()

    time_since_begining = np.round(end - start, 3)
    total = np.round(np.sum(fs.file_offsets) / (1024 * 1024 * 1024), 3)
    thrpt = np.round((total * 8 * 1024) / time_since_begining, 2)
    log.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
        total, time_since_begining, thrpt))
    print("running workers", jb.running_workers())
    reporting_process.terminate()
    jb.stop_workers()
    print("running workers", jb.running_workers())
    # for p in workers:
    #     if p.is_alive():
    #         p.terminate()
    #         p.join(timeout=0.1)


if __name__ == '__main__':
    # # print('Number of arguments:', len(sys.argv), 'arguments.')
    # # print('Argument List:', str(sys.argv))
    # # print(len(sys.argv),sys.argv[len(sys.argv)-1])
    # if(len(sys.argv)==2):
    #     dir= sys.argv[len(sys.argv)-1]
    # else:
    #     dir = configurations["data_dir"]
    # log.info(dir)
    fs = Fs(configurations["receiver"]["host"], configurations["receiver"]["port"])

    # fs.add_to_queue(configurations["data_dir"])
    time1 = time.time_ns()
    put_files = mp.Process(target=fs.add_to_queue, args=(configurations["data_dir"],))
    put_files.start()
    print("1",fs.is_finish())
    put_files.join()
    print(f"time is {time.time_ns() - time1}")

    print("mid",fs.is_finish())
    # mp.Process(target=fs.add_to_queue, args=(dir))
    initialize_transfer(fs)

    # mp.Process(target=fs.add_to_queue, args=(dir))
    # mp.Process(target=fs.add_to_queue, args=(configurations["data_dir"]))
    # fs.add_to_queue("/home/mbadhan/PycharmProjects/Falcon-File-Transfer-Optimizer2/inputs/")
    print("last",fs.is_finish())
