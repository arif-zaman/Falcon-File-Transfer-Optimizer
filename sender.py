## Only supports Concurrency optimization

import os
import time
import uuid
import socket
import warnings
import datetime
import numpy as np
import logging as logger
import multiprocessing as mp
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from config_sender import configurations
from search import base_optimizer, dummy, brute_force, hill_climb, cg_opt, lbfgs_opt, gradient_opt_fast
from utils import tcp_stats, run, get_dir_size

warnings.filterwarnings("ignore", category=FutureWarning)
configurations["cpu_count"] = mp.cpu_count()
configurations["thread_limit"] = configurations["max_cc"]

if configurations["thread_limit"] == -1:
    configurations["thread_limit"] = configurations["cpu_count"]

log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
log_file = "logs/" + datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + ".log"

if configurations["loglevel"] == "debug":
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.DEBUG,
        # filename=log_file,
        # filemode="w"
        handlers=[
            logger.FileHandler(log_file),
            logger.StreamHandler()
        ]
    )

    mp.log_to_stderr(logger.DEBUG)
else:
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.INFO,
        # filename=log_file,
        # filemode="w"
        handlers=[
            logger.FileHandler(log_file),
            logger.StreamHandler()
        ]
    )

    # mp.log_to_stderr(logger.INFO)


emulab_test = False
if "emulab_test" in configurations and configurations["emulab_test"] is not None:
    emulab_test = configurations["emulab_test"]

centralized = False
if "centralized" in configurations and configurations["centralized"] is not None:
    centralized = configurations["centralized"]

executor = ThreadPoolExecutor(max_workers=5)
if centralized:
    from redis import Redis
    transfer_id = str(uuid.uuid4())
    ## Redis Config
    hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
    port = os.environ.get("REDIS_PORT", 6379)
    send_key = f"report:{transfer_id}"
    receive_key = f"cc:{transfer_id}"
    register_key = f"transfer-registration"
    r_conn = Redis(hostname, port, retry_on_timeout=True)

file_transfer = True
if "file_transfer" in configurations and configurations["file_transfer"] is not None:
    file_transfer = configurations["file_transfer"]

manager = mp.Manager()
root_dir = configurations["data_dir"]
tmpfs_dir = "/dev/shm/data/"
probing_time = configurations["probing_sec"]
file_names = os.listdir(root_dir) * configurations["multiplier"]
file_sizes = [os.path.getsize(root_dir+filename) for filename in file_names]
file_count = len(file_names)
throughput_logs = manager.list()

exit_signal = 10 ** 10
chunk_size = 1 * 1024 * 1024
num_transfer_workers = mp.Value("i", 0)
num_io_workers = mp.Value("i", 0)
transfer_incomplete = mp.Value("i", file_count)
copy_incomplete = mp.Value("i", file_count)
clean_incomplete = mp.Value("i", file_count)

transfer_process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
io_process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
file_offsets = mp.Array("d", [0.0 for i in range(file_count)])
rQueue = mp.Queue()
tQueue = mp.Queue()
gQueue = mp.Queue()

HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
RCVR_ADDR = str(HOST) + ":" + str(PORT)


def copy_file(process_id):
    logger.info(f'Starting Copying Thread: {process_id}')
    while copy_incomplete.value > 0:
        if io_process_status[process_id] == 0:
            continue

        try:
            available = 170 - get_dir_size(logger, path=tmpfs_dir)
            logger.debug(f"available size = {available} GB")
            if available > 20:
                file_id = rQueue.get_nowait()
                fname = file_names[file_id]
                run(f'cp {root_dir}{fname} {tmpfs_dir}', logger)
                logger.debug(f'I/O :: {fname} now in temfs!')
                copy_incomplete.value = copy_incomplete.value - 1
                tQueue.put(file_id)
        except Exception as e:
            # logger.exception(e)
            time.sleep(0.1)

    logger.info(f'Exiting Copying Thread: {process_id}')


def garbage_collector(process_id):
    logger.info(f'Starting Garbage Collector Thread: {process_id}')
    while clean_incomplete.value > 0:
        try:
            file_id = gQueue.get_nowait()
            fname = file_names[file_id]
            run(f'rm {tmpfs_dir}{fname}', logger)
            clean_incomplete.value = clean_incomplete.value - 1
            logger.debug(f'GARBAGE COLLECTOR :: {fname} removed from tempfs!')
        except:
            time.sleep(1)


    logger.info(f'Exiting Garbage Collector Thread: {process_id}')


def transfer_file(process_id): #tQueue
    while transfer_incomplete.value > 0:
        if transfer_process_status[process_id] == 0 or tQueue.empty():
            pass
        else:
            while num_transfer_workers.value < 1:
                pass

            logger.debug("Start Process :: {0}".format(process_id))
            try:
                sock = socket.socket()
                sock.settimeout(3)
                sock.connect((HOST, PORT))

                if emulab_test:
                    target, factor = 20, 10
                    max_speed = (target * 1000 * 1000)/8
                    second_target, second_data_count = int(max_speed/factor), 0

                while (not tQueue.empty()) and (transfer_process_status[process_id] == 1):
                    try:
                        file_id = tQueue.get()
                    except:
                        transfer_process_status[process_id] = 0
                        break

                    offset = file_offsets[file_id]
                    to_send = file_sizes[file_id] - offset

                    if (to_send > 0) and (transfer_process_status[process_id] == 1):
                        filename = tmpfs_dir + file_names[file_id]
                        file = open(filename, "rb")
                        msg = file_names[file_id] + "," + str(int(offset))
                        msg += "," + str(int(to_send)) + "\n"
                        sock.send(msg.encode())

                        logger.debug("starting {0}, {1}, {2}".format(process_id, file_id, filename))
                        timer100ms = time.time()

                        while (to_send > 0) and (transfer_process_status[process_id] == 1):
                            if emulab_test:
                                block_size = min(chunk_size, second_target-second_data_count)
                                data_to_send = bytearray(int(block_size))
                                sent = sock.send(data_to_send)
                            else:
                                block_size = min(chunk_size, to_send)

                                if file_transfer:
                                    sent = sock.sendfile(file=file, offset=int(offset), count=int(block_size))
                                    # data = os.preadv(file, block_size, offset)
                                else:
                                    data_to_send = bytearray(int(block_size))
                                    sent = sock.send(data_to_send)

                            offset += sent
                            to_send -= sent
                            file_offsets[file_id] = offset

                            if emulab_test:
                                second_data_count += sent
                                if second_data_count >= second_target:
                                    second_data_count = 0
                                    while timer100ms + (1/factor) > time.time():
                                        pass

                                    timer100ms = time.time()

                    if to_send > 0:
                        tQueue.put(file_id)
                    else:
                        transfer_incomplete.value = transfer_incomplete.value - 1
                        gQueue.put(file_id)

                sock.close()

            except socket.timeout as e:
                pass

            except Exception as e:
                transfer_process_status[process_id] = 0
                logger.error("Process: {0}, Error: {1}".format(process_id, str(e)))

            logger.debug("End Process :: {0}".format(process_id))

    transfer_process_status[process_id] = 0


def event_receiver():
    while transfer_incomplete.value > 0:
        try:
            resp = r_conn.xread({receive_key: 0}, count=None)
            if resp:
                key, messages = resp[0]
                for message in messages:
                    _, data = message
                    cc = int(data[b"cc"].decode("utf-8"))
                    r_conn.delete(key)

                    cc = 1 if cc<1 else int(np.round(cc))
                    num_transfer_workers.value = cc
                    logger.info("Sample Transfer -- Probing Parameters: {0}".format(num_transfer_workers.value))

                    current_cc = np.sum(transfer_process_status)
                    for i in range(configurations["thread_limit"]):
                        if i < cc:
                            if (i >= current_cc):
                                transfer_process_status[i] = 1
                        else:
                            transfer_process_status[i] = 0

                    logger.debug("Active CC: {0}".format(np.sum(transfer_process_status)))
        except Exception as e:
            logger.exception(e)


def event_sender(sc, rc):
    B, K = int(configurations["B"]), float(configurations["K"])
    score = exit_signal
    if transfer_incomplete.value > 0:
        thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 1 else 0
        lr = rc/sc if (sc>rc and sc!= 0) else 0

        # score = thrpt
        plr_impact = B*lr
        # cc_impact_lin = (K-1) * num_transfer_workers.value
        # score = thrpt * (1- plr_impact - cc_impact_lin)
        cc_impact_nl = K**num_transfer_workers.value
        score = (thrpt/cc_impact_nl) - (thrpt * plr_impact)
        score = np.round(score * (-1))

    try:
        data = {}
        data["score"] = int(score)
        r_conn.xadd(send_key, data)
    except Exception as e:
        logger.exception(e)


def run_centralized():
    receiver_thread = Thread(target=event_receiver)
    receiver_thread.start()

    prev_sc, prev_rc = tcp_stats(RCVR_ADDR, logger)
    while transfer_incomplete.value > 0:
        time.sleep(0.9)
        curr_sc, curr_rc = tcp_stats(RCVR_ADDR, logger)
        sc, rc = curr_sc - prev_sc, curr_rc - prev_rc
        prev_sc, prev_rc = curr_sc, curr_rc
        executor.submit(event_sender, sc, rc)


def sample_transfer(params):
    global throughput_logs, exit_signal

    if transfer_incomplete.value == 0:
        return exit_signal

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    num_transfer_workers.value = params[0]
    num_io_workers.value = params[1]

    current_cc = np.sum(transfer_process_status)
    for i in range(configurations["thread_limit"]):
        if i < params[0]:
            if (i >= current_cc):
                transfer_process_status[i] = 1
        else:
            transfer_process_status[i] = 0

    current_io_cc = np.sum(io_process_status)
    for i in range(configurations["thread_limit"]):
        if i < params[0]:
            if (i >= current_io_cc):
                io_process_status[i] = 1
        else:
            io_process_status[i] = 0

    logger.debug("Active CC: {0}".format(np.sum(transfer_process_status)))

    time.sleep(1)
    prev_sc, prev_rc = tcp_stats(RCVR_ADDR, logger)
    n_time = time.time() + probing_time - 1.1
    # time.sleep(n_time)
    while (time.time() < n_time) and (transfer_incomplete.value > 0):
        time.sleep(0.1)

    curr_sc, curr_rc = tcp_stats(RCVR_ADDR, logger)
    sc, rc = curr_sc - prev_sc, curr_rc - prev_rc

    logger.debug("TCP Segments >> Send Count: {0}, Retrans Count: {1}".format(sc, rc))
    thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 2 else 0

    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])
    if sc != 0:
        lr = rc/sc if sc>rc else 0

    # score = thrpt
    plr_impact = B*lr
    # cc_impact_lin = (K-1) * num_transfer_workers.value
    # score = thrpt * (1- plr_impact - cc_impact_lin)
    cc_impact_nl = K**(num_transfer_workers.value+num_io_workers.value)
    score = (thrpt/cc_impact_nl) - (thrpt * plr_impact)
    score_value = np.round(score * (-1))

    logger.info("Sample Transfer -- Throughput: {0}Mbps, Loss Rate: {1}%, Score: {2}".format(
        np.round(thrpt), np.round(lr*100, 2), score_value))

    if transfer_incomplete.value == 0:
        return exit_signal
    else:
        return score_value


def normal_transfer(params):
    num_transfer_workers.value = max(1, int(np.round(params[0])))
    num_io_workers.value = max(1, int(np.round(params[1])))
    logger.info("Normal Transfer -- Probing Parameters: {0}".format([num_transfer_workers.value]))

    for i in range(num_transfer_workers.value):
        transfer_process_status[i] = 1

    if configurations["mp_opt"]:
        for i in range(num_io_workers.value):
            io_process_status[i] = 1

    while (np.sum(transfer_process_status) > 0) and (transfer_incomplete.value > 0):
        pass


def run_transfer():
    params = [2]

    if centralized:
        run_centralized()

    elif configurations["method"].lower() == "random":
        logger.info("Running Random Optimization .... ")
        params = dummy(configurations, sample_transfer, logger)

    elif configurations["method"].lower() == "brute":
        logger.info("Running Brute Force Optimization .... ")
        params = brute_force(configurations, sample_transfer, logger)

    elif configurations["method"].lower() == "hill_climb":
        logger.info("Running Hill Climb Optimization .... ")
        params = hill_climb(configurations, sample_transfer, logger)

    elif configurations["method"].lower() == "gradient":
        logger.info("Running Gradient Optimization .... ")
        params = gradient_opt_fast(configurations, sample_transfer, logger)

    elif configurations["method"].lower() == "cg":
        logger.info("Running Conjugate Optimization .... ")
        params = cg_opt(configurations, sample_transfer)

    elif configurations["method"].lower() == "lbfgs":
        logger.info("Running LBFGS Optimization .... ")
        params = lbfgs_opt(configurations, sample_transfer)

    elif configurations["method"].lower() == "probe":
        logger.info("Running a fixed configurations Probing .... ")
        max_thread = params = [configurations["fixed_probing"]["thread"]]
        if configurations["mp_opt"]:
            params = [max_thread, max_thread]
        else:
            params = [max_thread]

    else:
        logger.info("Running Bayesian Optimization .... ")
        params = base_optimizer(configurations, sample_transfer, logger)

    if transfer_incomplete.value > 0:
        normal_transfer(params)


def report_throughput(start_time):
    global throughput_logs
    previous_total = 0
    previous_time = 0

    while transfer_incomplete.value > 0:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining >= 0.1:
            total_bytes = np.sum(file_offsets)
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            throughput_logs.append(curr_thrpt)
            m_avg = np.round(np.mean(throughput_logs[-60:]), 2)

            logger.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps, 60Sec_Average: {3}Mbps".format(
                time_since_begining, curr_thrpt, thrpt, m_avg))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


if __name__ == '__main__':
    if centralized:
        try:
            r_conn.xadd(register_key, {"transfer_id": transfer_id})
        except ConnectionError as e:
            logger.error(f"Redis Connection Error: {e}")

    for i in range(file_count):
        rQueue.put(i)

    copy_workers = [mp.Process(target=copy_file, args=(i,)) for i in range(configurations["thread_limit"])]
    for p in copy_workers:
        p.daemon = True
        p.start()

    transfer_workers = [mp.Process(target=transfer_file, args=(i,)) for i in range(configurations["thread_limit"])]
    for p in transfer_workers:
        p.daemon = True
        p.start()

    cleanup_workers = [mp.Process(target=garbage_collector, args=(i,)) for i in range(5)]
    for p in cleanup_workers:
        p.daemon = True
        p.start()

    start = time.time()
    reporting_process = mp.Process(target=report_throughput, args=(start,))
    reporting_process.daemon = True
    reporting_process.start()
    run_transfer()
    end = time.time()
    reporting_process.terminate()

    time_since_begining = np.round(end-start, 3)
    total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
    thrpt = np.round((total*8*1024)/time_since_begining,2)

    while clean_incomplete.value > 0:
        time.sleep(0.1)

    logger.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
        total, time_since_begining, thrpt))

    for p in copy_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)

    for p in transfer_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)

    for p in cleanup_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)
