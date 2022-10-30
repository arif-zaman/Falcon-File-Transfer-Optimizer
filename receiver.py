import os
import mmap
import time
import socket
import warnings
import logging as logger
import numpy as np
import queue
import multiprocessing as mp
from threading import Thread
from config_receiver import configurations
from utils import available_space, run
from search import base_optimizer, hill_climb, cg_opt, gradient_opt_fast, exit_signal


warnings.filterwarnings("ignore", category=FutureWarning)
configurations["cpu_count"] = mp.cpu_count()
configurations["thread_limit"] = configurations["max_cc"]

if configurations["thread_limit"] == -1:
    configurations["thread_limit"] = configurations["cpu_count"]

chunk_size = mp.Value("i", 1024*1024)
root_dir = configurations["data_dir"]
tmpfs_dir = "/dev/shm/data/"
probing_time = configurations["probing_sec"]
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
transfer_complete = mp.Value("i", 0)
move_complete = mp.Value("i", 0)
cleanup_complete = mp.Value("i", 0)
transfer_done = mp.Value("i", 0)

io_process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
transfer_file_offsets = mp.Manager().dict()
io_file_offsets = mp.Manager().dict() ## figure out file_count
throughput_logs = mp.Manager().list()
io_throughput_logs = mp.Manager().list()

mQueue = mp.Manager().list()
gQueue = mp.Queue()
start, end = mp.Value("i", 0), mp.Value("i", 0)

direct_io = False
file_transfer = True
if "file_transfer" in configurations and configurations["file_transfer"] is not None:
    file_transfer = configurations["file_transfer"]

modular_test = False
if "modular_test" in configurations and configurations["modular_test"] is not None:
    modular_test = configurations["modular_test"]

log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.DEBUG,
    )

    mp.log_to_stderr(logger.DEBUG)
else:
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.INFO
    )

    # mp.log_to_stderr(logger.INFO)


def move_file(process_id):
    logger.debug(f'Starting File Mover Thread: {process_id}')
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        if io_process_status[process_id] != 0:
            try:
                fname = mQueue.pop()
                fd = os.open(root_dir+fname, os.O_CREAT | os.O_RDWR)
                with open(tmpfs_dir+fname, "rb") as ff:
                    chunk, offset = ff.read(1024*1024), 0
                    if modular_test:
                        target, factor = 1024, 8
                        max_speed = (target * 1024 * 1024)/8
                        second_target, second_data_count = int(max_speed/factor), 0
                        timer100ms = time.time()

                    while chunk and io_process_status[process_id] != 0:
                        os.lseek(fd, offset, os.SEEK_SET)
                        os.write(fd, chunk)
                        offset += len(chunk)
                        io_file_offsets[fname] = offset
                        # logger.info((fname, offset))
                        if modular_test:
                            second_data_count += len(chunk)
                            if second_data_count >= second_target:
                                second_data_count = 0
                                while timer100ms + (1/factor) > time.time():
                                    pass

                                timer100ms = time.time()

                        ff.seek(offset)
                        chunk = ff.read(1024*1024)

                    if io_file_offsets[fname] < transfer_file_offsets[fname]:
                        mQueue.append(fname)
                    else:
                        move_complete.value += 1
                        gQueue.put(fname)
                        logger.debug(f'I/O :: {fname} moved to {root_dir}')

                os.close(fd)
            except Exception as e:
                # logger.exception(e)
                time.sleep(0.1)

        else:
            time.sleep(0.01)

    logger.debug(f'Exiting File Mover Thread: {process_id}')


def garbage_collector(process_id):
    logger.debug(f'Starting Garbage Collector Thread: {process_id}')
    while transfer_done.value == 0 or cleanup_complete.value < transfer_complete.value:
        try:
            fname = gQueue.get_nowait()
            run(f'rm {tmpfs_dir}{fname}', logger)
            cleanup_complete.value += 1
            logger.debug(f'Cleanup :: {fname} removed from tempfs')

        except Exception as e:
            # logger.exception(e)
            time.sleep(0.1)

    logger.debug(f'Exiting Garbage Collector Thread: {process_id}')


def receive_file(sock, process_id):
    while True:
        try:
            client, address = sock.accept()
            logger.debug("{u} connected".format(u=address))
            _, free = available_space()
            while free < 25:
                time.sleep(0.01)

            if start.value == 0:
                start.value = int(time.time())

            process_status[process_id] = 1
            total = 0
            d = client.recv(1).decode()
            while d:
                header = ""
                while d != '\n':
                    header += str(d)
                    d = client.recv(1).decode()

                if file_transfer:
                    file_stats = header.split(",")
                    filename, offset, to_rcv = str(file_stats[0]), int(file_stats[1]), int(file_stats[2])

                    if direct_io:
                        fd = os.open(tmpfs_dir+filename, os.O_CREAT | os.O_RDWR | os.O_DIRECT | os.O_SYNC)
                        m = mmap.mmap(-1, to_rcv)
                    else:
                        fd = os.open(tmpfs_dir+filename, os.O_CREAT | os.O_RDWR)

                    os.lseek(fd, offset, os.SEEK_SET)
                    logger.debug("Receiving file: {0}".format(filename))
                    chunk = client.recv(chunk_size.value)

                    while chunk:
                        # logger.debug("Chunk Size: {0}".format(len(chunk)))
                        if direct_io:
                            m.write(chunk)
                            os.write(fd, m)
                        else:
                            os.write(fd, chunk)

                        to_rcv -= len(chunk)
                        total += len(chunk)
                        offset += len(chunk)
                        transfer_file_offsets[filename] = offset

                        if to_rcv > 0:
                            chunk = client.recv(min(chunk_size.value, to_rcv))
                        else:
                            logger.debug(f"Socket :: {filename} received from {address}")
                            transfer_complete.value += 1
                            io_file_offsets[filename] = 0
                            mQueue.append(filename)
                            break
                    os.close(fd)
                else:
                    chunk = client.recv(chunk_size.value)
                    while chunk:
                        chunk = client.recv(chunk_size.value)

                d = client.recv(1).decode()

            total = np.round(total/(1024*1024))
            logger.debug("{u} exited. total received {d} MB".format(u=address, d=total))
            client.close()
            process_status[process_id] = 0
        except Exception as e:
            logger.error(str(e))
            # raise e


def io_probing(params):
    global io_throughput_logs
    if transfer_done.value == 1 and move_complete.value == transfer_complete.value:
        return exit_signal

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("I/O -- Probing Parameters: {0}".format(params))

    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if i < params[0] else 0

    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    # time.sleep(n_time)
    while (time.time() < n_time) and (transfer_done.value == 0):
        time.sleep(0.1)

    thrpt = np.mean(io_throughput_logs[-2:]) if len(throughput_logs) > 2 else 0
    K = float(configurations["K"])
    # score = thrpt
    # cc_impact_lin = (K-1) * num_transfer_workers.value
    # score = thrpt * (1-cc_impact_lin)
    cc_impact_nl = K**params[0]
    score = thrpt/cc_impact_nl
    score_value = np.round(score * (-1))
    used_after, free = available_space()
    logger.info(f"Shared Memory -- Used: {used_after}GB, Free: {free}GB")
    logger.info("I/O Probing -- Throughput: {0}Mbps, Score: {1}".format(
        np.round(thrpt), score_value))

    if transfer_done.value == 1 and move_complete.value == transfer_complete.value:
        return exit_signal
    else:
        return score_value


def run_optimizer(probing_func):
    while start.value == 0:
        time.sleep(0.1)

    params = [2]

    if configurations["method"].lower() == "hill_climb":
        logger.info("Running Hill Climb Optimization .... ")
        params = hill_climb(configurations, probing_func, logger)

    elif configurations["method"].lower() == "gradient":
        logger.info("Running Gradient Optimization .... ")
        params = gradient_opt_fast(configurations, probing_func, logger)

    elif configurations["method"].lower() == "cg":
        logger.info("Running Conjugate Optimization .... ")
        params = cg_opt(configurations, probing_func)

    elif configurations["method"].lower() == "probe":
        logger.info("Running a fixed configurations Probing .... ")
        params = [configurations["fixed_probing"]["thread"]]

    else:
        logger.info("Running Bayesian Optimization .... ")
        params = base_optimizer(configurations, probing_func, logger)

    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        probing_func(params)


def report_network_throughput():
    global throughput_logs
    previous_total, previous_time = 0, 0

    while start.value <= 0:
        time.sleep(0.1)

    start_time = start.value
    while transfer_done.value == 0:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining >= 0.1:
            total_bytes = np.sum(transfer_file_offsets.values())
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            throughput_logs.append(curr_thrpt)

            # logger.info("Network Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(
            #     time_since_begining, curr_thrpt, thrpt))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def report_io_throughput():
    global io_throughput_logs
    previous_total, previous_time = 0, 0

    while start.value <= 0:
        time.sleep(0.1)

    start_time = start.value
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining >= 0.1:
            total_bytes = np.sum(io_file_offsets.values())
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            io_throughput_logs.append(curr_thrpt)

            # logger.info("I/O Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(
            #     time_since_begining, curr_thrpt, thrpt))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


if __name__ == '__main__':
    # run("rm")
    num_workers = configurations['thread_limit']
    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)

    iter = 0
    while iter<1:
        iter += 1
        logger.info(f">>>>>> Iterations: {iter} >>>>>>")


        process_status = mp.Array("i", [0 for _ in range(num_workers)])
        transfer_workers = [mp.Process(target=receive_file, args=(sock, i,)) for i in range(num_workers)]
        for p in transfer_workers:
            p.daemon = True
            p.start()

        io_workers = [mp.Process(target=move_file, args=(i,)) for i in range(num_workers)]
        for p in io_workers:
            p.daemon = True
            p.start()

        cleanup_workers = [mp.Process(target=garbage_collector, args=(i,)) for i in range(5)]
        for p in cleanup_workers:
            p.daemon = True
            p.start()


        # while True:
        #     try:
        #         time.sleep(1)
        #     except:
        #         break

        network_report_thread = Thread(target=report_network_throughput)
        network_report_thread.start()

        io_report_thread = Thread(target=report_io_throughput)
        io_report_thread.start()

        io_optimizer_thread = Thread(target=run_optimizer, args=(io_probing,))
        io_optimizer_thread.start()

        process_status[0] = 1
        alive = num_workers
        while alive>0:
            alive = sum(process_status)
            time.sleep(0.1)

        transfer_done.value = 1
        for p in transfer_workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)

        logger.info(f"Total Files Received: {transfer_complete.value}")
        while move_complete.value < transfer_complete.value:
            time.sleep(0.01)

        end.value = int(time.time())
        for p in io_workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)

        time_since_begining = np.round(end.value-start.value, 3)
        total = np.round(np.sum(transfer_file_offsets.values()) / (1024*1024*1024), 3)
        thrpt = np.round((total*8*1024)/time_since_begining,2)
        logger.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
            total, time_since_begining, thrpt))

        while cleanup_complete.value < move_complete.value:
            time.sleep(0.01)

        for p in cleanup_workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)