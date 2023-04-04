import os
import shutil
import signal
import mmap
import time
import socket
import warnings
import datetime
import logging as logger
import numpy as np
import multiprocessing as mp
from threading import Thread
from config_receiver import configurations
from utils import available_space, get_dir_size, run
from search import base_optimizer, hill_climb, cg_opt, gradient_opt_fast, exit_signal
warnings.filterwarnings("ignore", category=FutureWarning)


def move_file(process_id):
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        if io_process_status[process_id] != 0 and mQueue:
            logger.debug(f'Starting File Mover Thread: {process_id}')
            try:
                fname = mQueue.pop()
                fd = os.open(root_dir+fname, os.O_CREAT | os.O_RDWR)
                block_size = chunk_size
                if io_limit > 0:
                    target, factor = io_limit, 8
                    max_speed = (target * 1024 * 1024)/8
                    second_target, second_data_count = int(max_speed/factor), 0
                    block_size = min(block_size, second_target)
                    timer100ms = time.time()

                with open(tmpfs_dir+fname, "rb") as ff:
                    chunk, offset = ff.read(block_size), 0
                    if fname in io_file_offsets:
                        offset = int(io_file_offsets[fname])

                    while chunk and io_process_status[process_id] != 0:
                        os.lseek(fd, offset, os.SEEK_SET)
                        os.write(fd, chunk)
                        offset += len(chunk)
                        io_file_offsets[fname] = offset
                        # logger.debug((fname, offset))
                        if io_limit > 0:
                            second_data_count += len(chunk)
                            if second_data_count >= second_target:
                                second_data_count = 0
                                while timer100ms + (1/factor) > time.time():
                                    pass

                                timer100ms = time.time()

                        ff.seek(offset)
                        chunk = ff.read(block_size)

                    if io_file_offsets[fname] < transfer_file_offsets[fname]:
                        mQueue.append(fname)
                    else:
                        move_complete.value += 1
                        logger.debug(f'I/O :: {fname}')
                        run(f'rm {tmpfs_dir}{fname}', logger)
                        logger.debug(f'Cleanup :: {fname}')

                os.close(fd)

            except IndexError:
                time.sleep(0.1)

            except Exception as e:
                # logger.exception(e)
                time.sleep(0.1)

            logger.debug(f'Exiting File Mover Thread: {process_id}')
        else:
            time.sleep(0.1)


def receive_file(sock, process_id):
    while transfer_done.value != 1:
        try:
            client, address = sock.accept()
            logger.debug("{u} connected".format(u=address))
            used = get_dir_size(logger,tmpfs_dir)
            while used > memory_limit:
                time.sleep(0.1)

            if start.value == 0:
                start.value = int(time.time())

            transfer_process_status[process_id] = 1
            total = 0
            d = client.recv(1).decode()
            while d:
                header = ""
                while d != '\n':
                    header += str(d)
                    d = client.recv(1).decode()

                if file_transfer:
                    file_stats = header.split(",")
                    tq_size, rq_size = int(file_stats[0]), int(file_stats[1]),
                    filename = str(file_stats[2])
                    offset, to_rcv = int(file_stats[3]), int(file_stats[4])

                    if direct_io:
                        fd = os.open(tmpfs_dir+filename, os.O_CREAT | os.O_RDWR | os.O_DIRECT | os.O_SYNC)
                        m = mmap.mmap(-1, to_rcv)
                    else:
                        fd = os.open(tmpfs_dir+filename, os.O_CREAT | os.O_RDWR)

                    os.lseek(fd, offset, os.SEEK_SET)
                    logger.debug("Receiving file: {0}".format(filename))
                    chunk = client.recv(chunk_size)

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
                            chunk = client.recv(min(chunk_size, to_rcv))
                        else:
                            logger.debug(f"Socket :: {filename}")
                            transfer_complete.value += 1
                            io_file_offsets[filename] = 0
                            mQueue.append(filename)
                            break
                    os.close(fd)

                    if rq_size == 0 and tq_size == 0:
                        transfer_done.value = 1
                else:
                    chunk = client.recv(chunk_size)
                    while chunk:
                        chunk = client.recv(chunk_size)

                d = client.recv(1).decode()

            total = np.round(total/(1024*1024))
            logger.debug("{u} exited. total received {d} MB".format(u=address, d=total))
            client.close()
            transfer_process_status[process_id] = 0
        except Exception as e:
            logger.debug(str(e))
            # raise e


def io_probing(params):
    global io_throughput_logs
    if transfer_done.value == 1 and move_complete.value >= transfer_complete.value:
        return exit_signal

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("I/O -- Probing Parameters: {0}".format(params))

    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if i < params[0] else 0

    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    # time.sleep(n_time)
    while (time.time() < n_time) and (transfer_done.value == 0 or move_complete.value < transfer_complete.value):
        time.sleep(0.1)

    thrpt = np.mean(io_throughput_logs[-2:]) if len(throughput_logs) > 2 else 0
    K = float(configurations["K"])
    # score = thrpt
    # cc_impact_lin = (K-1) * num_transfer_workers.value
    # score = thrpt * (1-cc_impact_lin)
    cc_impact_nl = K**params[0]
    score = thrpt/cc_impact_nl
    score_value = np.round(score * (-1))
    used = get_dir_size(logger, tmpfs_dir)
    logger.info(f"Shared Memory -- Used: {used}GB")
    logger.info("I/O Probing -- Throughput: {0}Mbps, Score: {1}".format(
        np.round(thrpt), score_value))

    if transfer_done.value == 1 and move_complete.value >= transfer_complete.value:
        return exit_signal
    else:
        return score_value


def run_optimizer(probing_func):
    while start.value == 0:
        time.sleep(0.1)

    params = [2]
    if configurations["method"].lower() == "hill_climb":
        logger.info("Running Hill Climb Optimization .... ")
        params = hill_climb(configurations["thread_limit"], probing_func, logger)

    elif configurations["method"].lower() == "gradient":
        logger.info("Running Gradient Optimization .... ")
        params = gradient_opt_fast(configurations["thread_limit"], probing_func, logger)

    elif configurations["method"].lower() == "cg":
        logger.info("Running Conjugate Optimization .... ")
        params = cg_opt(False, probing_func)

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

    while start.value == 0:
        time.sleep(0.1)

    start_time = start.value
    while transfer_done.value == 0:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining>15:
            if sum(throughput_logs[-15:]) == 0:
                transfer_done.value  = 1
                break

        if time_since_begining >= 0.1:
            total_bytes = np.sum(transfer_file_offsets.values())
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            throughput_logs.append(curr_thrpt)

            logger.info("Network Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(
                time_since_begining, curr_thrpt, thrpt))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def report_io_throughput():
    global io_throughput_logs
    previous_total, previous_time = 0, 0

    while start.value == 0:
        time.sleep(0.1)

    start_time = start.value
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining>15:
            if sum(io_throughput_logs[-15:]) == 0:
                transfer_done.value = 1
                move_complete.value = transfer_complete.value
                break

        if time_since_begining >= 0.1:
            total_bytes = np.sum(io_file_offsets.values())
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            io_throughput_logs.append(curr_thrpt)

            logger.info("I/O Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(
                time_since_begining, curr_thrpt, thrpt))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def graceful_exit(signum=None, frame=None):
    logger.debug((signum, frame))
    try:
        sock.close()
        transfer_done.value  = 1
        move_complete.value = transfer_complete.value
        # time.sleep()
        shutil.rmtree(tmpfs_dir, ignore_errors=True)
    except Exception as e:
        logger.debug(e)

    exit(1)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
    log_file = f'logs/receiver.{datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'

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

    configurations["cpu_count"] = mp.cpu_count()
    configurations["thread_limit"] = configurations["max_cc"]

    if configurations["thread_limit"] == -1:
        configurations["thread_limit"] = configurations["cpu_count"]

    chunk_size = 1024*1024
    root_dir = configurations["data_dir"]
    tmpfs_dir = f"/dev/shm/data{os.getpid()}/"
    probing_time = configurations["probing_sec"]
    HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
    transfer_complete = mp.Value("i", 0)
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)
    io_process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
    transfer_file_offsets = mp.Manager().dict()
    io_file_offsets = mp.Manager().dict() ## figure out file_count
    throughput_logs = mp.Manager().list()
    io_throughput_logs = mp.Manager().list()

    mQueue = mp.Manager().list()
    start, end = mp.Value("i", 0), mp.Value("i", 0)

    direct_io = False
    file_transfer = True
    if "file_transfer" in configurations and configurations["file_transfer"] is not None:
        file_transfer = configurations["file_transfer"]

    io_limit = -1
    if "io_limit" in configurations and configurations["io_limit"] is not None:
        io_limit = int(configurations["io_limit"])

    try:
        os.mkdir(tmpfs_dir)
    except Exception as e:
        logger.debug(e)
        exit(1)

    _, free = available_space(tmpfs_dir)
    memory_limit = min(50, free/2)
    num_workers = configurations['thread_limit']

    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)
    transfer_process_status = mp.Array("i", [0 for _ in range(num_workers)])
    transfer_workers = [mp.Process(target=receive_file, args=(sock, i,)) for i in range(num_workers)]
    for p in transfer_workers:
        p.daemon = True
        p.start()

    io_workers = [mp.Process(target=move_file, args=(i,)) for i in range(num_workers)]
    for p in io_workers:
        p.daemon = True
        p.start()

    network_report_thread = Thread(target=report_network_throughput)
    network_report_thread.start()

    io_report_thread = Thread(target=report_io_throughput)
    io_report_thread.start()

    io_optimizer_thread = Thread(target=run_optimizer, args=(io_probing,))
    io_optimizer_thread.start()

    # transfer_process_status[0] = 1
    # while sum(transfer_process_status)>0:
    while transfer_done.value == 0:
        time.sleep(0.1)

    logger.info(f"Transfer Tasks Completed!")
    # transfer_done.value = 1
    time.sleep(1)

    for p in transfer_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)

    while move_complete.value < transfer_complete.value:
        time.sleep(0.1)

    time.sleep(1)
    for p in io_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)

    shutil.rmtree(tmpfs_dir, ignore_errors=True)
    logger.debug(f"Transfer Completed!")
    exit(1)