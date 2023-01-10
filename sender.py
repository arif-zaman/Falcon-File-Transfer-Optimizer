import os
import shutil
import signal
import time
import socket
import warnings
import datetime
import numpy as np
import logging as logger
import multiprocessing as mp
from threading import Thread
from config_sender import configurations
from search import base_optimizer, hill_climb, cg_opt, gradient_opt_fast, gradient_multivariate
from utils import tcp_stats, run, available_space
warnings.filterwarnings("ignore", category=FutureWarning)


def copy_file(process_id):
    while rQueue:
        if io_process_status[process_id] == 1:
            logger.debug(f'Starting Copying Thread: {process_id}')
            try:
                used, _ = available_space(tmpfs_dir)
                if used < memory_limit:
                    file_id, offset = rQueue.popitem()
                    if file_transfer:
                        fname = file_names[file_id]
                        fd = os.open(tmpfs_dir+fname, os.O_CREAT | os.O_RDWR)
                        block_size = chunk_size
                        if io_limit > 0:
                            target, factor = io_limit, 8
                            max_speed = (target * 1024 * 1024)/8
                            second_target, second_data_count = int(max_speed/factor), 0
                            block_size = min(block_size, second_target)
                            timer100ms = time.time()

                        with open(root_dir+fname, "rb") as ff:
                            ff.seek(int(offset))
                            chunk = ff.read(block_size)

                            os.lseek(fd, int(offset), os.SEEK_SET)
                            offset_update = time.time()
                            while chunk and io_process_status[process_id] == 1:
                                os.write(fd, chunk)
                                offset += len(chunk)

                                ## Update every 100 milliseconds
                                if time.time()-offset_update > 0.1:
                                    io_file_offsets[file_id] = offset
                                    offset_update = time.time()

                                if io_limit > 0:
                                    second_data_count += len(chunk)
                                    if second_data_count >= second_target:
                                        second_data_count = 0
                                        while timer100ms + (1/factor) > time.time():
                                            pass

                                        timer100ms = time.time()

                                chunk = ff.read(block_size)

                            io_file_offsets[file_id] = offset
                            if offset < file_sizes[file_id]:
                                logger.debug(f"I/O - file: {file_id}, offset: {offset}, size: {file_sizes[file_id]}")
                                rQueue[file_id] = offset
                            else:
                                logger.debug(f'I/O :: {file_id}')

                            if offset>0 and file_id not in tQueue:
                                tQueue[file_id] = 0

                        os.close(fd)

                    else:
                        io_file_offsets[file_id] = file_sizes[file_id]
                        tQueue[file_id] = 0
            except KeyError:
                time.sleep(0.1)

            except Exception as e:
                logger.debug(str(e))
                time.sleep(0.1)

            logger.debug(f'Exiting Copying Thread: {process_id}')

    io_process_status[process_id] = 0


def garbage_collector(process_id):
    logger.debug(f'Starting Garbage Collector Thread: {process_id}')
    while rQueue or tQueue:
        try:
            file_id = gQueue.pop()
            fname = file_names[file_id]
            run(f'rm {tmpfs_dir}{fname}', logger)
            logger.debug(f'Cleanup :: {file_id}!')

        except IndexError:
            time.sleep(1)

        except Exception as e:
            logger.debug(str(e))
            time.sleep(0.1)

    logger.debug(f'Exiting Garbage Collector Thread: {process_id}')


def transfer_file(process_id):
    while rQueue or tQueue:
        if transfer_process_status[process_id] == 1:
            try:
                logger.debug(f'Starting TCP Socket Thread: {process_id}')
                sock = socket.socket()
                sock.settimeout(3)
                sock.connect((HOST, PORT))
            except socket.timeout as e:
                # logger.exception(e)
                continue

            while transfer_process_status[process_id] == 1:
                try:
                    file_id, offset = tQueue.popitem()

                    if network_limit>0:
                        target, factor = network_limit, 8
                        max_speed = (target * 1024 * 1024)/8
                        second_target, second_data_count = int(max_speed/factor), 0

                    to_send = io_file_offsets[file_id] - offset
                    if (to_send > 0) and (transfer_process_status[process_id] == 1):
                        filename = tmpfs_dir + file_names[file_id]
                        if file_transfer:
                            file = open(filename, "rb")

                        msg = f"{file_count},{file_names[file_id]},{int(offset)},{int(to_send)}\n"
                        sock.send(msg.encode())
                        logger.debug(f"starting {process_id}, {filename}, {offset}, {len(tQueue)}")

                        timer100ms = offset_update = time.time()
                        while (to_send > 0) and (transfer_process_status[process_id] == 1):
                            if network_limit>0:
                                block_size = min(chunk_size, second_target-second_data_count, to_send)
                            else:
                                block_size = min(chunk_size, to_send)

                            if file_transfer:
                                    sent = sock.sendfile(file=file, offset=int(offset), count=int(block_size))
                            else:
                                data_to_send = bytearray(int(block_size))
                                sent = sock.send(data_to_send)

                            offset += sent
                            to_send -= sent

                            ## Update every 100 milliseconds
                            if time.time() - offset_update >= 0.1:
                                transfer_file_offsets[file_id] = offset
                                offset_update = time.time()

                            if network_limit>0:
                                second_data_count += sent
                                if second_data_count >= second_target:
                                    second_data_count = 0
                                    while timer100ms + (1/factor) > time.time():
                                        pass

                                    timer100ms = time.time()

                        if file_transfer:
                            file.close()

                        transfer_file_offsets[file_id] = offset
                        if offset < io_file_offsets[file_id] or file_id in rQueue:
                            logger.debug(f"Transfer - file: {file_id}, offset: {offset}, size: {file_sizes[file_id]}")
                            tQueue[file_id] = offset
                        else:
                            logger.debug(f'Transfer :: {file_id}!')
                            if file_transfer:
                                gQueue.append(file_id)

                    else:
                        tQueue[file_id] = offset
                except KeyError:
                    time.sleep(0.1)
                    continue

                except Exception as e:
                    transfer_process_status[process_id] = 0
                    logger.debug("Process: {0}, Error: {1}".format(process_id, str(e)))

            logger.debug(f'Exiting TCP Socket Thread: {process_id}')
            sock.close()

    transfer_process_status[process_id] = 0


def network_probing(params):
    global network_throughput_logs, exit_signal

    if not rQueue and not tQueue:
        return exit_signal

    while not tQueue:
        time.sleep(0.1)

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("Network -- Probing Parameters: {0}".format(params))
    for i in range(len(transfer_process_status)):
        transfer_process_status[i] = 1 if i < params[0] else 0

    logger.debug("Active CC - Socket: {0}".format(np.sum(transfer_process_status)))
    time.sleep(1)
    prev_sc, prev_rc = tcp_stats(RCVR_ADDR, logger)
    n_time = time.time() + probing_time - 1.05
    # time.sleep(n_time)
    while (time.time() < n_time) and (rQueue or tQueue):
        time.sleep(0.1)

    curr_sc, curr_rc = tcp_stats(RCVR_ADDR, logger)
    sc, rc = curr_sc - prev_sc, curr_rc - prev_rc

    logger.debug("TCP Segments >> Send Count: {0}, Retrans Count: {1}".format(sc, rc))
    thrpt = np.mean(network_throughput_logs[-2:]) if len(network_throughput_logs) > 2 else 0

    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])
    if sc != 0:
        lr = rc/sc if sc>rc else 0

    # score = thrpt
    plr_impact = B*lr
    # cc_impact_lin = (K-1) * params[0]
    # score = thrpt * (1- plr_impact - cc_impact_lin)
    cc_impact_nl = K**params[0]
    score = (thrpt/cc_impact_nl) - (thrpt * plr_impact)
    score_value = np.round(score * (-1))

    logger.info(f"rQueue:{len(rQueue)}, tQueue:{len(tQueue)}, gQueue: {len(gQueue)}")
    logger.info("Network Probing -- Throughput: {0}Mbps, Loss Rate: {1}%, Score: {2}".format(
        np.round(thrpt), np.round(lr*100, 2), score_value))

    if not rQueue and not tQueue:
        return exit_signal
    else:
        return score_value


def io_probing(params):
    global io_throughput_logs, exit_signal

    if not rQueue:
        return exit_signal

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("I/O -- Probing Parameters: {0}".format(params))
    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if i < params[0] else 0

    logger.debug("Active CC - I/O: {0}".format(np.sum(io_process_status)))
    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    used_before, _ = available_space(tmpfs_dir)
    # time.sleep(n_time)
    while (time.time() < n_time) and (rQueue or tQueue):
        time.sleep(0.1)

    used_disk, free = available_space(tmpfs_dir)
    logger.info(f"Shared Memory -- Used: {used_disk}GB, Free: {free}GB")
    thrpt = np.mean(io_throughput_logs[-2:]) if len(io_throughput_logs) > 2 else 0
    K = float(configurations["K"])
    limit = min(configurations["memory_use"]["threshold"], memory_limit//2)
    # storage_cost = K + max(0,used_disk-limit)/(limit*10)
    # cc_impact_nl = storage_cost**params[0]
    # score = thrpt/cc_impact_nl
    # score_value = np.round(score * (-1))

    storage_cost = 0
    if used_disk>limit and used_disk > used_before:
        storage_cost = (used_disk - used_before) / used_disk

    cc_impact_nl = K**params[0]
    score = thrpt/cc_impact_nl - thrpt*storage_cost
    score_value = np.round(score * (-1))

    # logger.info(f"rQueue:{len(rQueue)}, tQueue:{len(tQueue)}, gQueue: {len(gQueue)}")
    logger.info(f"I/O Probing -- Throughput: {np.round(thrpt)}Mbps, Score: {score_value}")
    if not rQueue:
        return exit_signal
    else:
        return score_value


def multi_params_probing(params):
    global io_throughput_logs, network_throughput_logs, exit_signal
    # global io_weight, net_weight

    if not rQueue and not tQueue:
        return [exit_signal, exit_signal]

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("Probing Parameters - [Network, I/O]: {0}".format(params))

    for i in range(len(transfer_process_status)):
        transfer_process_status[i] = 1 if i < params[0] else 0

    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if (i < params[1] and rQueue) else 0

    time.sleep(1)

    # Before
    prev_sc, prev_rc = tcp_stats(RCVR_ADDR, logger)
    n_time = time.time() + probing_time - 1.05
    used_before, _ = available_space(tmpfs_dir)
    # Sleep
    # time.sleep(n_time)
    while (time.time() < n_time) and (rQueue or tQueue):
        time.sleep(0.1)

    # After
    curr_sc, curr_rc = tcp_stats(RCVR_ADDR, logger)
    sc, rc = curr_sc - prev_sc, curr_rc - prev_rc
    logger.debug("TCP Segments >> Send Count: {0}, Retrans Count: {1}".format(sc, rc))
    used_disk, free = available_space(tmpfs_dir)

    ## Network Score
    net_thrpt = np.round(np.mean(network_throughput_logs[-2:])) if len(network_throughput_logs) > 2 else 0
    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])
    if sc != 0:
        lr = rc/sc if sc>rc else 0

    plr_impact = B*lr
    # cc_impact_lin = (K-1) * params[0]
    # net_score = net_thrpt * (1 - plr_impact - cc_impact_lin)
    cc_impact_nl = K**params[0]
    net_score = (net_thrpt/cc_impact_nl) - (net_thrpt * plr_impact)
    net_score_value = np.round(net_score * (-1))

    ## I/O score
    if rQueue:
        io_thrpt = np.round(np.mean(io_throughput_logs[-2:])) if len(io_throughput_logs) > 2 else 0
        limit = min(configurations["memory_use"]["threshold"], memory_limit//2)
        storage_cost = K + max(0,used_disk-limit)/(limit*10)
        cc_impact_nl = storage_cost**params[1]
        # io_score = io_thrpt/cc_impact_nl
        # io_score_value = np.round(io_score * (-1))

        storage_cost = 0
        if used_disk>limit and used_disk > used_before:
            storage_cost = (used_disk - used_before) / used_before

        cc_impact_nl = K**params[0]
        io_score = io_thrpt/cc_impact_nl - io_thrpt*storage_cost
        io_score_value = np.round(io_score * (-1))

        # if io_weight == net_weight:
        #     net_weight = (io_thrpt/params[1]) / ((io_thrpt/params[1]) + (net_thrpt/params[0]))
        #     io_weight = 1-net_weight
        #     logger.info(f"Weight: I/O - {io_weight}, Network - {net_weight}")
    else:
        io_score_value = 0
        # io_weight = 0
        # net_weight = 1
        io_thrpt = 0

    # score_value = io_weight * io_score_value + net_weight * net_score_value
    logger.info(f"Shared Memory -- Used: {used_disk}GB, Free: {free}GB")
    logger.info(f"rQueue:{len(rQueue)}, tQueue:{len(tQueue)}, gQueue: {len(gQueue)}")

    if not rQueue:
        io_score_value = exit_signal

    if not rQueue and not tQueue:
        net_score_value = exit_signal

    logger.info(f"Probing -- I/O: {io_thrpt}Mbps, Network: {net_thrpt}Mbps")
    return [net_score_value, io_score_value] #score_value


def normal_transfer(params):
    if len(params) != 2:
        params = [2,2]

    logger.info("Normal Transfer -- Probing Parameters [Network, I/O]: {0}".format(params))

    for i in range(len(transfer_process_status)):
        transfer_process_status[i] = 1 if i < params[0] else 0

    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if i < params[1] else 0

    while rQueue or tQueue:
        time.sleep(0.1)


def run_optimizer(probing_func):
    params = [2,2]

    if configurations["mp_opt"]:
        if configurations["method"].lower() == "cg":
            logger.info("Running Conjugate Optimization .... ")
            params = cg_opt(configurations, probing_func)

        elif configurations["method"].lower() == "mgd":
            logger.info("Running Multivariate Gradient Optimization .... ")
            params = gradient_multivariate(configurations, probing_func, logger)
        else:
            logger.info("Running Bayesian Optimization .... ")
            params = base_optimizer(configurations, probing_func, logger)

    else:
        if configurations["method"].lower() == "hill_climb":
            logger.info("Running Hill Climb Optimization .... ")
            params = hill_climb(configurations, probing_func, logger)

        elif configurations["method"].lower() == "gradient":
            logger.info("Running Gradient Optimization .... ")
            params = gradient_opt_fast(configurations["thread_limit"], probing_func, logger)

        elif configurations["method"].lower() == "cg":
            logger.info("Running Conjugate Optimization .... ")
            params = cg_opt(configurations, probing_func)

        elif configurations["method"].lower() == "probe":
            logger.info("Running a fixed configurations Probing .... ")
            params = [configurations["fixed_probing"]["thread"], configurations["fixed_probing"]["thread"]]

        else:
            logger.info("Running Bayesian Optimization .... ")
            params = base_optimizer(configurations, probing_func, logger)


    if rQueue or tQueue:
        normal_transfer(params)


def report_network_throughput(start_time):
    global network_throughput_logs
    previous_total = 0
    previous_time = 0

    while rQueue or tQueue:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining>10:
            if sum(network_throughput_logs[-10:]) == 0:
                logger.info(f"transfer queue: {tQueue}")
                rQueue.clear()
                tQueue.clear()

        if time_since_begining >= 0.1:
            total_bytes = np.sum(transfer_file_offsets)
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            network_throughput_logs.append(curr_thrpt)
            # logger.info(f"Network Throughput @{time_since_begining}s, Current: {curr_thrpt}Mbps, Average: {thrpt}Mbps")
            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def report_io_throughput(start_time):
    global io_throughput_logs
    previous_total = 0
    previous_time = 0

    while rQueue:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining >= 0.1:
            total_bytes = np.sum(io_file_offsets)
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            io_throughput_logs.append(curr_thrpt)
            # logger.info(f"I/O Throughput @{time_since_begining}s, Current: {curr_thrpt}Mbps, Average: {thrpt}Mbps")

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def graceful_exit(signum=None, frame=None):
    logger.debug((signum, frame))
    try:
        rQueue.clear()
        tQueue.clear()
    except Exception as e:
        logger.debug(e)

    shutil.rmtree(tmpfs_dir, ignore_errors=True)
    exit(1)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    net_cc = configurations["max_cc"]["network"]
    configurations["network_thread_limit"] = net_cc if net_cc>0 else mp.cpu_count()

    io_cc = configurations["max_cc"]["io"]
    configurations["io_thread_limit"] = io_cc if io_cc>0 else mp.cpu_count()

    log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
    log_file = f'logs/sender.{datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'

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

    network_limit = -1
    if "network_limit" in configurations and configurations["network_limit"] is not None:
        network_limit = configurations["network_limit"]

    io_limit = -1
    if "io_limit" in configurations and configurations["io_limit"] is not None:
        io_limit = int(configurations["io_limit"])

    file_transfer = True
    if "file_transfer" in configurations and configurations["file_transfer"] is not None:
        file_transfer = configurations["file_transfer"]

    manager = mp.Manager()
    root_dir = configurations["data_dir"]
    tmpfs_dir = f"/dev/shm/data{os.getpid()}/"
    probing_time = configurations["probing_sec"]
    file_names = os.listdir(root_dir)[:] * configurations["multiplier"]
    file_sizes = [os.path.getsize(root_dir+filename) for filename in file_names]
    file_count = len(file_names)
    network_throughput_logs = manager.list()
    io_throughput_logs = manager.list()
    exit_signal = 10 ** 10
    chunk_size = 1 * 1024 * 1024
    transfer_process_status = mp.Array("i", [0 for i in range(configurations["network_thread_limit"])])
    io_process_status = mp.Array("i", [0 for i in range(configurations["io_thread_limit"])])
    transfer_file_offsets = mp.Array("d", [0 for i in range(file_count)])
    io_file_offsets = mp.Array("d", [0 for i in range(file_count)])

    # io_weight, net_weight = 1, 1

    HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
    RCVR_ADDR = str(HOST) + ":" + str(PORT)

    try:
        os.mkdir(tmpfs_dir)
    except Exception as e:
        logger.debug(str(e))
        exit(1)

    _, free = available_space(tmpfs_dir)
    memory_limit = min(configurations["memory_use"]["maximum"], free/2)

    rQueue = manager.dict()
    tQueue = manager.dict()
    gQueue = manager.list()

    for i in range(file_count):
        rQueue[i] = 0

    copy_workers = [mp.Process(target=copy_file, args=(i,)) for i in range(configurations["io_thread_limit"])]
    for p in copy_workers:
        p.daemon = True
        p.start()

    transfer_workers = [mp.Process(target=transfer_file, args=(i,)) for i in range(configurations["network_thread_limit"])]
    for p in transfer_workers:
        p.daemon = True
        p.start()

    cleanup_workers = [mp.Process(target=garbage_collector, args=(i,)) for i in range(5)]
    for p in cleanup_workers:
        p.daemon = True
        p.start()

    start = time.time()
    # reporting_process = mp.Process(target=report_network_throughput, args=(start,))
    # reporting_process.daemon = True
    # reporting_process.start()
    network_report_thread = Thread(target=report_network_throughput, args=(start,))
    network_report_thread.start()

    io_report_thread = Thread(target=report_io_throughput, args=(start,))
    io_report_thread.start()

    if configurations["mp_opt"]:
        optimizer_thread = Thread(target=run_optimizer, args=(multi_params_probing,))
        optimizer_thread.start()

    else:
        io_optimizer_thread = Thread(target=run_optimizer, args=(io_probing,))
        io_optimizer_thread.start()

        network_optimizer_thread = Thread(target=run_optimizer, args=(network_probing,))
        network_optimizer_thread.start()

    while rQueue or tQueue:
        time.sleep(1)

    end = time.time()
    time_since_begining = np.round(end-start, 3)
    total = np.round(np.sum(file_sizes) / (1024*1024*1024), 3)
    thrpt = np.round((total*8*1024)/time_since_begining,2)
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

    shutil.rmtree(tmpfs_dir, ignore_errors=True)
