import socket
import os
import numpy as np
import logging
import time
import warnings
from sendfile import sendfile
import multiprocessing as mp
from config import configurations
from skopt.space import Integer
from skopt import gp_minimize, forest_minimize, dummy_minimize
warnings.filterwarnings("ignore", category=FutureWarning)

if configurations["loglevel"] == "debug":
    logger = mp.log_to_stderr(logging.DEBUG)
else:
    logger = mp.log_to_stderr(logging.INFO)
    
root = configurations["data_dir"]["sender"]
probing_time = 1
files_name = os.listdir(root) * configurations["multiplier"]
score = mp.Value("d", 0.0)
process_done = mp.Value("i", 0)
transfer_status = mp.Array("i", [0 for i in range(len(files_name))])
file_offsets = mp.Array("d", [0.0 for i in range(len(files_name))])
# transferred = mp.Manager().list()
# end_time = mp.Manager().list()
# BUFFER_SIZE = 256 * 1024 * 1024
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

            logger.debug("sending {u} ...".format(u=filename))
            total_sent = 0
            while True:
                sent = sendfile(sock.fileno(), file.fileno(), offset, buffer_size)
                offset += sent
                total_sent += sent
                
                duration = time.time() - start
                if sample_transfer and (duration > probing_time):
                    score.value = score.value - (total_sent/duration)
                    
                    if sent == 0:
                        transfer_status[i] = 1
                        logger.debug("finished {u} ...".format(u=filename))
                        
                    break
                
                if sent == 0:
                    transfer_status[i] = 1
                    logger.debug("finished {u} ...".format(u=filename)) 
                    break
                
            file_offsets[i] = offset
    
    process_done.value = process_done.value + 1
    sock.close()    


def get_buffer_size(unit):
    return (2 ** (unit-1)) * 1024


def do_transfer(params, sample_transfer=True):
    score.value = 0.0
    process_done.value = 0
    num_workers = params[0]
    buffer_size = get_buffer_size(params[1])
    logger.info(params)
    
    if len(files_name) < num_workers:
        num_workers = len(files_name)

    workers = [mp.Process(target=worker, args=(buffer_size,i,num_workers, sample_transfer)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()
    
    while process_done.value < num_workers:
            time.sleep(0.01)

    return score.value


def bayes_opt():
    search_space  = [
        Integer(1, mp.cpu_count(), name='transfer_threads'),
        Integer(1, 20, name='bsize')
    ]
    
    experiments = gp_minimize(
        do_transfer,
        search_space,
        acq_func="EI",
        n_calls=10,
        n_random_starts=5,
        random_state=0,
        verbose=True,
        xi=0.001
    )
    
    logger.info("Best parameters: %s and score: %d" % (experiments.x, experiments.fun))
    do_transfer(experiments.x, sample_transfer=False)


if __name__ == '__main__':
    start = time.time()
    bayes_opt()
    end = time.time()
    time_sec = np.round(end-start, 3)
    total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
    thrpt = np.round((total*8*1024)/time_sec,2)
    logger.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(total, time_sec, thrpt))
    # while True:
    #     if len(transferred) < len(files_name):
    #         time.sleep(0.01)
    #     else:
    #         time_sec = np.round(np.max(end_time), 3)
    #         total = np.round(np.sum(transferred) / (1024*1024*1024), 3)
    #         thrpt = np.round((total*8*1024)/time_sec,2)
    #         logger.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(total, time_sec, thrpt))
    #         break
        