import socket
import os
import numpy as np
import logging
import time
from sendfile import sendfile
import multiprocessing as mp
from config import configurations

if configurations["loglevel"] == "debug":
    logger = mp.log_to_stderr(logging.DEBUG)
else:
    logger = mp.log_to_stderr(logging.INFO)
    
root = configurations["data_dir"]["sender"]
files_name = os.listdir(root)
transferred = mp.Manager().list()
BUFFER_SIZE = 256 * 1024 * 1024
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]


def worker(indx, num_workers):
    sock = socket.socket()
    sock.connect((HOST, PORT))
    
    for i in range(indx, len(files_name), num_workers):
        filename = root + files_name[i]
        file = open(filename, "rb")
        offset = 0

        logger.debug("sending {u} ...".format(u=filename))
        while True:
            sent = sendfile(sock.fileno(), file.fileno(), offset, BUFFER_SIZE)
            offset += sent
            if sent == 0:
                break
            
        transferred.append(offset)
        logger.debug("finished {u} ...".format(u=filename))
        
    sock.close()


if __name__ == '__main__':
    start = time.time()
    num_workers = 4
    
    if len(files_name) < num_workers:
        num_workers = len(files_name)

    workers = [mp.Process(target=worker, args=(i,num_workers)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()

    while True:
        if len(transferred) < len(files_name):
            time.sleep(0.01)
        else:
            end = time.time()
            time_sec = np.round(end-start,3)
            total = np.round(np.sum(transferred) / (1024*1024*1024), 3)
            thrpt = np.round((total*8*1024)/time_sec,2)
            logger.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(total, time_sec, thrpt))
            break
        