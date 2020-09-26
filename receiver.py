import socket
import numpy as np
import multiprocessing as mp
import logging
import time
from config import configurations

chunk_size = mp.Value("i", 0)
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
if configurations["loglevel"] == "debug":
    logger = mp.log_to_stderr(logging.DEBUG)
else:
    logger = mp.log_to_stderr(logging.INFO)


def worker(sock):
    while True:
        try:
            total = 0
            client, address = sock.accept()
            logger.debug("{u} connected".format(u=address))
            
            chunk = client.recv(chunk_size.value)
            while chunk:
                chunk = client.recv(chunk_size.value)
                total += len(chunk)
            
            total = np.round(total/(1024*1024))
            logger.debug("{u} exited. total received {d} MB".format(u=address, d=total))
            client.close()
        except Exception as e:
            logger.error(str(e))


if __name__ == '__main__':
    num_workers = configurations['limits']["thread"]
    if num_workers == -1:
        num_workers = mp.cpu_count()
        
    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)
    chunk_size.value = 1024 * 1024 * 1024

    workers = [mp.Process(target=worker, args=(sock,)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()

    while True:
        try:
            time.sleep(10)
        except:
            break
        