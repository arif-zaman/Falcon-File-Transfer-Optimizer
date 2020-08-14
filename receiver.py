import socket
import numpy as np
import multiprocessing as mp
import logging as log
import time
from config import configurations

HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
if configurations["loglevel"] == "debug":
    logger = mp.log_to_stderr(log.DEBUG)
else:
    logger = mp.log_to_stderr(log.INFO)

buffer_size = mp.Value("i", 0)


def worker(sock):
    while True:
        total = 0
        client, address = sock.accept()
        logger.debug("{u} connected".format(u=address))
        
        chunk = client.recv(buffer_size.value)
        while chunk:
            chunk = client.recv(buffer_size.value)
            total += len(chunk)
        
        total = np.round(total/(1024*1024))
        logger.debug("{u} exited. total received {d} MB".format(u=address, d=total))
        client.close()


if __name__ == '__main__':
    num_workers = mp.cpu_count() * 1
    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)
    buffer_size.value = 1024 * 1024 * 1024

    workers = [mp.Process(target=worker, args=(sock,)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()

    while True:
        try:
            time.sleep(10)
        except:
            break
        