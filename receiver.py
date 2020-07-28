import socket
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
import logging as log
import time
from config import configurations

HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
if configurations["loglevel"] == "debug":
    logger = mp.log_to_stderr(log.DEBUG)
else:
    logger = mp.log_to_stderr(log.INFO)
    

def worker(sock):
    while True:
        client, address = sock.accept()
        logger.debug("{u} connected".format(u=address))
        
        chunk = client.recv(BUFFER_SIZE)
        while chunk:
            chunk = client.recv(BUFFER_SIZE)
    
    # sock.close()


if __name__ == '__main__':
    num_workers = mp.cpu_count() * 1
    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)
    
    BUFFER_SIZE = 1024 * 1024 * 1024
    total = 0

    workers = [mp.Process(target=worker, args=(sock,)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()
    
    # thread_pool = ThreadPoolExecutor(num_workers)
    # for i in range(num_workers):
    #     thread_pool.submit(worker, sock,)

    while True:
        try:
            time.sleep(10)
        except:
            break