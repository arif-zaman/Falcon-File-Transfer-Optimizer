import os
import mmap
import socket
from os import environ
from redis import Redis
import numpy as np
import multiprocessing as mp
import logging
import time
from config_receiver import configurations
import scitokens

chunk_size = mp.Value("i", 1024*1024)
root = configurations["data_dir"]
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
audience = "dtn2.cs.unr.edu"
issuer = "https://hpcn.unr.edu"

redis_host = environ.get("REDIS_HOSTNAME", "134.197.113.70")
redis_port = environ.get("REDIS_PORT", 6379)
redis_stream_key = "falcon-transfer:{0}".format(audience)

if configurations["loglevel"] == "debug":
    logger = mp.log_to_stderr(logging.DEBUG)
else:
    logger = mp.log_to_stderr(logging.INFO)

file_transfer = True
if "file_transfer" in configurations and configurations["file_transfer"] is not None:
    file_transfer = configurations["file_transfer"]

def worker(sock, process_num):
    while True:
        try:
            client, address = sock.accept()
            logger.info("{u} connected".format(u=address))
            process_status[process_num] = 1
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

#                     m = mmap.mmap(-1, chunk_size.value)
#                     fd = os.open(root + filename, os.O_CREAT | os.O_DIRECT | os.O_TRUNC | os.O_RDWR) #, os.O_CREAT | os.O_DIRECT | os.O_TRUNC | os.O_RDWR
#                     os.lseek(fd, offset, os.SEEK_SET)
                    file = open(root + filename, "wb+")
                    file.seek(offset)
                    logger.debug("Receiving file: {0}".format(filename))

                    chunk = client.recv(chunk_size.value)
#                     m.write(chunk)
                    while chunk:
                        logger.debug("Chunk Size: {0}".format(len(chunk)))
                        file.write(chunk)
#                         os.write(fd, m)
                        to_rcv -= len(chunk)
                        total += len(chunk)

                        if to_rcv > 0:
                            chunk = client.recv(min(chunk_size.value, to_rcv))
                        else:
                            logger.debug("Successfully received file: {0}".format(filename))
                            break

                    file.close()
#                     os.close(fd)
                else:
                    chunk = client.recv(chunk_size.value)
                    while chunk:
                        chunk = client.recv(chunk_size.value)

                d = client.recv(1).decode()

            total = np.round(total/(1024*1024))
            logger.info("{u} exited. total received {d} MB".format(u=address, d=total))
            client.close()
            process_status[process_num] = 0
        except Exception as e:
            logger.error(str(e))
            # raise e


def always_accept(value):
    if value or not value:
        return True


def load_public_pem(filename):
    with open(filename, 'rb') as pem_in:
        pem = pem_in.read()

    return pem


if __name__ == '__main__':
    # Redis Connection
    r = Redis(redis_host, redis_port, retry_on_timeout=True)

    # Get Remote Server Public Key
    public_pem = load_public_pem('publickey.pem')

    num_workers = configurations['max_cc']
    if num_workers == -1:
        num_workers = mp.cpu_count()

    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)

    process_status = mp.Array("i", [0 for _ in range(num_workers)])
    workers = [mp.Process(target=worker, args=(sock, i,)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()

    process_status[0] = 1
    alive = num_workers
    while alive>0:
        alive = 0
        for i in range(num_workers):
            if process_status[i] == 1:
                alive += 1

        time.sleep(0.1)

    for p in workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)
