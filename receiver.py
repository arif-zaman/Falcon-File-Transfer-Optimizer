import os
import mmap
import time
import socket
import logging
import numpy as np
import multiprocessing as mp
from config_receiver import configurations

chunk_size = mp.Value("i", 1024*1024)
root = configurations["data_dir"]
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]

if configurations["loglevel"] == "debug":
    log = mp.log_to_stderr(logging.DEBUG)
else:
    log = mp.log_to_stderr(logging.INFO)


def worker(sock, process_num):
    while True:
        try:
            client, address = sock.accept()
            log.info("{u} connected".format(u=address))
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

                    if direct_io:
                        fd = os.open(root+filename, os.O_CREAT | os.O_RDWR | os.O_DIRECT | os.O_SYNC)
                        m = mmap.mmap(-1, to_rcv)
                    else:
                        fd = os.open(root+filename, os.O_CREAT | os.O_RDWR)

                    os.lseek(fd, offset, os.SEEK_SET)
                    log.debug("Receiving file: {0}".format(filename))
                    chunk = client.recv(chunk_size.value)

                    while chunk:
                        log.debug("Chunk Size: {0}".format(len(chunk)))
                        if direct_io:
                            m.write(chunk)
                            os.write(fd, m)
                        else:
                            os.write(fd, chunk)

                        to_rcv -= len(chunk)
                        total += len(chunk)

                        if to_rcv > 0:
                            chunk = client.recv(min(chunk_size.value, to_rcv))
                        else:
                            log.debug("Successfully received file: {0}".format(filename))
                            break

                    os.close(fd)
                else:
                    chunk = client.recv(chunk_size.value)
                    while chunk:
                        chunk = client.recv(chunk_size.value)

                d = client.recv(1).decode()

            total = np.round(total/(1024*1024))
            log.info("{u} exited. total received {d} MB".format(u=address, d=total))
            client.close()
            process_status[process_num] = 0
        except Exception as e:
            log.error(str(e))
            # raise e


if __name__ == '__main__':
    direct_io = False
    file_transfer = True
    if "file_transfer" in configurations and configurations["file_transfer"] is not None:
        file_transfer = configurations["file_transfer"]

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
