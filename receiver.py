import os
import mmap
import time
import socket
import logging as log
import numpy as np
import psutil
import multiprocessing as mp
from config_receiver import configurations

chunk_size = mp.Value("i", 1024*1024)
root = configurations["data_dir"]
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
cpus = mp.Manager().list()

log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    log.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log.DEBUG,
    )

    mp.log_to_stderr(log.DEBUG)
else:
    log.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log.INFO
    )

    mp.log_to_stderr(log.INFO)


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
                        # log.debug("Chunk Size: {0}".format(len(chunk)))
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


def report_throughput():
    global cpus
    time.sleep(1)

    while sum(process_status) > 0:
        t1 = time.time()
        cpus.append(psutil.cpu_percent())
        log.info(f"cpu: curr - {np.round(cpus[-1], 4)}, avg - {np.round(np.mean(cpus), 4)}")
        t2 = time.time()
        time.sleep(max(0, 1 - (t2-t1)))


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

    iter = 0
    while iter<1:
        iter += 1
        log.info(">>>>>> Iterations: {0} >>>>>>".format(iter))

        process_status = mp.Array("i", [0 for _ in range(num_workers)])
        workers = [mp.Process(target=worker, args=(sock, i,)) for i in range(num_workers)]
        for p in workers:
            p.daemon = True
            p.start()

        # while True:
        #     try:
        #         time.sleep(1)
        #     except:
        #         break

        process_status[0] = 1
        # alive = num_workers
        # reporting_process = mp.Process(target=report_throughput)
        # reporting_process.daemon = True
        # reporting_process.start()

        while sum(process_status) > 0:
            # alive = sum(process_status)
            # for i in range(num_workers):
            #     if process_status[i] == 1:
            #         alive += 1

            time.sleep(0.1)

        # reporting_process.terminate()
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)
