from asyncio.log import logger
import os
import mmap
import time
import socket
import logging
import numpy as np
import multiprocessing as mp
from config_receiver import configurations

manager = mp.Manager()
chunk_size = mp.Value("i", 1024*1024)
dest_dir = configurations["data_dir"]
tmpfs_dir = "/dev/shm/data/"
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]

if configurations["loglevel"] == "debug":
    log = mp.log_to_stderr(logging.DEBUG)
else:
    log = mp.log_to_stderr(logging.INFO)


def run_command(cmd):
    log.debug(f"Running Command >> {cmd}")
    try:
        os.popen(cmd).read()
        return True, None

    except Exception as e:
        return False, e

def move_file(process_id):
    log.debug(f"Start Data Mover Worker :: {process_id}")
    while (file_count.value >0):
        try:
            if not move_queue.empty():
                filename = move_queue.get()
                # with open(tmpfs_dir+filename, "rb") as f:
                #     fd = os.open(dest_dir+filename, os.O_CREAT | os.O_RDWR)
                #     os.write(fd, f.read())
                #     os.close(fd)
                #     file_count.value = file_count.value - 1
                cmd = f'mv {tmpfs_dir+filename} {dest_dir}'
                status, error = run_command(cmd)

                if status:
                    file_count.value = file_count.value - 1
                    log.debug(f"Moved to SSD: {filename}")
                else:
                    # move_queue.put(filename)
                    raise error
            else:
                time.sleep(0.1)

        except Exception as e:
            log.error(f"Data Mover Worker: {process_id}, Error: {str(e)}")

    log.debug(f"End Data Mover Worker :: {process_id}")


def receive_file(sock, process_num):
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
                        fd = os.open(tmpfs_dir+filename, os.O_CREAT | os.O_RDWR | os.O_DIRECT | os.O_SYNC)
                        m = mmap.mmap(-1, to_rcv)
                    else:
                        fd = os.open(tmpfs_dir+filename, os.O_CREAT | os.O_RDWR)

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
                            # t1 = time.time()
                            # move_queue.put_nowait(filename)
                            # log.info(f'Time (Sec): {np.round(time.time() - t1, 3)}')
                            # file_count.value = file_count.value - 1
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
    iteration = 0
    while iteration < 1:
        iteration += 1
        move_cc = 8
        file_count = mp.Value("i", 100)
        move_queue = manager.Queue(maxsize=file_count.value)
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

        receiver_workers = [mp.Process(target=receive_file, args=(sock, i,)) for i in range(num_workers)]
        for p in receiver_workers:
            p.daemon = True
            p.start()

        process_status[0] = 1
        # alive = num_workers
        # while alive>0:
        #     alive = sum(process_status)
        #     time.sleep(0.1)

        move_workers = [mp.Process(target=move_file, args=(i,)) for i in range(move_cc)]
        for p in move_workers:
            p.daemon = True
            p.start()

        # t1 = time.time()
        # for i in range(1,file_count.value+1):
        #     move_queue.put(f'1GB{i}')

        while (file_count.value > 0):
            time.sleep(0.1)

        # log.info(f"time - sec: {np.round(time.time() - t1, 3)}")

        for workers in [receiver_workers, move_workers]:
            for p in workers:
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=0.1)
