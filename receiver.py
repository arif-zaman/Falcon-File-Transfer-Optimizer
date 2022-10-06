import chunk
import os
import mmap
import time
import socket
import logging as logger
import numpy as np
import subprocess
import multiprocessing as mp
from config_receiver import configurations

chunk_size = mp.Value("i", 1024*1024)
root_dir = configurations["data_dir"]
tmpfs_dir = "/dev/shm/data/"
HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
transfer_complete = mp.Value("i", 0)
move_complete = mp.Value("i", 0)
transfer_done = mp.Value("i", 0)
mQueue = mp.Queue()
start, end = mp.Value("i", 0), mp.Value("i", 0)

direct_io = False
file_transfer = True
if "file_transfer" in configurations and configurations["file_transfer"] is not None:
    file_transfer = configurations["file_transfer"]

modular_test = False
if "modular_test" in configurations and configurations["modular_test"] is not None:
    modular_test = configurations["modular_test"]

log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.DEBUG,
    )

    mp.log_to_stderr(logger.DEBUG)
else:
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.INFO
    )

    mp.log_to_stderr(logger.INFO)


def run(cmd):
    cmd_output = subprocess.run(cmd.split())

    logger.debug(f'[{cmd!r} exited with {cmd_output.returncode}]')
    if cmd_output.stdout:
        logger.debug(f'[stdout]\fcount{cmd_output.decode()}')
    if cmd_output.stderr:
        logger.debug(f'[stderr]\fcount{cmd_output.decode()}')


def move_file(indx):
    logger.info(f'Starting File Mover Thread: {indx}')
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        try:
            fname = mQueue.get()
            fd = os.open(root_dir+fname, os.O_CREAT | os.O_RDWR)
            with open(tmpfs_dir+fname, "rb") as ff:
                chunk, offset = ff.read(1024*1024), 0
                if modular_test:
                    target, factor = 1024, 8
                    max_speed = (target * 1024 * 1024)/8
                    second_target, second_data_count = int(max_speed/factor), 0
                    timer100ms = time.time()

                while chunk:
                    os.lseek(fd, offset, os.SEEK_SET)
                    os.write(fd, chunk)
                    offset += len(chunk)
                    # logger.info((fname, offset))
                    if modular_test:
                        second_data_count += len(chunk)
                        if second_data_count >= second_target:
                            second_data_count = 0
                            while timer100ms + (1/factor) > time.time():
                                pass

                            timer100ms = time.time()

                    ff.seek(offset)
                    chunk = ff.read(1024*1024)

            move_complete.value = move_complete.value + 1
            logger.info(f'I/O :: {fname} moved to {root_dir}')
        except Exception as e:
            logger.exception(e)
            time.sleep(0.1)

    logger.info(f'Exiting File Mover Thread: {indx}')


def receive_file(sock, process_num):
    while True:
        try:
            client, address = sock.accept()
            logger.info("{u} connected".format(u=address))
            if start.value == 0:
                start.value = int(time.time())

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
                    logger.debug("Receiving file: {0}".format(filename))
                    chunk = client.recv(chunk_size.value)

                    while chunk:
                        # logger.debug("Chunk Size: {0}".format(len(chunk)))
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
                            logger.info("Successfully received file: {0}".format(filename))
                            transfer_complete.value = transfer_complete.value + 1
                            mQueue.put(filename)
                            break

                    os.close(fd)
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


if __name__ == '__main__':
    num_workers = configurations['max_cc']
    if num_workers == -1:
        num_workers = mp.cpu_count()

    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)

    iter = 0
    while iter<1:
        iter += 1
        logger.info(f">>>>>> Iterations: {iter} >>>>>>")


        process_status = mp.Array("i", [0 for _ in range(num_workers)])
        transfer_workers = [mp.Process(target=receive_file, args=(sock, i,)) for i in range(num_workers)]
        for p in transfer_workers:
            p.daemon = True
            p.start()

        io_workers = [mp.Process(target=move_file, args=(i,)) for i in range(12)]
        for p in io_workers:
            p.daemon = True
            p.start()


        # while True:
        #     try:
        #         time.sleep(1)
        #     except:
        #         break

        process_status[0] = 1
        alive = num_workers
        while alive>0:
            alive = sum(process_status)
            time.sleep(0.1)

        transfer_done.value = 1
        while move_complete.value < transfer_complete.value:
            time.sleep(0.1)

        # logger.info(transfer_complete.value)
        time_since_begining = int(time.time()) - start.value
        logger.info("{0} Files Received in {1} Seconds ".format(transfer_complete.value, time_since_begining))
        for p in transfer_workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)

        for p in io_workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)