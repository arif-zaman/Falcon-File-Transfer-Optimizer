import os
import subprocess
import time
import numpy as np
import shutil


def tcp_stats(RCVR_ADDR, logger):
    start = time.time()
    sent, retm = 0, 0

    try:
        data = os.popen("ss -ti").read().split("\n")
        for i in range(1,len(data)):
            if RCVR_ADDR in data[i-1]:
                parse_data = data[i].split(" ")
                for entry in parse_data:
                    if "data_segs_out" in entry:
                        sent += int(entry.split(":")[-1])

                    if "bytes_retrans" in entry:
                        pass

                    elif "retrans" in entry:
                        retm += int(entry.split("/")[-1])

    except Exception as e:
        print(e)

    end = time.time()
    logger.debug("Time taken to collect tcp stats: {0}ms".format(np.round((end-start)*1000)))
    return sent, retm


def available_space(path="/dev/shm/data/"):
    try:
        space = shutil.disk_usage(path)
        return (np.round(space.used/10**9,3), np.round(space.free/10**9,3))
    except:
        return (0,0)


def run(cmd, logger):
    try:
        cmd_output = subprocess.run(cmd.split())

        logger.debug(f'[{cmd!r} exited with {cmd_output.returncode}]')
        if cmd_output.stdout:
            logger.debug(f'[stdout]\fcount{cmd_output.decode()}')
        if cmd_output.stderr:
            logger.debug(f'[stderr]\fcount{cmd_output.decode()}')
    except Exception as e:
        logger.debug(e)