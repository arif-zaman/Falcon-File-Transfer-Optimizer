import os
import time
import signal
import numpy as np
import logging as log
from statistics import mean
from redis import Redis
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
from search import  base_optimizer, gradient_opt_fast


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
log.basicConfig(
    format=log_FORMAT,
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=log.DEBUG,
)

stop = 0
exit_signal = 10 ** 10
lock = Lock()
executor = ThreadPoolExecutor(max_workers=15)
## Redis Config
hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
port = os.environ.get("REDIS_PORT", 6379)
send_key = "cc:{0}"
receive_key = "report:{0}"
register_key = f"transfer-registration"
r_conn = Redis(hostname, port, retry_on_timeout=True)
default_cc, curr_cc = 1, 1
cc_updated = 0
r_block = 3
transfers = dict()


def event_send_cc(transfer_id, new=False):
    try:
        if new:
            r_conn.xadd(send_key.format(transfer_id),{"cc": default_cc})
        else:
            r_conn.xadd(send_key.format(transfer_id),{"cc": curr_cc})

    except Exception as e:
        log.exception(e)


def cc_update_manager():
    global cc_updated, transfers
    while stop == 0:
        if cc_updated == 1:
            for id in transfers:
                executor.submit(event_send_cc, id, False)

            time.sleep(1)
            # lock.acquire()
            cc_updated = 0
            # lock.release()


def event_get_report(transfer_id):
    global transfers
    try:
        resp = r_conn.xread(streams={receive_key.format(transfer_id): 0}, count=None, block=r_block)
        if resp:
            key, messages = resp[0]
            r_conn.delete(key)
            for message in messages:
                _, data = message
                lock.acquire()

                transfers[transfer_id] = int(data["score".encode()].decode())
                log.info(f'Transfer ID -- {transfer_id}: {transfers[transfer_id]}')
                if transfers[transfer_id] == exit_signal:
                    del transfers[transfer_id]

                lock.release()

    except Exception as e:
        log.exception(e)


def score_report_manager():
    global cc_updated, transfers
    while stop == 0:
        for id in transfers:
            executor.submit(event_get_report, id)

        time.sleep(.1)


def register_manager():
    global transfers

    while stop == 0:
        try:
            resp = r_conn.xread(streams={register_key: 0}, count=1, block=r_block)
            if resp:
                key, messages = resp[0]
                r_conn.delete(key)
                for message in messages:
                    _, data = message
                    transfer_id = data["transfer_id".encode()].decode()
                    # lock.acquire()
                    transfers[transfer_id] = 0 #if len(transfers.values()) < 2 else mean(transfers.values())
                    # lock.release()

                    executor.submit(event_send_cc, transfer_id, True)

        except Exception as e:
            log.exception(e)

        time.sleep(1)


def sampling(params):
    global curr_cc, cc_updated
    n = len(transfers)
    if n == 0:
        curr_cc = default_cc
        return exit_signal

    curr_cc = int(np.ceil(params[0]/n))
    cc_updated = 1

    time.sleep(3)
    return sum(transfers.values())


def run_optimizer(method):
    global curr_cc, cc_updated
    configurations = dict()
    configurations["thread_limit"] = 15
    configurations["mp_opt"] = False
    configurations["bayes"] = {
        "num_of_exp": -1,
        "initial_run": 3
    }

    while stop == 0:
        while len(transfers) == 0:
            time.sleep(0.01)

        time.sleep(1)
        if method == "bayes":
            base_optimizer(configurations, sampling, log)

        if method == "gradient":
            gradient_opt_fast(configurations, sampling, log)
        else:
            curr_cc = 5
            cc_updated = 1


def handler(signum, frame):
    global stop
    stop = 1
    exit()

signal.signal(signal.SIGINT, handler)


if __name__ == "__main__":
    register_thread = Thread(target=register_manager)
    register_thread.start()

    update_thread = Thread(target=cc_update_manager)
    update_thread.start()

    report_thread = Thread(target=score_report_manager)
    report_thread.start()

    run_optimizer("gradient")

