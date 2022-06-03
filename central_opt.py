import os
import time
import signal
import numpy as np
import logging as log
from statistics import mean
from redis import Redis
from threading import Thread
from search import  base_optimizer, gradient_opt_fast


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
log.basicConfig(
    format=log_FORMAT,
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=log.DEBUG,
)

stop = 0
exit_signal = 10 ** 10
## Redis Config
hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
port = os.environ.get("REDIS_PORT", 6379)
send_key = "cc:{0}"
receive_key = "report:{0}"
register_key = f"transfer-registration"
r_conn = Redis(hostname, port, retry_on_timeout=True)
curr_cc = 1
cc_updated = 0
transfers = dict()


def event_send_cc(transfer_id):
    try:
        r_conn.xadd(send_key.format(transfer_id),{"cc": curr_cc})
    except Exception as e:
        log.exception(e)


def event_get_report(transfer_id):
    global transfers
    try:
        resp = r_conn.xread(streams={receive_key.format(transfer_id): 0}, count=None, block=1)
        if resp:
            key, messages = resp[0]
            r_conn.delete(key)
            _, data = messages[-1]
            transfers[transfer_id] = int(data["score".encode()].decode())
            log.info(transfers)

    except Exception as e:
        log.exception(e)


def register():
    global transfers
    try:
        resp = r_conn.xread(streams={register_key: 0}, count=1, block=1)
        if resp:
            key, messages = resp[0]
            r_conn.delete(key)
            _, data = messages[-1]
            transfer_id = data["transfer_id".encode()].decode()
            transfers[transfer_id] = 0 if len(transfers.values()) < 2 else mean(transfers.values())
            event_send_cc(transfer_id)

    except Exception as e:
        log.exception(e)


def redis_manager():
    global transfers, cc_updated
    while stop == 0:
        register()
        keys = list(transfers.keys())
        for key in keys:
            event_get_report(key)

            if transfers[key] == exit_signal:
                del transfers[key]

        if cc_updated == 1:
            for id in transfers:
                event_send_cc(id)

            cc_updated = 0


def sampling(params):
    global curr_cc, cc_updated
    curr_cc = int(np.ceil(params[0]/len(transfers)))
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

        time.sleep(2)
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
    t = Thread(target=redis_manager)
    t.start()

    run_optimizer("bayes")

