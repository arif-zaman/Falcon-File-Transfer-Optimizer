import os
import mmap
import socket
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

redis_host = os.environ.get("REDIS_HOSTNAME", "localhost")
redis_port = os.environ.get("REDIS_PORT", 6379)
redis_stream_key = "falcon-transfer:{0}".format(audience)

if configurations["loglevel"] == "debug":
    log = mp.log_to_stderr(logging.DEBUG)
else:
    log = mp.log_to_stderr(logging.INFO)

file_transfer = True
if "file_transfer" in configurations and configurations["file_transfer"] is not None:
    file_transfer = configurations["file_transfer"]

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
            raise e


def always_accept(value):
    if value or not value:
        return True


def process_token(pem, token):
    try:
        log.info(f'token: {token}')
        deserialized_token = scitokens.SciToken.deserialize(
            token,
            insecure=False,
            public_key = pem,
            audience = audience
        )

        enforcer = scitokens.Enforcer(issuer=issuer, audience=audience)
        enforcer.add_validator(audience, always_accept)
        enforcer.add_validator("user", always_accept)
        enforcer.add_validator("email", always_accept)
        enforcer.add_validator("source", always_accept)
        enforcer.add_validator("destination", always_accept)
        # enforcer.add_validator("io_dir", always_accept)
        scopes = enforcer.generate_acls(deserialized_token)
        log.info(scopes)
        return True, scopes

    except Exception as e:
        log.exception(e)
        return False, []



def get_configs(scopes):
    max_cc, max_bw, direct_io = -1, -1, False
    for entry in scopes:
        key, value = entry
        if key == "concurrency":
            max_cc = int(value.split("/")[-1])

        elif key == "bw_bps":
            max_bw = int(value.split("/")[-1])

        elif key == "direct_io":
            direct_io = True if value.split("/")[-1] == "1" else False

        else:
            continue

    log.info((max_cc, max_bw, direct_io))
    return max_cc, max_bw, direct_io


if __name__ == '__main__':
    # Redis Connection
    r_conn = Redis(redis_host, redis_port, retry_on_timeout=True)

    # Get Remote Server Public Key
    with open('publickey.pem', 'rb') as pem_in:
        public_pem = pem_in.read()

    while True:
        try:
            resp = r_conn.xread({redis_stream_key: 0}, count=1)
            if resp:
                key, messages = resp[0]
                _, data = messages[0]
                token = data[b"token"].decode("utf-8")
                r_conn.delete(key)

                valid, scopes = process_token(public_pem, token)
                if valid:
                    max_cc, max_bw, direct_io = get_configs(scopes)

                    num_workers = min(max_cc, configurations['max_cc'])

                    if num_workers == -1:
                        num_workers = mp.cpu_count()

                    file_transfer = not direct_io
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

        except ConnectionError as e:
            print("ERROR REDIS CONNECTION: {}".format(e))
