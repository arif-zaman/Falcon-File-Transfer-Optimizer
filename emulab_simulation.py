import socket
import numpy as np
import multiprocessing as mp
import logging
import time

HOST, PORT = "localhost", 32000
logger = mp.log_to_stderr(logging.DEBUG)
recv_buffer_size = 8192


def worker(sock):
    while True:
        try:
            client, address = sock.accept()
            logger.debug("{u} connected".format(u=address))
            
            client.sendall("start".encode('utf-8'))
            cc = int(client.recv(recv_buffer_size).decode())
            count = 0
            while cc > 0:
                if count == 30:
                    thrpt = -1
                else:   
                    thrpt = min(100 * cc, np.random.randint(935,945))
                time.sleep(1)
                output = str(thrpt)
                client.sendall(output.encode('utf-8'))
                cc = int(client.recv(recv_buffer_size).decode())
                count += 1
                
            client.close()
        except Exception as e:
            logger.error(str(e))
            

if __name__ == '__main__':
    num_workers = 1
    sock = socket.socket()
    sock.bind((HOST, PORT))
    sock.listen(num_workers)

    workers = [mp.Process(target=worker, args=(sock,)) for i in range(num_workers)]
    for p in workers:
        p.daemon = True
        p.start()

    while True:
        try:
            time.sleep(1)
        except:
            break