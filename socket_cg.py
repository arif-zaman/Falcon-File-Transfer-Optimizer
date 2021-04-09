"""
# please install scipy
# provide HOST, PORT of the opt_server in main functions
# it will send back parameters value (format: cc,pp,pipeline,blocksize) for probing,
    for example: "1,1,4,6" and will wait for throughput value
# send back throughput values in Mbps, for example: "10000.07"
# send "-1" to terminate the optimizer
"""
import warnings
warnings.filterwarnings('ignore')

import socket
from scipy.optimize import minimize
import numpy as np
import logging as logger
import time, os
import sys,signal
from threading import Thread


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
logger.basicConfig(
    format=log_FORMAT,
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logger.INFO
)

recv_buffer_size = 8192


def harp_response(params, opt_server):
    params = [int(np.round(x)) for x in params]
    params = [1 if x<1 else x for x in params]

    if len(params) == 1:
        n = params[0]
        # format >> "Concurrency"
        output = "{0}\n".format(params[0])

    elif len(params) == 3:
        n = params[0] * params[1]
        # format >> "Concurrency,Parallesism,Pipeline,Chunk/Block Size"
        output = "{0},{1},{2}\n".format(params[0],params[1],params[2])

    else:
        logger.info("Please check parameters list again ...")
        exit(1)

    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    opt_server.sendall(output.encode('utf-8'))

    while True:
        try:
            msg  = opt_server.recv(recv_buffer_size).decode()
            thrpt =  -1 if msg == "" else float(msg)
            if thrpt is not None:
                break

        except Exception as e:
            logger.exception(e)
            thrpt = -1

    if thrpt == -1:
        opt_server.close()
        logger.info("Optimizer Exits ...")
        exit(1)
    else:
        score = (thrpt/(1.02)**n) * (-1)
        logger.info("Sample Transfer -- Throughput: {0}Mbps, Score: {1}".format(
            np.round(thrpt), np.round(score)))

    return score


def cg_opt(opt_server, black_box_function, mp_opt=False):
    if mp_opt:
        starting_params = [1, 1, 1]
    else:
        starting_params = [1]

    optimizer = minimize(
        fun=black_box_function,
        x0=starting_params,
        args = (opt_server,),
        options={
            "eps":1, # step size
        }
    )

    while True:
        black_box_function(optimizer.x, opt_server)


def signal_handling(signum,frame):
    global terminate
    terminate = True
    os._exit(1)

signal.signal(signal.SIGINT,signal_handling)


if __name__ == '__main__':
    HOST, PORT = "localhost", 32000
    serversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversock.bind((HOST, PORT))
    serversock.listen()

    while True:
        print ("Waiting")
        (opt_server, address) = serversock.accept()
        print ("Connected", address)
        # now do something with the clientsocket
        # in this case, we'll pretend this is a threaded opt_server
        t = Thread(target=cg_opt, args=((opt_server, harp_response, True)))
        t.start()
        