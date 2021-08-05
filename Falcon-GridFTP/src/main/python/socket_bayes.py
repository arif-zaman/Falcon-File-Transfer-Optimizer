"""
# please install scikit-optimize
# provide HOST, PORT of the opt_server in main functions
# it will send back parameters value (format: cc,pp,pipeline) for probing,
    for example: "1,1,4" and will wait for throughput value
# send back throughput values in Mbps, for example: "10000.07"
# send "-1" to terminate the optimizer
"""
import warnings
warnings.filterwarnings('ignore')

import os
import socket
from skopt.space import Integer
from skopt import Optimizer as BO
import numpy as np
import time
import logging as logger
import sys,signal
from threading import Thread


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
logger.basicConfig(format=log_FORMAT, 
                    datefmt='%m/%d/%Y %I:%M:%S %p', 
                    level=logger.INFO)


recv_buffer_size = 8192
  

def harp_response(params):
    global opt_server
    params = [int(x) for x in params]
    
    n = params[0]
    # format >> "Concurrency"
    output = "{0}\n".format(params[0])
    
    if len(params) > 1:
        n = params[0] * params[1]
        # format >> "Concurrency,Parallesism,Pipeline"
        output = "{0},{1},{2}\n".format(params[0],params[1],params[2])
        
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    thrpt = 0
    opt_server.sendall(output.encode('utf-8'))
    
    while True:
        try:
            message  = opt_server.recv(recv_buffer_size).decode()
            thrpt = float(message) if message != "" else -1
            if thrpt is not None:
                break
            
        except Exception as e:
            logger.exception(e)
                
    if thrpt == -1:
        score = thrpt
    else:
        score = (thrpt/(1.02)**n) * (-1)
        logger.info("Sample Transfer -- Throughput: {0}Mbps, Score: {1}".format(
            np.round(thrpt), np.round(score)))
        
    return score


def base_optimizer(black_box_function, mp_opt=False):
    global max_cc
    limit_obs, count, init_points = 30, 0, 6 if mp_opt else 5
    
    if mp_opt:
        search_space  = [
            Integer(1, 32), # Concurrency
            Integer(1, 4), # Parallesism
            Integer(1, 10), # Pipeline
            ]
    else:
        search_space  = [
            Integer(1, max_cc), # Concurrency
            ]
        
    optimizer = BO(
        dimensions=search_space,
        base_estimator="GP", #[GP, RF, ET, GBRT],
        acq_func="gp_hedge", # [LCB, EI, PI, gp_hedge]
        acq_optimizer="auto", #[sampling, lbfgs, auto]
        n_initial_points=init_points,
        model_queue_size= limit_obs,
    )
        
    while True:
        count += 1

        if len(optimizer.yi) > limit_obs:
            optimizer.yi = optimizer.yi[-limit_obs:]
            optimizer.Xi = optimizer.Xi[-limit_obs:]
            
        logger.info("Iteration {0} Starts ...".format(count))

        t1 = time.time()
        res = optimizer.run(func=black_box_function, n_iter=1)
        t2 = time.time()

        logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
            count, res.x, np.round(res.fun), np.round(t2-t1, 2)))

        if optimizer.yi[-1] == -1:
            logger.info("Optimizer Exits ...")
            break


def signal_handling(signum,frame):
    global terminate
    terminate = True
    os._exit(1)

signal.signal(signal.SIGINT,signal_handling)


if __name__ == '__main__':
    max_cc, opt_server = 100, None
    HOST, PORT = "localhost", 32000
    serversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversock.bind((HOST, PORT))
    serversock.listen()

    while True:
        print ("Waiting")
        (opt_server, address) = serversock.accept()
        print ("Connected", address)
        # now do something with the clientsocket
        base_optimizer(harp_response)
        opt_server.close()
