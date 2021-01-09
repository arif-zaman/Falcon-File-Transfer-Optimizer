"""
# please install scikit-optimize
# provide HOST, PORT of the server in main functions
1. send message: "start" to start the optmizer
2. it will send back cc value for probing, for example: "1" and will wait for throughput value
3. send back throughput values in Mbps, for example: "10000.07"
4. send "-1" to terminate the optimizer
"""
import warnings

from numpy.lib.function_base import gradient
warnings.filterwarnings('ignore')

import socket
from skopt.space import Integer
from skopt import Optimizer as BO
import numpy as np
import time
import logging as logger


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
logger.basicConfig(format=log_FORMAT, 
                    datefmt='%m/%d/%Y %I:%M:%S %p', 
                    level=logger.INFO)


recv_buffer_size = 8192
  

def harp_response(params, count):
    global sock, max_cc
    cc = params[0]
    logger.info("Iteration {0} Starts ...".format(count))
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    thrpt = 0
    
    output = str(cc)
    sock.sendall(output.encode('utf-8'))
    
    while True:
        try:
            message  = sock.recv(recv_buffer_size).decode()
            thrpt = float(message)
            
            if thrpt is not None:
                break
            
        except Exception as e:
            logger.exception(e)
                
    if thrpt == -1:
        score = thrpt
    else:
        # cc_factor = (cc-1)/max_cc
        # score = np.round(thrpt * (1 - cc_factor) * (-1))
        score = (thrpt/(1.02)**cc) * (-1)
    
        logger.info("Sample Transfer -- Throughput: {0}Mbps, Score: {1}".format(
            np.round(thrpt), score))
        
    return score


def gradient(black_box_function):
    max_thread, count = max_cc, 0
    values = []
    ccs = [2]
    theta = 0

    while True:
        values.append(black_box_function([ccs[-1]-1], count+1))
        if values[-1] == -1:
            logger.info("Optimizer Exits ...")
            break
        
        # values.append(run_probe(ccs[-1], count+2, verbose, logger, black_box_function))
        values.append(black_box_function([ccs[-1]+1], count+2))
        if values[-1] == -1:
            logger.info("Optimizer Exits ...")
            break
        count += 2

        
        gradient = (values[-1] - values[-2])/2
        gradient_change = np.abs(gradient/values[-2])
        
        if gradient>0:
            if theta <= 0:
                theta -= 1
            else:
                theta = -1
                
        else:
            if theta >= 0:
                theta += 1
            else:
                theta = 1
        
        update_cc = int(theta * np.ceil(ccs[-1] * gradient_change))
        next_cc = min(max(ccs[-1] + update_cc, 2), max_thread-1)
        logger.info("Gradient: {0}, Gredient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, ccs[-1], next_cc))
        ccs.append(next_cc)


if __name__ == '__main__':
    max_cc = 100
    HOST, PORT = "localhost", 32000
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    
    if sock.recv(recv_buffer_size).decode() == "start":
        gradient(harp_response)
    
    sock.close()