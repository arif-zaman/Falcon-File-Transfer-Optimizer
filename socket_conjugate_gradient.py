"""
# please install scipy
# provide HOST, PORT of the server in main functions
1. send message: "start" to start the optmizer
2. it will send back parameters value (format: cc,pp,pipeline,blocksize) for probing, 
    for example: "1,1,4,6" and will wait for throughput value
3. send back throughput values in Mbps, for example: "10000.07"
4. send "-1" to terminate the optimizer
"""
import warnings
warnings.filterwarnings('ignore')

import socket
from scipy.optimize import fmin_cg
import numpy as np
import logging as logger


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
logger.basicConfig(format=log_FORMAT, 
                    datefmt='%m/%d/%Y %I:%M:%S %p', 
                    level=logger.INFO)


recv_buffer_size = 8192
  

def harp_response(params):
    global sock
    params = [int(np.round(x)) for x in params]
    params = [1 if x<1 else x for x in params]
    
    n = params[0]
    # format >> "Concurrency"
    output = "{0}".format(params[0])
    
    if len(params) > 1:
        n = params[0] * params[1]
        # format >> "Concurrency,Parallesism,Pipeline,Chunk/Block Size"
        output = "{0},{1},{2},{3}".format(params[0],params[1],params[2],params[3])
        
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    thrpt = 0
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
        exit(-1)
    else:
        score = (thrpt/(1.02)**n) * (-1)
        logger.info("Sample Transfer -- Throughput: {0}Mbps, Score: {1}".format(
            np.round(thrpt), np.round(score)))
        
    return score


def cg_opt(black_box_function, mp_opt=False):
    if mp_opt:
        starting_params = [1, 1, 1, 10]
    else:
        starting_params = [1]
        
    fmin_cg(
        f=black_box_function,
        x0=starting_params,
        epsilon=1, # step size
    )


if __name__ == '__main__':
    HOST, PORT = "localhost", 32000
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    
    if sock.recv(recv_buffer_size).decode() == "start":
        cg_opt(harp_response, mp_opt=False)
    
    sock.close()