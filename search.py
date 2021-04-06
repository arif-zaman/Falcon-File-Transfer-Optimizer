from skopt.space import Integer
from skopt import Optimizer, dummy_minimize
from scipy.optimize import fmin_cg, fmin_l_bfgs_b
import numpy as np
import time


def base_optimizer(configurations, black_box_function, logger, verbose=False):
    limit_obs, count = 25, 0
    max_thread = configurations["thread_limit"]
    iterations = configurations["bayes"]["num_of_exp"]
    mp_opt = configurations["mp_opt"]
    
    if mp_opt:
        search_space  = [
            Integer(1, max_thread), # Concurrency
            Integer(1, 32), # Parallesism
            Integer(1, 32), # Pipeline
            Integer(1, 20), # Chunk/Block Size in KB: power of 2
        ]
    else:
        search_space  = [
            Integer(1, max_thread), # Concurrency
        ]

    params = []
    optimizer = Optimizer(
        dimensions=search_space,
        base_estimator="GP", #[GP, RF, ET, GBRT],
        acq_func="gp_hedge", # [LCB, EI, PI, gp_hedge]
        acq_optimizer="auto", #[sampling, lbfgs, auto]
        n_random_starts=configurations["bayes"]["initial_run"],
        model_queue_size= limit_obs,
        # acq_func_kwargs= {},
        # acq_optimizer_kwargs={}
    )
    
    while True:
        count += 1

        if len(optimizer.yi) > limit_obs:
            optimizer.yi = optimizer.yi[-limit_obs:]
            optimizer.Xi = optimizer.Xi[-limit_obs:]
        
        if verbose:
            logger.info("Iteration {0} Starts ...".format(count))

        t1 = time.time()
        res = optimizer.run(func=black_box_function, n_iter=1)
        t2 = time.time()

        if verbose:
            logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
                count, res.x, res.fun, np.round(t2-t1, 2)))

        last_value = optimizer.yi[-1]
        if last_value == 10 ** 10:
            logger.info("Optimizer Exits ...")
            break
        
        cc = optimizer.Xi[-1][0]
        if iterations < 1:
            reset = False
            if (last_value > 0) and (cc < max_thread):
                max_thread = max(cc, 2)
                reset = True

            if (last_value < 0) and (cc == max_thread) and (cc < configurations["thread_limit"]):
                max_thread = min(cc+5, configurations["thread_limit"])
                reset = True
            
            if reset:
                search_space[0] = Integer(1, max_thread)
                optimizer = Optimizer(
                    dimensions=search_space,
                    n_initial_points=configurations["bayes"]["initial_run"],
                    acq_optimizer="lbfgs",
                    model_queue_size= limit_obs
                )

        if iterations == count:
            logger.info("Best parameters: {0} and score: {1}".format(res.x, res.fun))
            params = res.x
            break
        
    return params


def hill_climb(configurations, black_box_function, logger, verbose=True):
    max_thread = configurations["thread_limit"]
    params = [1]
    phase, count = 1, 0
    current_value, previous_value = 0, 0
    
    while True:
        count += 1
        
        if verbose:
            logger.info("Iteration {0} Starts ...".format(count))

        t1 = time.time()
        current_value = black_box_function(params) * (-1)
        t2 = time.time()

        if verbose:
            logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
                count, params, current_value, np.round(t2-t1, 2)))

        if current_value == 10 ** 10:
            logger.info("Optimizer Exits ...")
            break
        
        if phase == 1:
            if (current_value > previous_value):
                params[0] = min(max_thread, params[0]+1)
                previous_value = current_value
            else:
                params[0] = max(1, params[0]-1)
                phase = 0

        elif phase == -1:
            if (current_value > previous_value):
                params[0] = min(max_thread, params[0]+1)
                phase = 0
            else:
                params[0] = max(1, params[0]-1)
                previous_value = current_value
            
        else:
            change = (current_value-previous_value)/previous_value
            previous_value = current_value
            if change > 0.1:
                phase = 1
                params[0] = min(max_thread, params[0]+1)
            elif change < -0.1:
                phase = -1
                params[0] = max(1, params[0]-1)
                
    return params


def cg_opt(configurations, black_box_function):
    mp_opt = configurations["mp_opt"]
    
    if mp_opt:
        starting_params = [1, 1, 1, 10]
    else:
        starting_params = [1]
        
    fmin_cg(
        f=black_box_function,
        x0=starting_params,
        epsilon=1, # step size
    )
    

def lbfgs_opt(configurations, black_box_function):
    max_thread = configurations["thread_limit"]
    mp_opt = configurations["mp_opt"]
    
    if mp_opt:
        starting_params = [1, 1, 1, 10]
        search_space  = [
            (1, max_thread), # Concurrency
            (1, 32), # Parallesism
            (1, 32), # Pipeline
            (1, 20), # Chunk/Block Size: power of 2
            ]
    else:
        starting_params = [1]
        search_space  = [
            (1, max_thread), # Concurrency
            ]
        
    fmin_l_bfgs_b(
        func=black_box_function,
        x0=starting_params,
        bounds=search_space,
        approx_grad=True,
        epsilon=1 # step size
    )

    
def dummy(configurations, black_box_function, logger, verbose=False):    
    search_space  = [
        Integer(1, configurations["thread_limit"])
    ]
    
    optimizer = dummy_minimize(
        func=black_box_function,
        dimensions=search_space,
        n_calls=configurations["random"]["num_of_exp"],
        random_state=None,
        x0=None,
        y0=None,
        verbose=verbose,
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(optimizer.x, optimizer.fun))
    return optimizer.x


def brute_force(configurations, black_box_function, logger, verbose=False):
    score = []
    max_thread = configurations["thread_limit"]
    
    for i in range(1, max_thread+1):
        score.append(black_box_function([i]))
        
        if score[-1] == 10 ** 10:
            break
    
    
    cc = np.argmin(score) + 1
    logger.info("Best parameters: {0} and score: {1}".format([cc], score[cc-1]))
    return [cc]
