from skopt.space import Integer
from skopt import dummy_minimize as DM
from skopt import Optimizer as BO
import numpy as np
import time


def base_optimizer(configurations, black_box_function, logger, verbose=True):
    limit_obs, count = 20, 0
    max_thread = configurations["thread_limit"]
    iterations = configurations["bayes"]["num_of_exp"]  
    search_space  = [Integer(1, max_thread)]

    # if configurations["emulab_test"]:
    #     search_space.append(Integer(6, 7))
    # else:
    #     search_space.append(Integer(1, configurations["chunk_limit"]))
    
    params = []
    optimizer = BO(
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
                optimizer = BO(
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


def run_probe(current_cc, count, verbose, logger, black_box_function):
    if verbose:
        logger.info("Iteration {0} Starts ...".format(count))

    t1 = time.time()
    current_value = black_box_function([current_cc])
    t2 = time.time()

    if verbose:
        logger.info("Iteration {0} Ends, Took {1} Seconds. Score: {2}.".format(
            count, np.round(t2-t1, 2), current_value))
    
    return current_value


def gradient_ascent(configurations, black_box_function, logger, verbose=True):
    max_thread, count = configurations["thread_limit"], 0
    values = []
    ccs = [2]
    theta = 0

    while True:
        values.append(run_probe(ccs[-1]-1, count+1, verbose, logger, black_box_function))
        # values.append(run_probe(ccs[-1], count+2, verbose, logger, black_box_function))
        values.append(run_probe(ccs[-1]+1, count+2, verbose, logger, black_box_function))
        count += 2

        if values[-1] == 10 ** 10:
            logger.info("Optimizer Exits ...")
            break
        
        if len(ccs) == 1:
            ccs.append(2)
        
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

    return [ccs[-1]]


def gradient_ascent2(configurations, black_box_function, logger, verbose=True):
    max_thread, count = configurations["thread_limit"], 0
    values = []
    ccs = [1]
    theta = 0

    while True:
        count += 1
        values.append(run_probe(ccs[-1], count, verbose, logger, black_box_function))

        if values[-1] == 10 ** 10:
            logger.info("Optimizer Exits ...")
            break
        
        if len(ccs) == 1:
            ccs.append(2)
        
        else:
            dist = max(1, np.abs(ccs[-1] - ccs[-2]))
            if ccs[-1]>ccs[-2]:
                gradient = (values[-1] - values[-2])/dist
            else:
                gradient = (values[-2] - values[-1])/dist
            
            if values[-2] !=0:
                gradient_change = np.abs(gradient/values[-2])
            else:
                gradient_change = np.abs(gradient)
            
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
            next_cc = min(max(ccs[-1] + update_cc, 2), max_thread)
            logger.info("Gradient: {0}, Gredient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, ccs[-1], next_cc))
            ccs.append(next_cc)

    return [ccs[-1]]

    
def dummy(configurations, black_box_function, logger, verbose=True):    
    search_space  = [
        Integer(1, configurations["thread_limit"])
    ]
    
    optimizer = DM(
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


def brute_force(configurations, black_box_function, logger, verbose=True):
    score = []
    max_thread = configurations["thread_limit"]
    
    for i in range(1, max_thread+1):
        score.append(black_box_function([i]))
        
        if score[-1] == 10 ** 10:
            break
    
    
    cc = np.argmin(score) + 1
    logger.info("Best parameters: {0} and score: {1}".format([cc], score[cc-1]))
    return [cc]
