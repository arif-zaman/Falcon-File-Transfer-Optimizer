from skopt.space import Integer
from skopt import Optimizer, dummy_minimize
from scipy.optimize import minimize
from collections import OrderedDict
import numpy as np
import time

exit_signal = 10 ** 10

def base_optimizer(configurations, black_box_function, logger, verbose=True):
    limit_obs, count = 20, 0
    max_thread = configurations["network_thread_limit"]
    iterations = configurations["bayes"]["num_of_exp"]
    mp_opt = configurations["mp_opt"]

    if mp_opt:
        search_space  = [
            Integer(1, max_thread), # Concurrency
            Integer(1, max_thread), # Parallesism
            # Integer(1, 10), # Pipeline
            # Integer(5, 20), # Chunk/Block Size in KB: power of 2
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
        if last_value == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        cc = optimizer.Xi[-1][0]
        if iterations < 1:
            reset = False
            if (last_value > 0) and (cc < max_thread):
                max_thread = max(cc, 2)
                reset = True

            if (last_value < 0) and (cc == max_thread) and (cc < configurations["network_thread_limit"]):
                max_thread = min(cc+5, configurations["network_thread_limit"])
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
    max_thread = configurations["network_thread_limit"]
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
            logger.info("Iteration {0} Ends, Took {3} Seconds. Params: {1} and Score: {2}.".format(
                count, params, current_value, np.round(t2-t1, 2)))

        if abs(current_value) == exit_signal:
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
        starting_params = [1, 1] #, 1, 10]
    else:
        starting_params = [1]

    optimizer = minimize(
        method="CG",
        fun=black_box_function,
        x0=starting_params,
        options= {
            "eps":1, # step size
        },
    )

    return optimizer.x


def dummy(configurations, black_box_function, logger, verbose=False):
    search_space  = [
        Integer(1, configurations["network_thread_limit"])
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

    return optimizer.x


def brute_force(configurations, black_box_function, logger, verbose=False):
    score = []
    max_thread = configurations["network_thread_limit"]

    for i in range(1, max_thread+1):
        score.append(black_box_function([i]))

        if score[-1] == exit_signal:
            break


    cc = np.argmin(score) + 1
    logger.info("Best parameters: {0} and score: {1}".format([cc], score[cc-1]))
    return [cc]


def run_probe(current_cc, count, verbose, logger, black_box_function):
    if verbose:
        logger.info("Iteration {0} Starts ...".format(count))

    t1 = time.time()
    current_value = black_box_function(current_cc)
    t2 = time.time()

    if verbose:
        logger.info("Iteration {0} Ends, Took {1} Seconds. Score: {2}.".format(
            count, np.round(t2-t1, 2), current_value))

    return current_value


def gradient_opt(configurations, black_box_function, logger, verbose=True):
    max_thread, count = configurations["network_thread_limit"], 0
    soft_limit, least_cost = max_thread, 0
    values = []
    ccs = [2]
    theta = 0

    while True:
        values.append(run_probe([ccs[-1]-1], count+1, verbose, logger, black_box_function))
        if values[-1] == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        if values[-1] < least_cost:
            least_cost = values[-1]
            soft_limit = min(ccs[-1]+10, max_thread)

        values.append(run_probe([ccs[-1]+1], count+2, verbose, logger, black_box_function))
        if values[-1] == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        if values[-1] < least_cost:
            least_cost = values[-1]
            soft_limit = min(ccs[-1]+10, max_thread)

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
        next_cc = min(max(ccs[-1] + update_cc, 2), soft_limit-1)
        logger.info("Gradient: {0}, Gredient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, ccs[-1], next_cc))
        ccs.append(next_cc)

    return [ccs[-1]]


def gradient_opt_fast(cc_limit, black_box_function, logger, verbose=True):
    max_thread, count = cc_limit, 0
    soft_limit, least_cost = max_thread, 0
    values = []
    ccs = [1]
    theta = 0

    while True:
        count += 1
        values.append(run_probe([ccs[-1]], count, verbose, logger, black_box_function))

        if values[-1] == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        if values[-1] < least_cost:
            least_cost = values[-1]
            soft_limit = min(ccs[-1]+10, max_thread)

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
            next_cc = min(max(ccs[-1] + update_cc, 1), soft_limit)
            # print("curr limit: ", least_cost, soft_limit)
            logger.info("Gradient: {0}, Gredient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, ccs[-1], next_cc))
            ccs.append(next_cc)

    return [ccs[-1]]


def gradient_multivariate(configurations, black_box_function, logger, verbose=True):
    count = 0
    cache_net = OrderedDict()
    cache_io = OrderedDict()
    soft_limit_net = configurations["network_thread_limit"]-1
    soft_limit_io = configurations["io_thread_limit"]-1
    io_opt = True
    values = []
    ccs = [[1,1]]

    while True:
        count += 1
        values.append(run_probe(ccs[-1], count, verbose, logger, black_box_function))
        cache_net[abs(values[-1][0])] = ccs[-1][0]
        cache_io[abs(values[-1][0])] = ccs[-1][1]


        if len(cache_net) >= 10:
            soft_limit_net = cache_net[max(cache_net.keys())]
            cache_net.popitem(last=True)

        if len(cache_io) >= 10:
            soft_limit_io = cache_io[max(cache_io.keys())]
            cache_io.popitem(last=True)


        if values[-1][0] == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        if values[-1][1] == exit_signal:
            logger.info("I/O Optimizer Exits ...")
            io_opt = False

        if len(ccs) == 1:
            ccs.append([2,2])

        else:
            # Network
            difference = ccs[-1][0] - ccs[-2][0]
            prev, curr = values[-2][0], values[-1][0]
            if difference != 0 and prev !=0:
                gradient = (curr - prev)/(difference*prev)
            else:
                gradient = (curr - prev)/prev if prev != 0 else 1

            update_cc_net = int(np.ceil(ccs[-1][0] * gradient))
            next_cc_net = min(max(ccs[-1][0] + update_cc_net, 1), soft_limit_net+1)

            if io_opt:
                difference = ccs[-1][1] - ccs[-2][1]
                prev, curr = values[-2][1], values[-1][1]
                if difference != 0 and prev !=0:
                    gradient = (curr - prev)/(difference*prev)
                else:
                    gradient = (curr - prev)/prev if prev !=0 else 1

                update_cc_io = int(np.ceil(ccs[-1][1] * gradient))
                next_cc_io = min(max(ccs[-1][1] + update_cc_io, 1), soft_limit_io+1)
            else:
                next_cc_io = 0

            ccs.append([next_cc_net, next_cc_io])
            logger.debug(f"Gradient: {gradient}")
            logger.info(f"Previous CC: {ccs[-2]}, Choosen CC: {ccs[-1]}")

    return ccs[-1]
