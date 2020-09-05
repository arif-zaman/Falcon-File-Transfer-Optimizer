from skopt.space import Integer
from skopt import gp_minimize, dummy_minimize, gbrt_minimize, Optimizer
import numpy as np
import time


def base_optimizer(configurations, black_box_function, logger, verbose=True):
    limit_obs = 50  
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
        Integer(1, configurations["chunk_limit"])
    ]
    
    if configurations["emulab_test"]:
        search_space  = [
            Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
            Integer(6, 7)
        ]
        
    experiments = Optimizer(
        dimensions=search_space,
        n_initial_points=configurations["thread"]["random_probe"],
        # acq_func="LCB",
        # acq_func_kwargs={"kappa": 1.96},
        acq_optimizer="lbfgs",
        model_queue_size= limit_obs
    )

    count = configurations["thread"]["random_probe"]
    if verbose:
        logger.info("Running Initial {0} Random Evaluations ...".format(count))

    experiments.run(func=black_box_function, n_iter=count)
    
    while (experiments.yi[-1] != 10 ** 10):
        count += 1
        experiments.yi = experiments.yi[-limit_obs:]
        experiments.Xi = experiments.Xi[-limit_obs:]
        
        if verbose:
            logger.info("Iteration {0} Starts ...".format(count))

        t1 = time.time()
        res = experiments.run(func=black_box_function, n_iter=1)
        t2 = time.time()

        # for i in range(len(experiments.Xi)-1):
        #     if experiments.Xi[i] == experiments.Xi[-1]:
        #         experiments.yi[i] = experiments.yi[-1]

        if verbose:
            # indx = np.argmin(experiments.yi)
            logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
                count, res.x, res.fun, np.round(t2-t1, 2)))



def gp(configurations, black_box_function, logger, verbose=True):  
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
        Integer(1, configurations["chunk_limit"])
    ]
    
    if configurations["emulab_test"]:
        search_space  = [
            Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
            Integer(6, 7)
        ]
        
    experiments = gp_minimize(
        func=black_box_function,
        dimensions=search_space,
        acq_func="LCB", # [LCB, EI, PI, gp_hedge]
        # acq_optimizer="sampling", # [sampling, lbfgs]
        # n_points=20,
        n_calls=configurations["thread"]["iteration"],
        n_random_starts=configurations["thread"]["random_probe"],
        random_state=None,
        x0=None,
        y0=None,
        verbose=verbose,
        # callback=None,
        xi=0.01, # EI or PI
        kappa=5, # LCB only
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    return experiments.x


def gbrt(configurations, black_box_function, logger, verbose=True):  
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
        Integer(1, configurations["chunk_limit"])
    ]
    
    if configurations["emulab_test"]:
        search_space  = [
            Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
            Integer(6, 7)
        ]
        
    experiments = gbrt_minimize(
        func=black_box_function,
        dimensions=search_space,
        # acq_func="LCB", # [LCB, EI, PI]
        n_calls=configurations["thread"]["iteration"],
        n_random_starts=configurations["thread"]["random_probe"],
        random_state=None,
        x0=None,
        y0=None,
        verbose=verbose,
        # callback=None,
        # xi=0.01, # EI or PI
        kappa=10, # LCB only
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    return experiments.x
    

def dummy(configurations, black_box_function, logger, verbose=True):    
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
        Integer(1, configurations["chunk_limit"])
    ]
    
    experiments = dummy_minimize(
        func=black_box_function,
        dimensions=search_space,
        n_calls=configurations["bayes"]["num_of_exp"],
        random_state=None,
        x0=None,
        y0=None,
        verbose=verbose,
        # callback=None,
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    return experiments.x


def repetitive_probe(configurations, black_box_function, logger, verbose=True):  
    if configurations["thread"]["min"] >= configurations["thread"]["max"]: 
        return [configurations["thread"]["min"]]
          
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
        Integer(1, configurations["chunk_limit"])
    ]
    
    if configurations["emulab_test"]:
        search_space  = [
            Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
            Integer(6, 7)
        ]
    
    experiments = gp_minimize(
        func=black_box_function,
        dimensions=search_space,
        # acq_func="EI", # [LCB, EI, PI]
        n_calls=configurations["thread"]["iteration"],
        n_random_starts=configurations["thread"]["random_probe"],
        random_state=None,
        x0=None,
        y0=None,
        verbose=verbose,
        # callback=None,
        # xi=0.01, # EI or PI
        # kappa=1.96, # LCB only
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    return experiments.x


def brute_force(configurations, black_box_function, logger, verbose=True):
    score = []
    max_thread = configurations["thread"]["max"]
    max_chunk_size = configurations["chunk_limit"]
    
    if not configurations["emulab_test"]:
        for i in range(max_thread):
            for j in range(max_chunk_size):
                params = [i+1, j+1]
                score.append(black_box_function(params))
    
    else:
        ccs = [i+1 for i in range(max_thread)]
        np.random.shuffle(ccs)
        max_chunk_size = 1
        j = 6
        for i in range(max_thread):
            params = [ccs[i], j+1]
            score.append(black_box_function(params))
    
    min_score_indx= int(np.argmin(score)/max_chunk_size)
    params = [ccs[min_score_indx], j+1]
    logger.info("Best parameters: {0} and score: {1}".format(params, score[min_score_indx]))
    return params


def random_brute_search(configurations, black_box_function, logger, verbose=True):
    time.sleep(0.1)
    max_thread = configurations["thread"]["max"]
    cc = max_thread
    score = [1000000 for i in range(max_thread)]

    while cc > 0:
        params = [cc, 7]
        score[cc-1] = black_box_function(params)
        cc = int(cc/2)
    
    
    min_scores= np.argsort(score)[:2]
    min_cc, max_cc = np.min(min_scores)+1, np.max(min_scores)+1
    search_range = max_cc - min_cc - 2

    if search_range<=5:
        cc = min_cc+1
        while cc<max_cc:
            params = [cc, 7]
            score[cc-1] = black_box_function(params)
            cc += 1
    else:
        cc_set = np.random.choice(range(min_cc+1,max_cc), 5, replace=False)
        for cc in cc_set:
            params = [cc, 7]
            score[cc-1] = black_box_function(params)

    
    min_score_indx = np.argmin(score)
    params = [min_score_indx+1, 7]
    logger.info("Best parameters: {0} and score: {1}".format(params, score[min_score_indx]))
    return params
