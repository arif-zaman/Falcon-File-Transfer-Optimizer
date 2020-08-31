from skopt.space import Integer
from skopt import gp_minimize, dummy_minimize, gbrt_minimize
import numpy as np
import time
# from bayes_opt import BayesianOptimization


def bayes_gp(configurations, black_box_function, logger, verbose=True):  
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
        # acq_optimizer="lbfgs", # [sampling, lbfgs]
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


def bayes_gbrt(configurations, black_box_function, logger, verbose=True):  
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
        kappa=2.58, # LCB only
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    return experiments.x


def repetitive_probe(configurations, black_box_function, logger, verbose=True):  
    if configurations["thread"]["min"] >= configurations["thread"]["max"]: 
        return [configurations["thread"]["min"]]
          
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
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
    

def random_opt(configurations, black_box_function, logger, verbose=True):    
    search_space  = [
        Integer(1, configurations["thread_limit"], name='transfer_threads'),
        Integer(1, configurations["chunk_limit"], name='bsize')
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
        max_chunk_size = 1
        j = 6
        for i in range(max_thread):
            params = [i+1, j+1]
            score.append(black_box_function(params))
    
    min_score_indx= int(np.argmin(score)/max_chunk_size)
    params = [min_score_indx+1, j+1]
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


def probe_test_config(black_box_function, params):
    black_box_function(params)