from skopt.space import Integer
from skopt import gp_minimize, dummy_minimize
import numpy as np
# from bayes_opt import BayesianOptimization


def initial_probe(configurations, black_box_function, logger, verbose=True):  
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
        Integer(1, configurations["chunk_limit"])
    ]
    
    if configurations["emulab_test"]:
        search_space  = [
            Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
            Integer(9, 10)
        ]
        
    experiments = gp_minimize(
        func=black_box_function,
        dimensions=search_space,
        # acq_func="gp_hedge", # [LCB, EI, PI]
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


def repetitive_probe(configurations, black_box_function, logger, verbose=True):  
    if configurations["thread"]["min"] >= configurations["thread"]["max"]: 
        return [configurations["thread"]["min"]]
          
    search_space  = [
        Integer(configurations["thread"]["min"], configurations["thread"]["max"]),
    ]
    
    experiments = gp_minimize(
        func=black_box_function,
        dimensions=search_space,
        # acq_func="gp_hedge", # [LCB, EI, PI]
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
        for i in range(max_thread):
            params = [i+1, 10]
            score.append(black_box_function(params))
    
    max_score_indx= np.argmin(score)
    params = [max_score_indx+1, 10]
    logger.info("Best parameters: {0} and score: {1}".format(params, np.max(score)))
    return params
    
    
def probe_test_config(black_box_function, params):
    black_box_function(params)