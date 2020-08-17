from skopt.space import Integer
from skopt import gp_minimize, dummy_minimize
# from bayes_opt import BayesianOptimization


def bayes_sci_opt(configurations, black_box_function, logger, verbose=True):    
    search_space  = [
        Integer(1, configurations["thread_limit"], name='transfer_threads'),
        Integer(1, configurations["chunk_limit"], name='bsize')
    ]
    
    experiments = gp_minimize(
        func=black_box_function,
        dimensions=search_space,
        # acq_func="gp_hedge", # [LCB, EI, PI]
        # acq_optimizer="lbfgs", # [sampling, lbfgs]
        n_calls=configurations["bayes"]["num_of_exp"],
        n_random_starts=configurations["bayes"]["initial_run"],
        random_state=None,
        x0=None,
        y0=None,
        verbose=verbose,
        # callback=None,
        # xi=0.01, # EI or PI
        # kappa=1.96, # LCB only
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    indx = experiments.func_vals.argsort()[1]
    logger.info("2nd Best parameters: {0} and score: {1}".format(experiments.x_iters[indx], 
                                                                 experiments.func_vals[indx]))
    # return experiments.x
    return experiments.x_iters[indx]
    

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
    
    
def probe_test_config(black_box_function, params):
    black_box_function(params, sample_transfer=False)