from skopt.space import Integer
from skopt import gp_minimize, dummy_minimize


def bayes_opt(configurations, black_box_function, logger, verbose=True):
    thread_limit = configurations['limits']["thread"]
    chunk_limit = configurations['limits']["bsize"]
    
    if thread_limit == -1:
        thread_limit = configurations["cpu_count"]
    
    if chunk_limit == -1:
        chunk_limit = 21
    
    search_space  = [
        Integer(1, thread_limit, name='transfer_threads'),
        Integer(1, chunk_limit, name='bsize')
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
    black_box_function(experiments.x, sample_transfer=False)
    

def random_opt(black_box_function):
    thread_limit = configurations['limits']["thread"]
    chunk_limit = configurations['limits']["bsize"]
    
    if thread_limit == -1:
        thread_limit = configurations["cpu_count"]
    
    if chunk_limit == -1:
        chunk_limit = 21
    
    search_space  = [
        Integer(1, thread_limit, name='transfer_threads'),
        Integer(1, chunk_limit, name='bsize')
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
    black_box_function(experiments.x, sample_transfer=False)
    
    
def probe_test_config(black_box_function, params):
    black_box_function(params, sample_transfer=False)