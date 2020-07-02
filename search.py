from skopt.space import Integer
from skopt import gp_minimize, forest_minimize, dummy_minimize


def bayes_opt(configurations, black_box_function, logger, verbose=True):
    search_space  = [
        Integer(1, configurations['cpu_count'], name='transfer_threads'),
        Integer(1, 20, name='bsize')
    ]
    
    experiments = forest_minimize(
        black_box_function,
        search_space,
        acq_func="EI",
        n_calls=configurations["bayes"]["num_of_exp"],
        n_random_starts=configurations["bayes"]["initial_run"],
        random_state=0,
        verbose=verbose,
        xi=0.01
    )
    
    logger.info("Best parameters: {0} and score: {1}".format(experiments.x, experiments.fun))
    black_box_function(experiments.x, sample_transfer=False)
    

def random_opt(black_box_function):
    black_box_function([3,12], sample_transfer=False)