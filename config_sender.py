configurations = {
    "receiver": {
        "host": "134.197.113.71",
        "port": 50021
    },
    "data_dir": "/home/arif/data/",
    "method": "probe", #[gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1
    },
    "random": {
        "num_of_exp": 10
    },
    "emulab_test": False,
    "file_transfer": False,
    "B": 10,
    "K": 1.02,
    "loglevel": "info",
    "probing_sec": 5,
    "multiplier": 1000,
    "mp_opt": False,
    "fixed_probing": {
        "bsize": 10,
        "thread": 3
    },
    "max_cc": 10,
}
