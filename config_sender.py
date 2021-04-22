configurations = {
    "receiver": {
        "host": "10.3.33.2",
        "port": 50021
    },
    "data_dir": "/data/src_dir/",
    "method": "gradient", #[gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 5,
        "num_of_exp": -1
    },
    "random": {
        "num_of_exp": 10
    },
    "emulab_test": False,
    "file_transfer": True,
    "B": 10,
    "K": 1.02,
    "loglevel": "info",
    "probing_sec": 5,
    "multiplier": 1,
    "mp_opt": False,
    "fixed_probing": {
        "bsize": 7,
        "thread": 4
    },
    "max_cc": 100,
}