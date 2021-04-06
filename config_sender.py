configurations = {
    "receiver": {
        "host": "134.197.113.71",
        "port": 50021
    },
    "data_dir": "/data/arif/",
    "method": "gradient", #[bayes, random, brute, probe, lbfgs]
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
    "probing_sec": 3,
    "multiplier": 1,
    "mp_opt": False,
    "fixed_probing": {
        "bsize": 7,
        "thread": 4
    },
    "max_cc": 100,
}
