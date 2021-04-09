configurations = {
    "receiver": {
        "host": "10.1.2.3",
        "port": 50021
    },
    "data_dir": "/proj/Cross-layer/arif/data/",
    "method": "gradient", #[bayes, random, brute, probe, lbfgs]
    "bayes": {
        "initial_run": 5,
        "num_of_exp": -1
    },
    "random": {
        "num_of_exp": 10
    },
    "emulab_test": True,
    "file_transfer": False,
    "B": 10,
    "K": 1.02,
    "loglevel": "info",
    "probing_sec": 5,
    "multiplier": 100,
    "mp_opt": False,
    "fixed_probing": {
        "bsize": 7,
        "thread": 4
    },
    "max_cc": 100,
}