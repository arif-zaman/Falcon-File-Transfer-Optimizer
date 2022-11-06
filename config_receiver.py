configurations = {
    "receiver": {
        "host": "10.10.1.2",
        "port": 50021
    },
    "data_dir": "/data/arif/",
    "method": "hill_climb", # options: [gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "max_cc": 12,
    "K": 1.04,
    "probing_sec": 3, # probing interval in seconds
    "file_transfer": True,
    "mp_opt": False,
    "modular_test": True, # 1Gbps I/O limit per thread
    "loglevel": "info",
}