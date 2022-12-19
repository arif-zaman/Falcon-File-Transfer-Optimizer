configurations = {
    "receiver": {
        "host": "10.1.2.3",
        "port": 50021
    },
    "data_dir": "/data/arif1/",
    "method": "gradient", # options: [gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "max_cc": 12,
    "K": 1.02,
    "probing_sec": 3.1, # probing interval in seconds
    "file_transfer": True,
    "io_limit": -1, # I/O limit (Mbps) per thread
    "loglevel": "info",
}