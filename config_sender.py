configurations = {
    "receiver": {
        "host": "10.10.2.2",
        "port": 50022
    },
    "data_dir": "/data/arif/",
    "method": "probe", #[gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1
    },
    "random": {
        "num_of_exp": 10
    },
    "file_transfer": True,
    "B": 10,
    "K": 1.02,
    "loglevel": "info",
    "probing_sec": 5,
    "multiplier": 1,
    "mp_opt": False,
    "fixed_probing": {
        "bsize": 10,
        "thread": 10
    },
    "max_cc": 32,
}
