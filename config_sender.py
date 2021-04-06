configurations = {
    "receiver": {
        "host": "127.0.0.1",
        "port": 50021
    },
    "data_dir": "/home/marifuzzaman/labwork/data/",
    "method": "brute", #[bayes, random, brute, probe, lbfgs]
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
    "multiplier": 100,
    "fixed_probing": {
        "bsize": 7,
        "thread": 4
    },
    "max_cc": -1,
}
