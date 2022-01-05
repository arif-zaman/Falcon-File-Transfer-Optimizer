import time

import parsl
import psutil
from parsl import set_stream_logger
from parsl.app.app import python_app
from parsl.config import Config
from parsl.data_provider.files import File
from parsl.executors.threads import ThreadPoolExecutor

from Parsl.falcon import falconStage

# set the stream logger to print debug messages
set_stream_logger()

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_falcon',
            working_dir="/home/mbadhan/data/receive/",
            storage_access=[falconStage(
                address="127.0.0.1"
            )]
        )
    ],
)

parsl.load(config)


@python_app
def read_files(time_p, inputs=[]):
    import time
    print("------------------------------------------------------------------")
    print(inputs)
    f = open(inputs, "r")


    print(f.read())
    print("time for execution:" + str(time.time()))
    print("------------------------------------------------------------------")
    return "time for execution:" + str(time.time() - time_p)


@python_app
def file_process(file_name):
    falcon_file = File('falcon://127.0.0.1/home/mbadhan/' + file_name)
    return falcon_file

# files = ["data/send/"]
files = ["data/send/", "PycharmProjects/Falcon-File-Transfer-Optimizer2/inputs/foo4.txt"]
# files = [ "PycharmProjects/Falcon-File-Transfer-Optimizer2/inputs/foo4.txt"]
# files=["foo1.txt","foo2.txt"]
# files=["foo1.txt","foo2.txt","foo3.txt"]
# files=["foo1.txt","foo2.txt","foo3.txt","foo4.txt"]

results = []
for i in range(len(files)):

    # results.append(File('falcon://127.0.0.1/home/mbadhan/' + files[i]))
    # AttributeError: 'File'object has no attribute  'result'
    # results.append(file_process(File('falcon://127.0.0.1/home/mbadhan/' + files[i])))
    results.append(file_process(files[i]))

outputs = []
curr = time.time()
current_process = psutil.Process()
cur_cpu = psutil.cpu_percent()
for r in results:
    outputs.append(read_files(time.time(), r.result()))

print("Job Status: {}".format([o.done() for o in outputs]))
final_ = [o.result() for o in outputs]

print("heloooooooooooooooooooooooooooooooooooooooooooooooooooo",final_)

# print total time
print(time.time() - curr)
print("CPU Time:" +str(psutil.cpu_percent()-cur_cpu))

# print each job status,
print("\n Job Status: {}".format([r.done() for r in results]))

parsl.clear()
