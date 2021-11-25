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
            label='local_threads_harp',
            working_dir="files/",
            storage_access=[falconStage(
                address="127.0.0.1"
            )]
        )
    ],
)

parsl.load(config)


@python_app
def read_files(time_p, inputs=[]):
    # print (inputs)
    import time
    return "time for execution:" + str(time.time() - time_p)


@python_app
def file_transfer(file_name):
    # print (file_name)
    # harp_file = File('falcon://127.0.0.1/home/mbadhan/PycharmProjects/Falcon-File-Transfer-Optimizer2/inputs/' + file_name)
    harp_file = File('falcon://127.0.0.1/home/mbadhan/PycharmProjects/Falcon-File-Transfer-Optimizer2/inputs/')
    return harp_file


files = ["foo4.txt"]
# files=["foo1.txt","foo2.txt"]
# files=["foo1.txt","foo2.txt","foo3.txt"]
#files=["foo1.txt","foo2.txt","foo3.txt","foo4.txt"]

results = []
for i in range(len(files)):
    results.append(file_transfer(files[i]))

outputs = []
curr = time.time()
current_process = psutil.Process();
cur_cpu = psutil.cpu_percent()
for r in results:
    outputs.append(read_files(time.time(), r.result()))

print("Job Status: {}".format([o.done() for o in outputs]))
final_ = [o.result() for o in outputs]
print(final_)

# print total time
print(time.time() - curr)
# print("CPU Time:" +str(psutil.cpu_percent()-cur_cpu))

# print each job status,
print("\n Job Status: {}".format([r.done() for r in results]))

parsl.clear()
