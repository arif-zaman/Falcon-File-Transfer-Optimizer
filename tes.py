import parsl
from parsl.app.app import python_app, bash_app
from parsl.data_provider.files import File
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.data_provider.data_manager import default_staging
from parsl import set_stream_logger

set_stream_logger()
config = Config(
    executors=[
        HighThroughputExecutor(
            storage_access=default_staging,
            # equivalent to the following
            # storage_access=[NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPSeparateTaskStaging()],
        )
    ]
)
parsl.load(config)

# @python_app
# def print_file(inputs=[]):
#     with open(inputs[0].filepath, 'r') as inp:
#         content = inp.read()
#         return(content)

@bash_app
def print_file(inputs=[]):
    print(str([i.filepath for i in inputs]))
    str1 = " "
    file = [i.filepath for i in inputs]
    files= (str1.join(file))

    run="python3 main.py "+ files
    print(run)
    return run

# create an remote Parsl file
f = File('file:/home/mbadhan/PycharmProjects/stagingexp/read.txt')
f2 = File('file:/home/mbadhan/PycharmProjects/stagingexp/read2.txt')

# call the print_file app with the Parsl file
r = print_file(inputs=[f,f2])
print(r.result())

parsl.clear()