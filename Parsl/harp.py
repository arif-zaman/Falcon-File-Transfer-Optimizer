import logging
import json
import os, sys
import parsl
import typeguard
import subprocess
from subprocess import call
from functools import partial
from typing import Optional
from collections import namedtuple
from parsl.app.app import python_app
from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging
import zmq
import uuid
from zmq.devices.basedevice import ProcessDevice
from multiprocessing import Process
import time
from collections import defaultdict
logger = logging.getLogger(__name__)
harp_config=[]

# harp staging must be run explicitly in the same process/interpreter
# as the DFK. it relies on persisting harp state between actions in that
# process.

def _get_harp_provider(dfk, executor_label):
    if executor_label is None:
        raise ValueError("executor_label is mandatory")
    executor = dfk.executors[executor_label]
    logger.debug('harp init'+str(executor))
    if not hasattr(executor, "storage_access"):
        raise ValueError("specified executor does not have storage_access attribute")
    for provider in executor.storage_access:
        if isinstance(provider, HARPStaging):
            return provider

    raise Exception('No suitable harp adress defined for executor {}'.format(executor_label))

def init_queue(self):
        harp_config.clear()
        self.queuedevice = ProcessDevice(zmq.QUEUE, zmq.PULL, zmq.PUSH)
        self.queuedevice.bind_in("tcp://127.0.0.1:%d" % self.frontend_port)
        self.queuedevice.bind_out("tcp://127.0.0.1:%d" % self.backend_port)
        self.queuedevice.setsockopt_in(zmq.IDENTITY, b'PULL')
        self.queuedevice.setsockopt_out(zmq.IDENTITY, b'PUSH')
        self. queuedevice.start()
        self.sts_queuedevice = ProcessDevice(zmq.QUEUE, zmq.XREP, zmq.XREQ)
        self.sts_queuedevice.bind_in("tcp://127.0.0.1:%d" % self.sts_frontend_port)
        self.sts_queuedevice.bind_out("tcp://127.0.0.1:%d" % self.sts_backend_port)
        self.sts_queuedevice.setsockopt_in(zmq.SNDHWM, 1)
        self.sts_queuedevice.setsockopt_out(zmq.RCVHWM, 1)
        self.sts_queuedevice.start()
        time.sleep (2)  

def get_harp(self):
    logger.debug('harp init')
    #init_queue(self)
    return harp()

def timed_out(waited_time,timeout):
    return waited_time > timeout

def get_status(task_id):
    context = zmq.Context()
    sts_frontend_port = 6950
    sts_socket = context.socket(zmq.REQ)
    #task = namedtuple('TaskID', 'Status, Estimation')
    #logger.debug("Connecting with Harp")
    sts_socket.connect("tcp://127.0.0.1:%s" % sts_frontend_port)
    #Request status of task
    sts_socket.send_string("Request For Job Status from Parsl : " +str(task_id))
    #Get Reply from HARP
    message = sts_socket.recv().decode("utf-8")
    #logger.debug(": Reply from HARP "+str(message))
    task = {}
    staskid=""
    if message!="":
        msg= json.loads(message)
        #estimation = msg["list"][str(task_id)]
        msg_list =  msg["list"]

        for x in msg_list:
            if x==task_id:
                staskid=x
        if staskid!="":
            task["estimation"] = msg_list[task_id]
            if task["estimation"]==0:
                task["status"] = "Finished"
            else:
                task["status"] = "ACTIVE"

        sts_socket.close()

    return task

def harp_task_wait(task_id, timeout=60, polling_interval=15):
    polling_interval = min(timeout, polling_interval)
    waited_time = 0
    while True:
            # get task, check if status != ACTIVE
            task = get_status(task_id)
            count=0
            #task_count=len(task_all)
            #task_id_list=""
            waited_time +=1
            if len(task)==0:
                status="Just Started"
                if waited_time%5==0:
                    logger.debug("harp_task_wait(task_id={}) terminated after {}s with status={}".format(task_id, waited_time, status))
            else:
                status = task['status']

            if status == "Finished":
                logger.debug("harp_task_wait(task_id={}) terminated after {}s with status={}".format(task_id, waited_time, status))
                return True
                
            # make sure to check if we timed out before sleeping again, so we
            # don't sleep an extra polling_interval
            # waited_time += polling_interval
            # if timed_out(waited_time,timeout):
            #     logger.debug("harp_task_wait(task_id={}) timed out".format(task_id))
            #     return False
           
            if waited_time%5==0:
                if status!="Just Started":
                    logger.debug("harp_task_wait(task_id={}) waiting {}s, estimated completion time : {}".format(task_id, waited_time, task["estimation"]))
        
            time.sleep(1)

def submit_transfer(msg_config):
    #print ("Connecting a server to queue device")
    context = zmq.Context()
    frontend_port = 4950
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:%s" % frontend_port)
    socket.send_string('%s' % msg_config)
    logger.debug("Task sent to HARP "+str(msg_config))
    socket.close()

class harp(object):
    """
    All communication with the harp Auth and harp Transfer services is enclosed
    in the harp class. In particular, the harp class is reponsible for:
     - managing an OAuth2 authorizer - getting access and refresh tokens,
       refreshing an access token, storing to and retrieving tokens from
       .harp.json file,
     - submitting file transfers,
     - monitoring transfers.
    """

    #stage in-----file.netloc, harp_address['address'],file.path, dst_path)
            #src_ep:    file.netloc -> gridftp.stampede2.tacc.xsede.org:2811
            #dst_ep:    harp_address['address'] -> oasis-dm.sdsc.xsede.org:2811/oasis/scratch-comet/saktar/temp_project/test/test1/
            #src_path:  file.path -> /scratch/06898/tg861972/test/test1/foo1.txt
            #dst_path:  dst_path -> foo1.txt

    #stage out-----harp_address['address'], file.netloc, src_path, file.path
            #src_ep:    harp_address['address'] -> oasis-dm.sdsc.xsede.org:2811/oasis/scratch-comet/saktar/temp_project/test/test1/
            #dst_ep:    file.netloc -> gridftp.stampede2.tacc.xsede.org:2811
            #src_path:  src_path -> foo1.txt
            #dst_path:  file.path -> /scratch/06898/tg861972/test/test1/foo1.txt

    @classmethod
    def transfer_file(cls, src_ep, dst_ep, src_path, dst_path, harp_directory):
        #logger.debug('harp transfer_file')
        filename ="-filename "+str(os.path.basename(src_path))
        fl=os.path.basename(src_path)
        src=str(src_ep+src_path)
        dst=str(dst_ep+dst_path).replace(str(fl),"")
        task_id=str(uuid.uuid1())
        task_="-taskid "+str(task_id)
        s="-s gsiftp://"+str(src)  
        d="-d gsiftp://"+str(dst)
        rtt="-rtt 0.04"
        bandwidth="-bandwidth 10"
        buffer_size="-buffer-size 32"
        maxcc="-maxcc 4"
        msg_config=str(s)+" "+str(d)+" "+str(rtt)+" "+str(bandwidth)+" "+str(buffer_size)+" "+str(maxcc)+" "+str(filename)+" "+str(task_)
        harp_config.append(task_id)
        try:
            #submit_transfer(msg_config)
            s=Process(target=submit_transfer, args=(msg_config,))
            s.start()
            time.sleep(1)
            s.terminate()
            #logger.debug("Task Submitted "+ str(len(harp_config)))
        except Exception as e:
            raise Exception('harp transfer from {}{} to {}{} failed due to error: {}'.format(src_ep, src_path, dst_ep, dst_path,e))
        
        
        while not harp_task_wait(task_id, 60, 10):
           logger.debug("")
            
        task = get_status(task_id)
        if task['status'] == 'Finished':
            logger.debug('HARP transfer {}, from {}{} to {}{} succeeded'.format(
                task_id, src_ep, src_path, dst_ep, dst_path))
        else:
            logger.debug('HARP transfer {}, from {}{} to {}{} failed due to error'.format(
                task_id, src_ep, src_path, dst_ep, dst_path))


class HARPStaging(Staging, RepresentationMixin):
    """Specification for accessing data on a remote executor via harp.

    Parameters
    ----------
    address : str
        Universally unique identifier of the harp address at which the data can be accessed.
        This can be found in the `Manage Endpoints <https://www.harp.org/app/endpoints>`_ page.
    
    harp_directory : str 
    endpoint_path : str, optional
        FIXME
    local_path : str, optional
        FIXME
    """

    def can_stage_in(self, file):
        logger.debug("HARP checking file {}".format(repr(file)))
        return file.scheme == 'harp'

    def can_stage_out(self, file):
        logger.debug("HARP checking file {}".format(repr(file)))
        return file.scheme == 'harp'

    def stage_in(self, dm, executor, file, parent_fut):
        harp_provider = _get_harp_provider(dm.dfk, executor)
        harp_provider._update_local_path(file, executor, dm.dfk)
        stage_in_app = harp_provider._harp_stage_in_app(executor=executor, dfk=dm.dfk)
        app_fut = stage_in_app(outputs=[file], staging_inhibit_output=True, parent_fut=parent_fut)
        return app_fut._outputs[0]

    def stage_out(self, dm, executor, file, app_fu):
        harp_provider = _get_harp_provider(dm.dfk, executor)
        harp_provider._update_local_path(file, executor, dm.dfk)
        stage_out_app = harp_provider._harp_stage_out_app(executor=executor, dfk=dm.dfk)
        return stage_out_app(app_fu, inputs=[file])

    @typeguard.typechecked
    def __init__(self, address: str, harp_directory: str, endpoint_path: Optional[str] = None, local_path: Optional[str] = None):
        self.address = address
        self.harp_directory = harp_directory
        self.endpoint_path = endpoint_path
        self.local_path = local_path
        self.harp = None

    def _harp_stage_in_app(self, executor, dfk):
        executor_obj = dfk.executors[executor]
        f = partial(_harp_stage_in, self, executor_obj)
        return python_app(executors=['data_manager'], data_flow_kernel=dfk)(f)

    def _harp_stage_out_app(self, executor, dfk):
        executor_obj = dfk.executors[executor]
        f = partial(_harp_stage_out, self, executor_obj)
        return python_app(executors=['data_manager'], data_flow_kernel=dfk)(f)

    # could this happen at __init__ time?
    def initialize_harp(self):
        self.frontend_port = 4950
        self.backend_port = 4550

        self.sts_frontend_port = 6950
        self.sts_backend_port = 6550

        self.client_id=1
        if self.harp is None:
            self.harp = get_harp(self)

    def _get_harp_address(self, executor):
        if executor.working_dir:
            working_dir = os.path.normpath(executor.working_dir)
        else:
            raise ValueError("executor working_dir must be specified for harpStaging")
        if self.endpoint_path and self.local_path:
            endpoint_path = os.path.normpath(self.endpoint_path)
            local_path = os.path.normpath(self.local_path)
            common_path = os.path.commonpath((local_path, working_dir))
            if local_path != common_path:
                raise Exception('"local_path" must be equal or an absolute subpath of "working_dir"')
            relative_path = os.path.relpath(working_dir, common_path)
            endpoint_path = os.path.join(endpoint_path, relative_path)
        else:
            endpoint_path = working_dir
        return {'address': self.address,
                'harp_directory': self.harp_directory,
                'endpoint_path': endpoint_path,
                'working_dir': working_dir}

    def _update_local_path(self, file, executor, dfk):
        executor_obj = dfk.executors[executor]
        harp_address = self._get_harp_address(executor_obj)
        file.local_path = os.path.join(harp_address['working_dir'], file.filename)


# this cannot be a class method, but must be a function, because I want
# to be able to use partial() on it - and partial() does not work on
# class methods
def _harp_stage_in(provider, executor, parent_fut=None, outputs=[], staging_inhibit_output=True):
    harp_address = provider._get_harp_address(executor)
    file = outputs[0]
    dst_path = os.path.join(
            harp_address['endpoint_path'], file.filename)

    provider.initialize_harp()

    provider.falcon.transfer_file(
            file.netloc, harp_address['address'],
            file.path, dst_path,harp_address['harp_directory'])


def _harp_stage_out(provider, executor, app_fu, inputs=[]):
    """
    Although app_fu isn't directly used in the stage out code,
    it is needed as an input dependency to ensure this code
    doesn't run until the app_fu is complete. The actual change
    that is represented by the app_fu completing is that the
    executor filesystem will now contain the file to stage out.
    """
    harp_address = provider._get_harp_address(executor)
    file = inputs[0]
    src_path = os.path.join(harp_address['endpoint_path'], file.filename)

    provider.initialize_harp()

    provider.falcon.transfer_file(
        harp_address['address'], file.netloc,
        src_path, file.path, harp_address['harp_directory']
    )
