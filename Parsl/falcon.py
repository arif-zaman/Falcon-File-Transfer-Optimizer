import logging
import logging
import os
import time

import parsl
from parsl.data_provider.staging import Staging
from parsl.utils import RepresentationMixin

from sender import initialize_transfer, get_fs

logger = logging.getLogger(__name__)


class falconStage(Staging, RepresentationMixin):
    """Performs FTP staging as a separate parsl level task."""

    def __init__(self, address: str):
        self.address = address

    def can_stage_in(self, file):
        logger.debug("FTPSeparateTaskStaging checking file {}".format(repr(file)))
        return file.scheme == 'falcon'

    def stage_in(self, dm, executor, file, parent_fut):
        working_dir = dm.dfk.executors[executor].working_dir
        if working_dir:
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename
        stage_in_app = _falcon_stage_in_app(dm, executor=executor)
        app_fut = stage_in_app(working_dir, outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]


def in_task_transfer_wrapper(func, file, working_dir):
    def wrapper(*args, **kwargs):
        import ftplib
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)

        with open(file.local_path, 'wb') as f:
            ftp = ftplib.FTP(file.netloc)
            ftp.login()
            ftp.cwd(os.path.dirname(file.path))
            ftp.retrbinary('RETR {}'.format(file.filename), f.write)
            ftp.quit()

        result = func(*args, **kwargs)
        return result

    return wrapper


def _falcon_stage_in(working_dir, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    file = outputs[0]

    if file.path == "/home/mbadhan/PycharmProjects/Falcon-File-Transfer-Optimizer2/inputs/":
        logger.info("small  file pause ")
        time.sleep(1)
        logger.info("---resumed-----")

    ds = get_fs()
    ds.filesIn.print_state(logger)

    ds.set_connection("127.0.0.1", 50021,logger)

    logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.info(file.path)
    logger.info("queue size")
    logger.info(ds.q.qsize())
    logger.info(ds.filesIn.get_file_incomplete())
    logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    if ds.filesIn.get_file_incomplete() > 0:
        logger.info("q------- ----------just adding q")
        ds.add_to_queue(file.path, logger)
    else:
        logger.info("q------- ----------stating engine and  adding q")
        ds.add_to_queue(file.path, logger)
        initialize_transfer(ds, logger)

    logger.info("===================================================================================================")
    logger.info(ds.filesIn.get_file_incomplete())

def _falcon_stage_in_app(dm, executor):
    return parsl.python_app(executors=[executor], data_flow_kernel=dm.dfk)(_falcon_stage_in)
