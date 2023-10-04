import logging 

import azure.functions as func 
import great_expectations as gx
from additional_functions import Validator

bp = func.Blueprint() 

@bp.queue_trigger(arg_name="cdfdq", queue_name="inbaction", connection="storageAccountConnectionString")
@bp.queue_output(arg_name="notq", queue_name="notify", connection="storageAccountConnectionString")
def dequeue_task(cdfdq: func.QueueMessage, notq: func.Out[str]) -> None: 
    logging.info('CDF DEQUEUE INIT') 
    command = cdfdq.get_json()
    funcion = command.name
    asset   = command.asset
    v = Validator(funcion, asset)
    logging.warn(v.execute())


    
    
    

