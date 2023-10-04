import logging 

import azure.functions as func 

bp = func.Blueprint() 

@bp.route(route="queue_task") 
@bp.queue_output(arg_name="cdfq", queue_name="inbaction", connection="storageAccountConnectionString")
def queue_task(req: func.HttpRequest, cdfq: func.Out[str]) -> func.HttpResponse: 
    logging.info('CDF INIT') 

    name = req.params.get('name') 
    if not name: 
        try: 
            req_body = req.get_json() 
        except ValueError: 
            pass 
        else: 
            name = req_body.get('name') 

    if name: 
        cdfq.set(f'{"task_name":"{name}"}')
        return func.HttpResponse( 
            f"Task {name} succesfully queued") 
    else: 
        return func.HttpResponse( 
            "Task name missing", 
            status_code=400 
        )

