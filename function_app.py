"""Demo."""

import os
import datetime
import time

import azure.functions as func
import azure.durable_functions as df

from centraal_dataframework.tasks import TaskInfo, Task
from centraal_dataframework.pipeline import Pipeline
from centraal_dataframework.configuration import FrameworkConfiguration
from centraal_dataframework.runner import Runner


DUMMY_STRING = "DefaultEndpointsProtocol=https;AccountName=na;AccountKey=key!=k;EndpointSuffix=core.windows.net"
OVERWRITE_TAREA = os.environ.get("SOBREESCRIBIR_TAREA", True)
TAREA_DEBE_ESTAR_CONF = os.environ.get("TAREA_DEBE_CONFIGURACION", True)
UTC_OFFSET = datetime.timedelta(hours=os.environ.get("UTC_OFFSET", -5))


@Task
def first_step_of_pipeline(taskinfo: TaskInfo):
    print("first step start", taskinfo)
    time.sleep(10)
    print("first step delay 10 seconds")
    time.sleep(10)


@Task
def second_step_of_pipeline(taskinfo: TaskInfo):
    print("second step", taskinfo)
    time.sleep(10)
    print("second step delay 30 seconds")
    time.sleep(30)


@Task
def other_step_of_pipeline(taskinfo: TaskInfo):
    print("other second step", taskinfo)
    time.sleep(10)
    print("second step delay 60 seconds")
    time.sleep(60)


bp = df.Blueprint()
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


pipeline = Pipeline(name="test_pipeline", tasks=[first_step_of_pipeline, second_step_of_pipeline])

otherpipeline = Pipeline(
    name="other_pipeline", tasks=[first_step_of_pipeline, other_step_of_pipeline, second_step_of_pipeline]
)

pipeline_test = Pipeline(name="test_pipeline_2", tasks=[first_step_of_pipeline])


config = FrameworkConfiguration()
runner = Runner(pipelines=[pipeline, pipeline_test], conf=config, bp=bp)

app.register_functions(bp)
runner.register_framework_functions(app)
