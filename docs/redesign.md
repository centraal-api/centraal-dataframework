# New Proposed Design

General principles that will follow this new design of the framework:
1. We need to use [durable design patterns](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=in-process%2Cnodejs-v3%2Cv1-model&pivots=python). The key idea is to take advantage of "stateful" workflows inside a serverless compute envieroment, In this way We think that we can increase the usability and keep the costs low associated to use a serverless enviroment.
2. The keys aspects of usability with this stateful features are:
    1. Normally you need to split your "data operations"  in a succesion of related steps.
    2. to keep using the serverless capabilities we want users define functions that then can be a part of a "flow" or "pipeline"
    3. Each of the steps needs to run independtly, we want to at bare minium the framework is able to use all the resources in ona single task an then move to ther other step
    4. each task is related within a pipeline, so we want to give users the control of that flow, i.e, user ca decide if the next task can run depending of the state of the previous step
    5. We want user can able to recieve alerts for each flow, with the details of the steps failed
    6. We want users can debug easly and use the [azure monitor in house tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-monitoring), but this need to be very transparent for the user and not required bery heavy setup
3. We want to avoid configuration files and keep an approach that the user only needs to "code" to configure all the aspects of the flow.
4. We recnognize that sometimes, the user needs to present the steps to a broader audience, we want to provide mechanisms that help to that communication (i.e simple diagrams to describe the flow of taks, etc.)
5. We want the framework communicates with [Azure datalake](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) and be able to reuse datasets among the tasks.
6. We want to take advantage of [blueprints](https://github.com/Azure/azure-functions-durable-python/blob/dev/samples-v2/blueprint/durable_blueprints.py).
7. First versions of this design will rely heaviyly on azure products, if these kay ideas are proven valuable in future other cloud providers could be included.

## General technical details

> Note: This section is use to provide the concepts that needs to be proven, so these ideas has to be seen as things that we need to test and figure the correct way to do it.

The pattern that will be used is the `Function Chaining`, but the idea to adapt to that pattern to use a `pipeline` and a set of `task`, so the idea is to achieve the following experience:

```
from centraal_dataframework import pipeline, task, Result


@pipeline(days="1,2,4", hours="*")
def process_xyz(context):
    result1 = yield context.call_activity("step_1", "Seattle")
    result2 = yield context.call_activity("step_2", "Tokyo")

@task
def step_1(input: str, dl: Datalake):
    return Result(succed=True)


@task
def step_2(input: str, dl: Datalake):
    return Result(succed=True)

```

The initial idea is that each `pipeline` is a orchestation function, and each function is a `task`.

A `pipeline` would have mechanisms to monitor all the tasks.
    - depending of all the result we will activate an alert. The alert will depend of very basic logic app tied to a email inside of the active directory.

Each `task` should yield or return a result to inform the pipeline the status. Each task will be a azure function


We will have a  2 function start function:
1. A function http trigger with a queue function output
2. A function with queue input bidding trigger that will starts the orquestation trigger when there is no orquestation available
3. A fucntion with timer biding that will check if the a new pipeline can be start.







































