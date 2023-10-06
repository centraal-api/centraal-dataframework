# centraal-dataframework
`centraal-dataframework` es una libreria de python que implementa practicas para usar de manera eficiente [azure function](https://azure.microsoft.com/en-us/products/functions#:~:text=Azure%20Functions%20is%20an%20event,highest%20level%20of%20hardware%20abstraction.) para ejecutar procesos de transformación y calidad de datos. Los procesos de transformación se ejecutan usando la libreria [pandas](https://pandas.pydata.org/) y las reglas de calidad con [great expectations](https://github.com/great-expectations/great_expectations).


## Ejemplo de uso rapido

```python
"""---Contenido de other/module/logica.py---"""
from centraal_dataframework import task
from centraal_dataframework import task_dq

@task(name = etl_origen)
def process_csv():
    info = dl.read_read_csv("contenedor/path/to/csv/file.csv")
    result = info.pipe(flag = 0).groupby('origen')['conteo'].sum()
    dl.to_csv(result)


@task_dq(name=regla_dq)
def aplicar_regla():
    validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
    validator.expect_column_values_to_not_be_null("pickup_datetime")
```

```yml
#---contenido de config.yaml---
etl_origen:
    horas: 8,12,20
    prioridad: 1
regla_dq:
    horas: 8,12,22
    prioridad: 2
```

```python
"""---contenido de function app --- """
import azure.functions as func 
# se deben importar los modulos custom
from centraal_dataframework import runner
from other.module import logica
# si tiene otro modulo tambien importarlo
# form otro.modulo import logica
# ...
# y luego traer el runner:

app = func.FunctionApp() 
app.register_functions(runner) 

```



## Arquitectura

La arquitectura general de la libreria esta basada en los siguientes servicios:

![Arquitectura](docs/arq.jpg)


El diseño inical del API de la libreria se comopone de los siguientes objetos:

1. config.yml
2. runner
3. task
    1. log
    2. alerta
4. dq-task: es un tipo de `task` especifica para reportar tareas de calidad de datos (`dq`).



```mermaid
---
title: Diseño inicial del API de la libreria
---
  graph TD;
      config[config.yml]:::cdf--usa-->runner:::cdf;
      m[Llamado manual]:::manual-->httptf[Http Trigger function]:::az;
      httptf --> runner;
      runner --encola tarea --> qtf(Queue Trigger Function):::az ;
      qtf --> task:::cdf
      task --usa--> log:::cdf --usa-->alerta:::cdf
      task --> dqt[dq-task]:::cdf
      dqt --> log
      classDef manual stroke:#f66,stroke-width:2px,stroke-dasharray: 5 5
      classDef az stroke:#2D9BF0,stroke-width:2px
      classDef cdf stroke:#FAC710
      
```


