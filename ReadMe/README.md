# Motor de Calidad de Datos
El presente documenta presenta las funcionalidades del motor de calidad desde el punto técnico para su utilización
## Instalación
Para la instalación del motor de calidad en el kernel a utilizar debe ejecutarse el comando 

```python
python setup.py bdist_wheel 
```

Este comando creara un archivo .whl en la carpeta dist

Luego este archivo debe ser agregado como librería en el kernel en el que se desea utilizar el motor.

Finalmente para hacer uso del motor se debe crear una sesión de spark e importar la función starValidation desde la librería

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Motor_de_Calidad").getOrCreate()
from motordecalidad.functions import startValidation
```
Para la utilización de la función startValidation es necesario pasarle como parámetros de entrada la sesión de spark y la ruta al json
con la configuración de las reglas

[Estructura de Json](json.md)
