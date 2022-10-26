Para el json es necesario seguir la siguiente estructura

## Input
La sección input debe contener los siguientes atributos

|Atributo|Definición|Tipo|
| ------------- | ------------- | ------------|
|COUNTRY_ID|Acrónimo de Pais de la entidad|Obligatorio|
|PROJECT|Nombre de proyecto|Obligatorio|
|ENTITY_ID|Nombre de la entidad|Obligatorio|
|DOMAIN|Dominio de la entidad|Obligatorio|
|SUB_DOMAIN|Subdominio de la entidad|Obligatorio|
|SEGMENT|Segmento de la entidad|Obligatorio|
|AREA|Area de la entidad|Obligatorio|
|PATH|Ruta en la que se ubica el fichero que será evaluado|Aplica para todos los origenes excepto bases de datos|
|TYPE|Tipo de origen de datos|Opcional (Se aceptan parquet,csv,mysql y postgre, si se deja vacío se toma como csv)|
|HEADER|Valor de Cabecera para CSV (TRUE o FALSE)|Opcional (solo aplica para ficheros csv)|
|ACCOUNT|Nombre de cuenta de acceso para almacenamiento de azure|Opcional (solo aplica csv y parquet)|
|DELIMITER|Caracter delimitador para CSV|Opcional (solo aplica para ficheros csv)|
|KEY|Llave de acceso para almacenamiento de azure|Opcional (solo aplica csv y parquet)|
|HOST|URL del Host por la base de datos|Opcional (solo aplica para bases de datos)|
|PORT|Puerto de conexión a la base de datos (Para MYSQL "3306")|Opcional (solo aplica para bases de datos)|
|DATABASE_NAME|Nombre de la base de datos|Opcional (solo aplica para bases de datos)|
|DATABASE_TABLE|Nombre de la tabla en la base de datos|Opcional (solo aplica para bases de datos)|
|DATABASE_USER|Nombre de usuario de acceso para la base de datos|Opcional (solo aplica para bases de datos)|
|DATABASE_PASSWORD|Contraseña de acceso para la base de datos|Opcional (solo aplica para bases de datos)|

## Error
La sección error contiene la información necesaria para la escritura de la data observada y debe contener los siguiente atributos

|Atributo|Definición|Tipo|
| ------------- | ------------- | ------------|
|PATH|Ruta para la escritura|Obligatorio|
|HEADER|Valor de Cabecera para CSV (TRUE o FALSE)|Obligatorio|
|DELIMITER|Caracter delimitador para CSV|Obligatorio|
|ACCOUNT|Nombre de cuenta de acceso para almacenamiento de azure|Obligatorio|
|KEY|Llave de acceso para almacenamiento de azure|Obligatorio|

## Rules
La sección de reglas contiene las reglas que serán ejecutadas y los parámetros de ejecución que estas necesitan. En esta sección los atributos son los códigos de las reglas. Si es necesario ejecutar una regla más de una vez se puede anteponer 

1. [Regla de Nulos - 101](#regla-de-nulos-101)
2. [Regla de Duplicados - 102](#regla-de-duplicados-102)
3. [Regla de Integridad Referencial - 103](#regla-de-integridad-referencial-103)
4. [Regla de Formato de Fecha - 104](#regla-de-formato-de-fecha-104)
5. [Regla de Rango - 105](#regla-de-rango-105)
6. [Regla de Catálogo - 106](#regla-de-catálogo-106)
7. [Regla de Caracteres Prohibidos - 107](#regla-de-caracteres-prohibidos-107)
8. [Regla de Tipo de Dato(CSV) - 108](#regla-de-tipo-de-datocsv-108)
9. [Regla de Compisición - 109](#regla-de-composición-109)

## Output
La sección output contiene la información necesaria para la escritura de los resultados de la validación y debe contener los siguientes atributos

|Atributo|Definición|Tipo|
| ------------- | ------------- | ------------|
|PATH|Ruta para la escritura|Obligatorio|
|HEADER|Valor de Cabecera para CSV (TRUE o FALSE)|Obligatorio|
|DELIMITER|Caracter delimitador para CSV|Obligatorio|
|ACCOUNT|Nombre de cuenta de acceso para almacenamiento de azure|Obligatorio|
|KEY|Llave de acceso para almacenamiento de azure|Obligatorio|

### Regla de Nulos-101
Esta regla evalúa si los registros dentro de las columnas específicas son nulos
Debe contener los siguientes atributos

|Atributo|Definición|Tipo|
| ------------- | ------------- | ------------|
|FIELDS|Lista que contiene el nombre de los columnas que serán evaluadas|Obligatorio|
|THRESHOLD|Umbral de calidad de ejecución|Obligatorio|
|WRITE|Valor que determina si se escribirá o no la data observada (TRUE o FALSE)|Opcional (Se toma TRUE por defecto)|


### Regla de Duplicados-102
Esta regla evalúa si los registros dentor las columnas específicadas son únicas en la tabla

|Atributo|Definición|Tipo|
| ------------- | ------------- | ------------|
|FIELDS|Lista que contiene el nombre de los columnas que serán evaluadas|Obligatorio|
|THRESHOLD|Umbral de calidad de ejecución|Obligatorio|
|WRITE|Valor que determina si se escribirá o no la data observada (TRUE o FALSE)|Opcional (Se toma TRUE por defecto)|

### Regla de Integridad Referencial-103

### Regla de Formato de Fecha-104

### Regla de Rango-105

### Regla de Catálogo-106

|Atributo|Definición|Tipo|
| ------------- | ------------- | ------------|
|FIELDS|Lista que contiene el nombre de los columnas que serán evaluadas|Obligatorio|
|VALUES|Lista que contiene los valores permitidos|Obligatorio|
|THRESHOLD|Umbral de calidad de ejecución|Obligatorio|
|WRITE|Valor que determina si se escribirá o no la data observada (TRUE o FALSE)|Opcional (Se toma TRUE por defecto)|

### Regla de Caracteres Prohibidos-107

### Regla de Tipo de Dato(CSV)-108

### Regla de Composición-109