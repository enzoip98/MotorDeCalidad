##Constants
from pyspark.sql.functions import col
class Rules:
    class Pre_Requisites:
        code = "100"
    class NullRule:
        name = "Completitud"
        property = "Completitud de Registro"
        code = "101"
    class DuplicatedRule:
        name = "Consistencia"
        property = "Riesgo de Inconsistencia"
        code = "102"
    class IntegrityRule:
        name = "Consistencia"
        property = "Integridad referencial"
        code = "103"
    class FormatDate:
        name = "Exactitud"
        property = "Exactitud Sintactica"
        code = "104"
    class RangeRule:
        name = "Exactitud"
        property = "Rango de Exactitud"
        code = "105"
    class CatalogRule:
        name = "Exactitud"
        property = "Exactitud Sintactica"
        code = "106"
    class ForbiddenRule:
        name = "Exactitud"
        property = "Exactitud Sintactica"
        code = "107"
    class Type:
        name = "Exactitud"
        property = "Exactitud Sintactica"
        code = "108"
    class Composision:
        name = "Consistencia"
        property = "Consistencia de Formato"
        code = "109"
    class LengthRule:
        name = "Consistencia"
        property = "Consistencia de Formato"
        code = "110"
    class DataTypeRule:
        name = "Exactitud"
        property = "Exactitud de Formato"
        code = "111"
    class NumericFormatRule:
        name = "Exactitud"
        property = "Exactitud de Formato"
        code = "112" 
    class OperationRule:
        name = "Exactitud"
        property = "Exactitud de Resultado"
        code = "113"
    class StatisticsResult:
        name = "Exactitud"
        property = "Resultado"
        code = "114"

class JsonParts:
    Input = "INPUT"
    Output = "OUTPUT"
    Rules = "RULES"
    Header= "HEADER"
    Delimiter = "DELIMITER"
    Fields = "FIELDS"
    Country = "COUNTRY_ID"
    Entity = "ENTITY_ID"
    Project = "PROJECT"
    Path = "PATH"
    Account = "ACCOUNT"
    Key = "KEY"
    FormatDate = "FORMAT_DATE"
    Domain = "DOMAIN"
    SubDomain = "SUB_DOMAIN"
    Segment = "SEGMENT"
    Area = "AREA"
    Threshold = "THRESHOLD"
    Values = "VALUES"
    MinRange = "MIN_RANGE"
    MaxRange = "MAX_RANGE"
    DataType = "DATA_TYPE"
    Type = "TYPE"
    Write = "WRITE"
    Error = "ERROR"
    Host = "HOST"
    Port = "PORT"
    DBName = "DATABASE_NAME"
    DBTable = "DATABASE_TABLE"
    DBUser = "DATABASE_USER"
    DBPassword = "DATABASE_PASSWORD"
    MaxInt = "MAX_INT"
    Sep = "SEP"
    NumDec = "NUM_DEC"
    TempPath = "TEMPORAL_PATH"
    Filter = "FILTER"
    Input_val = "INPUT_VAL"
    Error_val = "ERROR_VAL"
    Operator = "OPERATOR"
    Scope = "SCOPE"
    Partitions = "PARTITIONS"
    DataDate = "DATA_DATE"

class Field:
    def __init__(self,colName):
        self.name = colName
        self.column = col(colName)
    def value(self,colValue):
        return (colValue).alias(self.name)
CountryId = Field("CODIGO_DE_PAIS")
DataDate = Field("FECHA_DE_INFORMACION")
Country = Field("PAIS")
Project = Field("PROYECTO")
Entity = Field("ENTIDAD")
AuditDate = Field("FECHA_EJECUCION_REGLA")
Domain  = Field("DOMINIO_ENTIDAD")
SubDomain = Field("SUBDOMINIO_ENTIDAD")
Segment = Field("SEGMENTO_ENTIDAD")
Area = Field("AREA_FUNCIONAL_ENTIDAD")
TestedFields = Field("ATRIBUTOS")
RuleCode = Field("CODIGO_REGLA")
RuleDescription = Field("DESCRIPCION_FUNCION")
SucessRate = Field("PORCENTAJE_CALIDAD_OK")
TestedRegisterAmount = Field("TOTAL_REGISTROS_VALIDADOS")
FailedRegistersAmount = Field("TOTAL_REGISTROS_ERRONEOS")
PassedRegistersAmount = Field("TOTAL_REGISTROS_CORRECTOS")
DataRequirement = Field("REQUISITO_DATOS")
QualityRequirement = Field("REQUISITO_CALIDAD")
RiskApetite = Field("APETITO_RIESGO")
Threshold = Field("UMBRAL_ACEPTACION")
RuleGroup = Field("CARACTERISTICA_REGLA")
RuleProperty = Field("PROPIEDAD_REGLA")
FailRate = Field("PORCENTAJE_CALIDAD_KO")
FunctionCode = Field("CODIGO_FUNCION")

LeftAntiType = "leftanti"
One = 1
Zero = 0
OneHundred = 100
OutputDataFrameColumns = [TestedRegisterAmount.name,FunctionCode.name,RuleGroup.name,RuleProperty.name,RuleCode.name,Threshold.name,DataRequirement.name,TestedFields.name,SucessRate.name,FailedRegistersAmount.name]
PermitedFormatDate = ['yyyy-MM-dd','yyyy/MM/dd', 'yyyyMMdd', 'yyyyMM']
