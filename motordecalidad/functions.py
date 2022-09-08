import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType

##Constants
NullRuleCode = "101"
DuplicatedRuleCode = "102"
InputSection = "INPUT"
RulesSection = "RULES"
Route = "ROUTE"
Header = "HEADER"
Delimiter = "DELIMITER"
Fields = "FIELDS"
OutputDataFrameColumns = ["CODIGO_DE_REGLA","RATIO_DE_ERROR","CANTIDAD_DE_REGISTROS_CON_ERROR"]

def extractParamsFromJson(config):
    file = open(config)
    data = json.load(file)
    Input = data.get(InputSection)
    Rout = Input.get(Route)
    Header = Input.get()
    Rules = data.get(RulesSection)
    return[Rout,Header,Delimiter,Rules]

def validateRules(spark,object:DataFrame,rules:dict,registerAmount:IntegerType):
    RulesData = [],
    for code in rules:
        match code:
            case "101":
                Data = validateNull(object,rules[code].get(Fields),registerAmount)
            case "102":
                Data = validateDuplicates(object,rules[code].get(Fields),registerAmount)
            case _:
                pass
        RulesData.append(Data)
    validationData = spark.createDataFrame(data = RulesData, schema = OutputDataFrameColumns)
    return validationData

def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType): 
    nullCount = object.select(field).filter(col(field).isNull()).count()
    ratio = nullCount/ registersAmount
    return (NullRuleCode,ratio,nullCount)

def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType):
    uniqueRegistersAmount = object.select(fields).dropDuplicates().count()
    nonUniqueRegistersAmount = registersAmount - uniqueRegistersAmount
    ratio = nonUniqueRegistersAmount / registersAmount
    return (DuplicatedRuleCode,ratio,nonUniqueRegistersAmount)

def validateIntegridadReferencial(
    testDataFrame: DataFrame,
    referenceDataFrame: DataFrame,
    key_1: List, key_2: List,
    registersAmount: IntegerType):
    referenceDataFrame = referenceDataFrame.select(key_2).toDF(*key_1).dropDuplicates() # No debe tener registros duplicados
    innerDf = testDataFrame.select(key_1).join(referenceDataFrame, on = key_1, how = "inner")
    innerCount = innerDf.count()
    nonIntegrityRegistersAmount = registersAmount - innerCount
    ratio = nonIntegrityRegistersAmount/ registersAmount
    return [NullRuleCode, ratio, nonIntegrityRegistersAmount]
