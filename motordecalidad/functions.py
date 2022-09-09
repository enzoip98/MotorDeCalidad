import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import InputSection,Route,Header,Delimiter,RulesSection,Fields,OutputDataFrameColumns,NullRuleCode,DuplicatedRuleCode, KeyField, IntegrityRuleCode

def startValidation(spark,config):
    rout,header,delimiter,rules = extractParamsFromJson(config)
    object = spark.read.option("delimiter",delimiter).option("header",header).csv(rout)
    registerAmount = object.count()
    validationData = validateRules(spark,object,rules,registerAmount)
    return validationData

def extractParamsFromJson(config):
    file = open(config)
    data = json.load(file)
    input = data.get(InputSection)
    rout = input.get(Route)
    header = input.get(Header)
    delimiter = input.get(Delimiter)
    rules = data.get(RulesSection)
    return rout,header,delimiter,rules

def validateRules(spark,object:DataFrame,rules:dict,registerAmount:IntegerType):
    rulesData = []
    for code in rules:
        if code == NullRuleCode:
            data = []
            for field in rules[code].get(Fields):
                data = validateNull(object,field,registerAmount)
                rulesData.append(data)
        elif code == DuplicatedRuleCode:
            data = validateDuplicates(object,rules[code].get(Fields),registerAmount)
            rulesData.append(data)
        elif code == IntegrityRuleCode:
            referalData = rules[code].get(InputSection)
            data = validateReferentialIntegrity(
                object,referalData.get(Route),rules[code].get(Fields),referalData.get(KeyField),registerAmount
                )
            rulesData.append(data)
        else:
            pass
    validationData = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)
    return validationData

def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType): 
    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = notNullCount/ registersAmount
    return (NullRuleCode,field,ratio,nullCount)

def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType):
    uniqueRegistersAmount = object.select(fields).dropDuplicates().count()
    nonUniqueRegistersAmount = registersAmount - uniqueRegistersAmount
    ratio = uniqueRegistersAmount / registersAmount
    return (DuplicatedRuleCode,fields,ratio,nonUniqueRegistersAmount)

def validateReferentialIntegrity(
    testDataFrame: DataFrame,
    referenceDataFrame: DataFrame,
    key_1: List, key_2: List,
    registersAmount: IntegerType):
    referenceDataFrame = referenceDataFrame.select(key_2).toDF(*key_1).dropDuplicates() # No debe tener registros duplicados
    innerDf = testDataFrame.select(key_1).join(referenceDataFrame, on = key_1, how = "inner")
    innerCount = innerDf.count()
    nonIntegrityRegistersAmount = registersAmount - innerCount
    ratio = innerCount/ registersAmount
    return (NullRuleCode,key_1,ratio, nonIntegrityRegistersAmount)
