import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import One, LeftAntiType, TestedRegisterAmount,InputSection,Route,Header,Delimiter,RulesSection,Fields,OutputDataFrameColumns,NullRuleCode,DuplicatedRuleCode, KeyField, IntegrityRuleCode, Country


# Main function
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def startValidation(spark,config):

    route,header,delimiter,rules,country = extractParamsFromJson(config)
    object = spark.read.option("delimiter",delimiter).option("header",header).csv(route)
    registerAmount = object.count()
    validationData = validateRules(spark,object,rules,registerAmount,country,route)
    return validationData


# Function that extracts the information from de JSON File
# @config Variable that contains the JSON route
def extractParamsFromJson(config):

    file = open(config)
    data = json.load(file)
    input = data.get(InputSection)
    route = input.get(Route)
    country = input.get(Country)
    header = input.get(Header)
    delimiter = input.get(Delimiter)
    rules = data.get(RulesSection)
    return route,header,delimiter,rules,country

#Function that validate rules going through the defined options
# @spark Variable containing spark session
# @object DataFrame that is going to be tested
# @rules Dictionary with the rules that are going to be used and the rules parameters
# @registerAmount Amount of registers in the DataFrame
# @country Variable containing the Country 
# @route Variable containing the Route of the Object
def validateRules(spark,object:DataFrame,rules:dict,registerAmount:IntegerType,country: StringType, route: StringType):

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
                spark,referalData.get(Delimiter),referalData.get(Header),object,referalData.get(Route),rules[code].get(Fields),referalData.get(Fields),registerAmount
                )
            rulesData.append(data)
        else:
            pass
    validationData = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)
    return validationData.withColumn(Country,lit(country)).withColumn(Route,lit(route)).withColumn(TestedRegisterAmount,lit(registerAmount))


#Function that valides the amount of Null registers for certain columns of the dataframe
# @object DataFrame that is going to be tested
# @field Column that is going to be tested
# @registersAmount Amount of registers in the DataFrame
def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType):

    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = notNullCount/ registersAmount
    return (NullRuleCode,field,ratio,nullCount)

#Function that valides the amount of Duplicated registers for certain columns of the dataframe
# @object DataFrame that is going to be tested
# @field Column that is going to be tested
# @registersAmount Amount of registers in the DataFrame
def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType):

    uniqueRegistersAmount = object.select(fields).dropDuplicates().count()
    nonUniqueRegistersAmount = registersAmount - uniqueRegistersAmount
    ratio = uniqueRegistersAmount / registersAmount
    return (DuplicatedRuleCode,','.join(fields),ratio,nonUniqueRegistersAmount)

#Function that valides the equity between certain columns of two objects
# @spark Variable containing spark session
# @delimiter Variable containing the delimitir of the reference object
# @header Variable that shows if the Object has a header
# @testDataFrame Variable Cotaining the object to be tested
# @referenceRoute Variable Containing the referenceObject route
# @testColumn List with the key columns in the tested object
# @referenceColumn List with the key columns in the reference DataFrame
# @RegistersAmount Amount of registers in the tested DataFrame
def validateReferentialIntegrity(
    spark,
    delimiter,
    header,
    testDataFrame: DataFrame,
    referenceRoute: StringType,
    testColumn: List,
    referenceColumn: List,
    registersAmount: IntegerType):

    referenceDataFrame = spark.read.option("delimiter",delimiter).option("header",header).csv(referenceRoute).select(referenceColumn).toDF(*testColumn)
    innerDf = testDataFrame.select(testColumn).join(referenceDataFrame, on = testColumn, how = LeftAntiType)
    innerCount = innerDf.count()
    ratio = One - innerCount/registersAmount
    return (IntegrityRuleCode,','.join(testColumn),ratio, innerCount)
