import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import *

# Main function
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def startValidation(spark,config, country, date):

    route,header,delimiter,rules = extractParamsFromJson(config)
    object = spark.read.option("delimiter",delimiter).option("header",header).csv(route)
    registerAmount = object.count()
    validationData = validateRules(spark,object,rules,registerAmount,country,route,date)
    return (date, country, route, validationData.select("RULE_CODE"), validationData.select("TEST_FIELD"), validationData.select("SUCESS_RATE"), validationData.select("FAILED_REGISTERS_AMOUNT"), registerAmount,  validationData.select("DATE"))


# Function that extracts the information from de JSON File
# @config Variable that contains the JSON route
def extractParamsFromJson(config):

    file = open(config)
    data = json.load(file)
    input = data.get(InputSection)
    route = input.get(Route)
    #country = input.get(Country)
    header = input.get(Header)
    delimiter = input.get(Delimiter)
    rules = data.get(RulesSection)
    return route,header,delimiter,rules

#Function that validate rules going through the defined options
# @spark Variable containing spark session
# @object DataFrame that is going to be tested
# @rules Dictionary with the rules that are going to be used and the rules parameters
# @registerAmount Amount of registers in the DataFrame
# @country Variable containing the Country 
# @route Variable containing the Route of the Object
def validateRules(spark,object:DataFrame,rules:dict,registerAmount:IntegerType,country: StringType, route: StringType, date):

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
        elif code[0:3] == IntegrityRuleCode:
            referalData = rules[code].get(InputSection)
            data = validateReferentialIntegrity(
                spark,referalData.get(Delimiter),referalData.get(Header),object,referalData.get(Route),rules[code].get(Fields),referalData.get(Fields),registerAmount
                )
            rulesData.append(data)
        else:
            pass
    validationData = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)
    return validationData.withColumn(Country,lit(country)).withColumn(Route,lit(route)).withColumn(TestedRegisterAmount,lit(registerAmount)).withColumn(DateColumn, lit(date))


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

#Function / method that valides strings contained in a column
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @wordList Variable type list containing the exact words that the column needs to have
# @registersAmount Count of total registers in column
def checkContain(columnName, wordList: list, object, registersAmount):
    countString = object.filter((object.columnName).isin(wordList)).count()
    ratio = countString/registersAmount
    return(CheckStringRuleCode, columnName, ratio, countString)

#Function than validates type bool [0,1] contained in column
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkBool(columnName, object, registersAmount):
    countBool = object.filter((object.columnName).isin([0,1])).count()
    ratio = countBool/registersAmount
    return(CheckBoolRuleCode, columnName, ratio, countBool)

#Function that validates the amount of strings that contain "".
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkComillasDobles(columnName, object, registersAmount):
    countComillas = object.filter(col(columnName).rlike("(?i)^*""$")).count()
    ratio = countComillas/registersAmount
    return(CheckComillasDoblesRuleCode, columnName, ratio, countComillas)

#Function that validates the amount of strings that contain "".
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkTypeFloat(columnName, object, registersAmount):
    countFloat = object.filter(col(columnName).rlike("(?i)^*.00$")).count()
    ratio = countFloat/registersAmount
    return(CheckComillasDoblesRuleCode, columnName, ratio, countFloat)