import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import *
import datetime

print("Motor de Calidad Version Beta 1.0")

# Main function
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def startValidation(spark,config):
    object,rules,country,date,project,entity = extractParamsFromJson(spark,config)
    registerAmount = object.count()
    validationData = validateRules(spark,object,rules,registerAmount,country,entity,date,project)
    return validationData

# Function that reads the CSV file as a Dataframe
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def readDf(spark,input):
    header = input.get(JsonParts.HeaderSection)
    delimiter = input.get(JsonParts.DelimiterSection)
    zone = input.get(JsonParts.ZoneSection)
    country:StringType = input.get(JsonParts.CountrySection)
    entity:StringType = input.get(JsonParts.EntitySection)
    date = input.get(JsonParts.CutOffDateSection)
    route = ""
    if zone == Zone.Landing :
        spark.conf.set(AzureKeys.Landing.Account,AzureKeys.Landing.Key)
        route = f"abfss://landing@adlseuedltb2b001.dfs.core.windows.net/b2b/{country.lower()}/crm_{entity.lower()}/{entity.upper()}_{date}.csv.gz"
    elif zone == Zone.Solution :
        spark.conf.set(AzureKeys.Solution.Account,AzureKeys.Solution.Key)
        route = f"abfss://data@adlseuedltb2b002.dfs.core.windows.net/{input.get(JsonParts.DomainSection)}/{input.get(JsonParts.SubDomainSection)}/{entity}/b2b/{country.lower()}/{date}/"
    return spark.read.option("delimiter",delimiter).option("header",header).csv(route)


# Function that extracts the information from de JSON File
# @config Variable that contains the JSON route
def extractParamsFromJson(spark,config):
    file = open(config)
    data = json.load(file)
    input = data.get(JsonParts.InputSection)
    country = input.get(JsonParts.CountrySection)
    date = input.get(JsonParts.CutOffDateSection)
    entity:StringType = input.get(JsonParts.EntitySection)
    entityDf = readDf(spark,input)
    project = input.get(JsonParts.ProjectSection)
    rules = data.get(JsonParts.RulesSection)
    return entityDf,rules,country,date,project, entity

#Function that validate rules going through the defined options
# @spark Variable containing spark session
# @object DataFrame that is going to be tested
# @rules Dictionary with the rules that are going to be used and the rules parameters
# @registerAmount Amount of registers in the DataFrame
# @country Variable containing the Country 
# @route Variable containing the Route of the Object
def validateRules(spark,object:DataFrame,rules:dict,registerAmount:IntegerType,country: StringType, entity: StringType, date, project):

    rulesData = []
    for code in rules:
        if code == RuleCodes.NullRuleCode:
            data = []
            for field in rules[code].get(JsonParts.FieldsSection):
                data = validateNull(object,field,registerAmount)
                rulesData.append(data)
        elif code == RuleCodes.DuplicatedRuleCode:
            data = validateDuplicates(object,rules[code].get(JsonParts.FieldsSection),registerAmount)
            rulesData.append(data)
        elif code[0:3] == RuleCodes.IntegrityRuleCode:
            referalData = rules[code].get(JsonParts.InputSection)
            data = validateReferentialIntegrity(
                spark,referalData.get(JsonParts.DelimiterSection),referalData.get(JsonParts.HeaderSection),object,readDf(spark,referalData),rules[code].get(JsonParts.FieldsSection),referalData.get(JsonParts.FieldsSection),registerAmount
                )
            rulesData.append(data)
        else:
            pass
    validationData = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns).withColumn(CountryColumn,lit(country)).withColumn(EntityColumn,lit(entity)).withColumn(TestedRegisterAmountColumn,lit(registerAmount)).withColumn(DateColumn, lit(date)).withColumn(ProjectColumn,lit(project)).withColumn(AuditDateColumn,lit(datetime.datetime.now()))
    return validationData.select(
        AuditDateColumn,
        ProjectColumn,
        CountryColumn,
        DateColumn,
        EntityColumn,
        TestedFieldsColumn,
        RuleCodeColumn,
        TestedRegisterAmountColumn,
        FailedRegistersAmountColumn,
        SucessRateColumn
        )


#Function that valides the amount of Null registers for certain columns of the dataframe
# @object DataFrame that is going to be tested
# @field Column that is going to be tested
# @registersAmount Amount of registers in the DataFrame
def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType):

    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = notNullCount/ registersAmount
    return (RuleCodes.NullRuleCode,field,ratio,nullCount)

#Function that valides the amount of Duplicated registers for certain columns of the dataframe
# @object DataFrame that is going to be tested
# @field Column that is going to be tested
# @registersAmount Amount of registers in the DataFrame
def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType):

    uniqueRegistersAmount = object.select(fields).dropDuplicates().count()
    nonUniqueRegistersAmount = registersAmount - uniqueRegistersAmount
    ratio = uniqueRegistersAmount / registersAmount
    return (RuleCodes.DuplicatedRuleCode,','.join(fields),ratio,nonUniqueRegistersAmount)

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
    testDataFrame: DataFrame,
    referenceDataFrame: DataFrame,
    testColumn: List,
    referenceColumn: List,
    registersAmount: IntegerType):
    innerDf = testDataFrame.select(testColumn).join(referenceDataFrame.select(referenceColumn).toDF(*testColumn), on = testColumn, how = LeftAntiType)
    innerCount = innerDf.count()
    ratio = One - innerCount/registersAmount
    return (RuleCodes.IntegrityRuleCode,','.join(testColumn),ratio, innerCount)

#Function / method that valides strings contained in a column
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @wordList Variable type list containing the exact words that the column needs to have
# @registersAmount Count of total registers in column
def checkContain(columnName, wordList: list, object, registersAmount):
    countString = object.filter((object.columnName).isin(wordList)).count()
    ratio = countString/registersAmount
    return(RuleCodes.CheckStringRuleCode, columnName, ratio, countString)

#Function than validates type bool [0,1] contained in column
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkBool(columnName, object, registersAmount):
    countBool = object.filter((object.columnName).isin([0,1])).count()
    ratio = countBool/registersAmount
    return(RuleCodes.CheckBoolRuleCode, columnName, ratio, countBool)

#Function that validates the amount of strings that contain "".
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkComillasDobles(columnName, object, registersAmount):
    countComillas = object.filter(col(columnName).rlike("(?i)^*""$")).count()
    ratio = countComillas/registersAmount
    return(RuleCodes.CheckComillasDoblesRuleCode, columnName, ratio, countComillas)

#Function that validates the amount of strings that contain "".
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkTypeFloat(columnName, object, registersAmount):
    countFloat = object.filter(col(columnName).rlike("(?i)^*.00$")).count()
    ratio = countFloat/registersAmount
    return(RuleCodes.CheckComillasDoblesRuleCode, columnName, ratio, countFloat)