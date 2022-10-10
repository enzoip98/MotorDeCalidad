import json
from typing import List
#from dbutils import DBUtils
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, when
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import *
import datetime
import time

print("Motor de Calidad Version Beta 1.2")

# Main function
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def startValidation(inputspark,config,inputcountry,inputdate):
    global spark
    global country
    global processDate
    spark = inputspark
    country = inputcountry
    processDate = inputdate
    object,rules,entity,project,output = extractParamsFromJson(config)
    registerAmount = object.count()
    validationData = validateRules(object,rules,registerAmount,entity,project)
    writeDf(validationData, output)
    return validationData


# Function that extracts the information from de JSON File
# @config Variable that contains the JSON route
def extractParamsFromJson(config):
    file = open(config)
    data = json.load(file)
    input = data.get(JsonParts.Input)
    output = data.get(JsonParts.Output)
    entity:StringType = input.get(JsonParts.Entity)
    project:StringType = input.get(JsonParts.Project)
    entityDf = readDf(input)
    rules = data.get(JsonParts.Rules)
    print("Extraccion de JSON completada")
    return entityDf,rules,entity,project, output

# Function that reads the CSV file as a Dataframe
def readDf(input):
    header = input.get(JsonParts.Header)
    spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key))
    return spark.read.option("delimiter",input.get(JsonParts.Delimiter)).option("header",header).csv(input.get(JsonParts.Path))

def writeDf(object:DataFrame,output):
    
    header = output.get(JsonParts.Header)
    spark.conf.set(output.get(JsonParts.Account),output.get(JsonParts.Key))
    object.coalesce(One).write.mode("overwrite").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path)))
    return print("Se escribio en el blob")

def writeDfappend(object:DataFrame,output):
    header = output.get(JsonParts.Header)
    spark.conf.set(output.get(JsonParts.Account),output.get(JsonParts.Key))
    object.coalesce(One).write.mode("append").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path)))
    return print("Se escribio en el blob")

#Function that validate rules going through the defined options
def validateRules(object:DataFrame,rules:dict,registerAmount:IntegerType, entity: StringType, project:StringType):
    runTime = datetime.datetime.now()

    rulesData = []
    for code in rules:
        if code == Rules.NullRule.code:
            data = []
            columns = rules[code].get(JsonParts.Fields)
            print("Inicializando reglas de Nulos")
            for field in columns:
                t1_s = time.time()
                data, errorDf = validateNull(object,field,registerAmount)
                errorDesc = "Nulos - " + str(field)
                
                if data[3] > 0 :
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time", lit(runTime))
                    writeDfappend(errorTotal, rules[code].get(JsonParts.Output))

                rulesData.append(data)
                print("regla de nulos: %s segundos" % (time.time() - t1_s))

        elif code == Rules.DuplicatedRule.code:
            t2 = time.time()
            print("Inicializando reglas de Duplicidad")
            testColumn = rules[code].get(JsonParts.Fields)
            data, errorDf = validateDuplicates(object,testColumn,registerAmount)
            errorDesc = "Duplicidad - " + str(testColumn)

            if data[3] > 0 :
                errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                .withColumn("run_time", lit(runTime))
                writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
            
            rulesData.append(data)
            print("regla de duplicados: %s segundos" % (time.time() - t2))

        elif code[0:3] == Rules.IntegrityRule.code:
            print("Inicializando reglas de Integridad referencial")
            t3 = time.time()
            referalData = rules[code].get(JsonParts.Input)
            testColumn = rules[code].get(JsonParts.Fields)
            referenceColumn = referalData.get(JsonParts.Fields)
            data, errorDf = validateReferentialIntegrity(object,referalData, testColumn, referenceColumn,registerAmount)
            errorDesc = "Integridad referencial - " + str(testColumn) + " - "\
            + str(referenceColumn) + " - " + str(referalData)

            if data[3] > 0 :
                errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                .withColumn("run_time", lit(runTime))
                writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
            
            rulesData.append(data) 
            print("regla de IR: %s segundos" % (time.time() - t3))

        elif code == Rules.FormatDate.code:
            print("Inicializando regla de formato")
            
            columnName = rules[code].get(JsonParts.Fields)
            formatDate = rules[code].get(JsonParts.FormatDate)

            for field in columnName:
                t4 = time.time()
                if formatDate in PermitedFormatDate:

                    data, errorDf = validateFormatDate(object, formatDate, field, registerAmount)
                    errorDesc = "Formato - " + str(field)

                    if data[3] > 0 :
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time", lit(runTime))
                        writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
                    
                    rulesData.append(data) 
                    print("regla de formato: %s segundos" % (time.time() - t4))
                
                else:
                    print("Formato de fecha no reconocido por el motor")
                    print("Los formatos permitidos son: ", PermitedFormatDate)
                    print("El formato solicitado fue: ", formatDate)
                    print("regla de formato: %s segundos" % (time.time() - t4))

        else:
            pass
    validationData:DataFrame = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)\
    .withColumn(CountryColumn,lit(country))\
    .withColumn(EntityColumn,lit(entity))\
    .withColumn(TestedRegisterAmountColumn,lit(registerAmount))\
    .withColumn(DateColumn, lit(processDate))\
    .withColumn(ProjectColumn,lit(project))\
    .withColumn(AuditDateColumn,lit(runTime))
    return validationData.select(
        AuditDateColumn,
        ProjectColumn,
        CountryColumn,
        DateColumn,
        EntityColumn,
        TestedFieldsColumn,
        RuleColumn,
        TestedRegisterAmountColumn,
        FailedRegistersAmountColumn,
        SucessRateColumn
        )


#Function that valides the amount of Null registers for certain columns of the dataframe
def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType):

    errorDf = object.filter(col(field).isNull())
    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = notNullCount/ registersAmount
    return (Rules.NullRule.name,field,ratio,nullCount), errorDf

#Function that valides the amount of Duplicated registers for certain columns of the dataframe
def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType):

    duplicates = object.groupBy(fields).count().filter(col("count") != 1)
    errorDf = object.join(duplicates.select(fields), fields, 'inner')
    nonUniqueRegistersAmount = errorDf.count()
    uniqueRegistersAmount = registersAmount - nonUniqueRegistersAmount
    ratio = uniqueRegistersAmount / registersAmount

    return (Rules.DuplicatedRule.name,','.join(fields),ratio,nonUniqueRegistersAmount), errorDf

#Function that valides the equity between certain columns of two objects
def validateReferentialIntegrity(
    testDataFrame: DataFrame,
    referalData,
    testColumn: List,
    referenceColumn: List,
    registersAmount: IntegerType):

    referenceDataFrame = readDf(referalData)
    errorDf = testDataFrame.select(testColumn).join(referenceDataFrame.select(referenceColumn).toDF(*testColumn), on = testColumn, how = LeftAntiType)
    errorCount = errorDf.count()
    ratio = One - errorCount/registersAmount
    return (Rules.IntegrityRule.name,','.join(testColumn),ratio, errorCount), errorDf


def validateFormatDate(object:DataFrame,
    formatDate:StringType,
    columnName:StringType,
    registerAmount:IntegerType):

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    errorDf = object.withColumn("output", to_date(col(columnName), formatDate))\
    .filter(col("output").isNull()).drop("output")

    errorCount = errorDf.count()
    ratio = 1 - errorCount/registerAmount
    return (Rules.FormatDate.name + " - " + formatDate,columnName,ratio, errorCount), errorDf

#Function / method that valides strings contained in a column
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @wordList Variable type list containing the exact words that the column needs to have
# @registersAmount Count of total registers in column
def checkContain(columnName, wordList: list, object, registersAmount):
    countString = object.filter((object.columnName).isin(wordList)).count()
    ratio = countString/registersAmount
    return(Rules.CheckStringRuleCode, columnName, ratio, countString)

#Function than validates type bool [0,1] contained in column
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkBool(columnName, object, registersAmount):
    countBool = object.filter((object.columnName).isin([0,1])).count()
    ratio = countBool/registersAmount
    return(Rules.CheckBoolRuleCode, columnName, ratio, countBool)

#Function that validates the amount of strings that contain "".
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkComillasDobles(columnName, object, registersAmount):
    countComillas = object.filter(col(columnName).rlike("(?i)^*""$")).count()
    ratio = countComillas/registersAmount
    return(Rules.CheckComillasDoblesRuleCode, columnName, ratio, countComillas)

#Function that validates the amount of strings that contain "".
# @object Variable containing dataframe
# @columnName Variable containing the column name of the df
# @registersAmount Count of total registers in column
def checkTypeFloat(columnName, object, registersAmount):
    countFloat = object.filter(col(columnName).rlike("(?i)^*.00$")).count()
    ratio = countFloat/registersAmount
    return(Rules.CheckComillasDoblesRuleCode, columnName, ratio, countFloat)