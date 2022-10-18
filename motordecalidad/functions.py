import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import *
import datetime
import time
import operator

print("Motor de Calidad Version Beta 1.3")

# Main function
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def startValidation(inputspark,config):
    global spark
    spark = inputspark
    object,output,country,project,entity,domain,subDomain,segment,area,rules = extractParamsFromJson(config)
    registerAmount = object.count()
    validationData = validateRules(object,rules,registerAmount,entity,project,country,domain,subDomain,segment,area)
    writeDf(validationData, output)
    return validationData


# Function that extracts the information from de JSON File
# @config Variable that contains the JSON route
def extractParamsFromJson(config):
    file = open(config)
    data = json.load(file)
    input = data.get(JsonParts.Input)
    output = data.get(JsonParts.Output)
    country:StringType = input.get(JsonParts.Country)
    project:StringType = input.get(JsonParts.Project)
    entity:StringType = input.get(JsonParts.Entity)
    domain: StringType = input.get(JsonParts.Domain)
    subDomain: StringType = input.get(JsonParts.SubDomain)
    segment: StringType = input.get(JsonParts.Segment)
    area: StringType = input.get(JsonParts.Area)

    entityDf = readDf(input)
    rules = data.get(JsonParts.Rules)
    print("Extraccion de JSON completada")
    return entityDf,output,country,project,entity,domain,subDomain,segment,area,rules

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
def validateRules(object:DataFrame,rules:dict,registerAmount:IntegerType, entity: StringType, project:StringType,country,domain,subDomain,segment,area):
    runTime = datetime.datetime.now()

    rulesData:List = []
    for code in rules:
        if code == Rules.NullRule.code:
            print("Inicializando reglas de Nulos")
            data:List = []
            columns = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            for field in columns:
                t = time.time()
                data, errorDf = validateNull(object,field,registerAmount,entity,threshold)
                errorDesc = "Nulos - " + str(field)
                if data[-One] > Zero :
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time", lit(runTime))
                    writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
                rulesData.append(data)
                print("regla de nulos: %s segundos" % (time.time() - t))

        elif code == Rules.DuplicatedRule.code:
            print("Inicializando reglas de Duplicidad")
            t = time.time()
            testColumn = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            data, errorDf = validateDuplicates(object,testColumn,registerAmount,entity,threshold)
            errorDesc = "Duplicidad - " + str(testColumn)
            if data[-1] > 0 :
                errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                .withColumn("run_time", lit(runTime))
                writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
            rulesData.append(data)
            print("regla de duplicados: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.IntegrityRule.code:
            print("Inicializando reglas de Integridad referencial")
            t = time.time()
            referalData = rules[code].get(JsonParts.Input)
            testColumn = rules[code].get(JsonParts.Fields)
            referenceColumn = referalData.get(JsonParts.Fields)
            referenceEntity = referalData.get(JsonParts.Entity)
            threshold = rules[code].get(JsonParts.Threshold)
            data, errorDf = validateReferentialIntegrity(object,referalData, testColumn, referenceColumn,registerAmount,entity,referenceEntity,threshold)
            errorDesc = "Integridad referencial - " + str(testColumn) + " - "\
            + str(referenceColumn) + " - " + str(referalData)

            if data[-One] > Zero :
                errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                .withColumn("run_time", lit(runTime))
                writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
            
            rulesData.append(data) 
            print("regla de IR: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.FormatDate.code:
            print("Inicializando regla de formato")
            columnName = rules[code].get(JsonParts.Fields)
            formatDate = rules[code].get(JsonParts.FormatDate)
            threshold = rules[code].get(JsonParts.Threshold)
            for field in columnName:
                t = time.time()
                if formatDate in PermitedFormatDate:
                    data, errorDf = validateFormatDate(object, formatDate, field, registerAmount,entity,threshold)
                    errorDesc = "Formato - " + str(field)
                    if data[-One] > Zero :
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time", lit(runTime))
                        writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
                    rulesData.append(data) 
                    print("regla de formato: %s segundos" % (time.time() - t))
                else:
                    print("Formato de fecha no reconocido por el motor")
                    print("Los formatos permitidos son: ", PermitedFormatDate)
                    print("El formato solicitado fue: ", formatDate)
                    print("regla de formato: %s segundos" % (time.time() - t))
        
        elif code[0:3] == Rules.CatalogRule.code:
            print("Inicializando regla de catálogo")
            columnName = rules[code].get(JsonParts.Fields)
            listValues = rules[code].get(JsonParts.Values)
            threshold = rules[code].get(JsonParts.Threshold)
            for field in columnName :
                t = time.time()
                data, errorDf = validateCatalog(object,field,listValues,registerAmount,entity,threshold)
                errorDesc = "Catalogo - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
                rulesData.append(data)
                print("regla de catalogo: %s segundos" % (time.time() - t))
        
        elif code[0:3] == Rules.RangeRule.code:
            print("Inicializando regla de rango")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            minRange = rules[code].get(JsonParts.MinRange)
            maxRange = rules[code].get(JsonParts.MaxRange)

            for field in columnName :
                t = time.time()
                data, errorDf = validateRange(object,field,registerAmount,entity,threshold,minRange,maxRange)
                errorDesc = "Rango - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
                rulesData.append(data)
                print("regla de rango: %s segundos" % (time.time() - t))
        
        elif code == Rules.ForbiddenRule.code:
            print("Inicializando regla de caracteres prohibidos")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            listValues = rules[code].get(JsonParts.Values)

            for field in columnName :
                t = time.time()
                data, errorDf = validateForbiddenCharacters(object,field,listValues,registerAmount,entity,threshold)
                errorDesc = "Caracteres prohibidos - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, rules[code].get(JsonParts.Output))
                rulesData.append(data)
                print("regla de caracteres prohibidos: %s segundos" % (time.time() - t))

        else:
            pass
    validationData:DataFrame = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)
    return validationData.select(
        Country.value(lit(country)),
        Project.value(lit(project)),
        Entity.value(lit(entity)),
        TestedFields.column,
        Domain.value(lit(domain)),
        SubDomain.value(lit(subDomain)),
        Segment.value(lit(segment)),
        Area.value(lit(area)),
        AuditDate.value(lit(datetime.date.today().strftime("%Y-%m-%d"))),
        FunctionCode.column,
        RuleCode.column,
        DataRequirement.column,
        Threshold.column,
        RuleGroup.column,
        RuleProperty.column,
        TestedRegisterAmount.column,
        PassedRegistersAmount.value(TestedRegisterAmount.column - FailedRegistersAmount.column),
        SucessRate.column,
        FailedRegistersAmount.column,
        FailRate.value(lit(One)-SucessRate.column)
        )

#Function that valides the amount of Null registers for certain columns of the dataframe
def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType,entity: StringType,threshold):
    dataRequirement = f"El atributo {entity}.{field} debe ser obligatorio (NOT NULL)."
    errorDf = object.filter(col(field).isNull())
    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = notNullCount/ registersAmount
    return (registersAmount,Rules.NullRule.code,Rules.NullRule.name,Rules.NullRule.property,Rules.NullRule.code + "/" + entity + "/" + field,threshold,dataRequirement,field,ratio,nullCount), errorDf

#Function that valides the amount of Duplicated registers for certain columns of the dataframe
def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType,entity: StringType,threshold):
    fieldString = ','.join(fields)
    dataRequirement = f"Todos los identificadores {entity}.({fieldString}) deben ser distintos (PRIMARY KEY)."
    duplicates = object.groupBy(fields).count().filter(col("count") != 1)
    errorDf = object.join(duplicates.select(fields), fields, 'inner')
    nonUniqueRegistersAmount = errorDf.count()
    uniqueRegistersAmount = registersAmount - nonUniqueRegistersAmount
    ratio = uniqueRegistersAmount / registersAmount

    return (registersAmount,Rules.DuplicatedRule.code,Rules.DuplicatedRule.name,Rules.DuplicatedRule.property,Rules.DuplicatedRule.code + "/" + entity + "/" + fieldString,threshold,dataRequirement,fieldString,ratio,nonUniqueRegistersAmount), errorDf

#Function that valides the equity between certain columns of two objects
def validateReferentialIntegrity(
    testDataFrame: DataFrame,
    referalData,
    testColumn: List,
    referenceColumn: List,
    registersAmount: IntegerType,
    entity: StringType,
    referenceEntity: StringType,
    threshold):
    fieldString = ','.join(testColumn)
    referenceFieldString = ','.join(referenceColumn)
    dataRequirement = f"El atributo {entity}.({fieldString}) debe ser referencia a la tabla y atributo {referenceEntity}.({referenceFieldString}) (FOREIGN KEY)."
    referenceDataFrame = readDf(referalData)
    errorDf = testDataFrame.select(testColumn).join(referenceDataFrame.select(referenceColumn).toDF(*testColumn), on = testColumn, how = LeftAntiType)
    errorCount = errorDf.count()
    ratio = (One - errorCount/registersAmount) * 100
    return (registersAmount,Rules.IntegrityRule.code,Rules.IntegrityRule.name,Rules.IntegrityRule.property,Rules.IntegrityRule.code + "/" + entity + "/" + fieldString,threshold,dataRequirement,fieldString,ratio, errorCount), errorDf


def validateFormatDate(object:DataFrame,
    formatDate:StringType,
    columnName:StringType,
    registerAmount:IntegerType,
    entity:StringType,
    threshold):
    dataRequirement = f"El atributo {entity}.{columnName} debe tener el formato {formatDate}."
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    errorDf = object.withColumn("output", to_date(col(columnName), formatDate))\
    .filter(col("output").isNull()).drop("output")
    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * 100
    return (registerAmount,Rules.FormatDate.code,Rules.FormatDate.name + " - " + formatDate,Rules.FormatDate.property,Rules.FormatDate.code + "/" + entity + "/" + columnName,threshold,dataRequirement,columnName,ratio, errorCount), errorDf

def validateRange(object:DataFrame,
    columnName:StringType,
    registerAmount:IntegerType,
    entity,
    threshold,
    minRange:float = None,
    maxRange:float = None,
    includeLimitRight:bool = True,
    includeLimitLeft:bool = True,
    inclusive:bool = True,):
    dataRequirement =  f"El atributo {entity}.{columnName}, debe estar entre los valores {minRange} y {maxRange}"
    opel,opeg=chooseComparisonOparator(includeLimitLeft,includeLimitRight,inclusive)

    if inclusive:
        if minRange is None and maxRange is not None:
            errorDf = object.filter(opeg(col(columnName),maxRange))
        elif minRange is not None and maxRange is None:
            errorDf = object.filter(opel(col(columnName), minRange))
        else: 
            errorDf = object.filter(opel(col(columnName),minRange) | opeg(col(columnName),maxRange))       
    else:
        errorDf = object.filter(opel(col(columnName),minRange) & opeg(col(columnName),maxRange))

    errorCount = errorDf.count()
    ratio = 1 - errorCount/registerAmount

    return (registerAmount,Rules.RangeRule.code,Rules.RangeRule.name,Rules.RangeRule.property,Rules.RangeRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount), errorDf

def chooseComparisonOparator(includeLimitLeft:bool,includeLimitRight:bool,inclusive:bool):
    res=[]
    if inclusive:
        if includeLimitLeft:
            res.append(operator.lt)
        else:
            res.append(operator.le)

        if includeLimitRight:
            res.append(operator.gt)
        else:
            res.append(operator.ge)

    else:
        if includeLimitLeft:
            res.append(operator.ge)
        else:
            res.append(operator.gt)

        if includeLimitRight:
            res.append(operator.le)
        else:
            res.append(operator.lt)
    
    return res[0],res[1]


def validateCatalog(object:DataFrame,
    columnName:StringType, 
    listValues:list,
    registerAmount:IntegerType,
    entity:StringType,
    threshold):
    fieldsString = ','.join(listValues)
    dataRequirement = f"El atributo {entity}.{columnName}, debe tomar solo los valores {fieldsString}."
    errorDf = object.filter(~col(columnName).isin(listValues))

    errorCount = errorDf.count()
    ratio = One - errorCount/registerAmount

    return (registerAmount,Rules.CatalogRule.code,Rules.CatalogRule.name,Rules.CatalogRule.property,Rules.CatalogRule.code + "/" + entity + columnName ,threshold,dataRequirement,columnName, ratio, errorCount), errorDf 


def validateForbiddenCharacters(object:DataFrame,
    columnName:StringType, 
    listValues:list,
    registerAmount:IntegerType,
    entity:StringType,
    threshold):

    fieldsString = ','.join(listValues)

    dataRequirement = f"El atributo {entity}.{columnName}, no debe contener los siguentes caracteres: {fieldsString}."

    vals="["+"".join(listValues)+"]"
    object = object.withColumn("replaced", regexp_replace(col(columnName),vals, ""))

    errorDf=object.filter(col(columnName)!=col('replaced')).drop('replaced')

    errorCount = errorDf.count()
    ratio = 1 - errorCount/registerAmount

    return (registerAmount, Rules.ForbiddenRule.code,Rules.ForbiddenRule.name,Rules.ForbiddenRule.property,Rules.ForbiddenRule.code + "/" + entity + columnName ,threshold,dataRequirement, columnName, ratio, errorCount), errorDf 

