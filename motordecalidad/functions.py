import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
from motordecalidad.constants import *
import datetime
import time
import operator

print("Motor de Calidad Version Beta 1.4")

# Main function
# @spark Variable containing spark session
# @config Route with the json that contains de information of the execution
def startValidation(inputspark,config):
    global spark
    spark = inputspark
    print("Inicio de validacion")
    object,output,country,project,entity,domain,subDomain,segment,area,rules,error = extractParamsFromJson(config)
    registerAmount = object.count()
    validationData = validateRules(object,rules,registerAmount,entity,project,country,domain,subDomain,segment,area,error)
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
    error = data.get(JsonParts.Error)

    entityDf = readDf(input)
    rules = data.get(JsonParts.Rules)
    print("Extraccion de JSON completada")
    return entityDf,output,country,project,entity,domain,subDomain,segment,area,rules,error

# Function that reads the CSV file as a Dataframe
def readDf(input):
    print("inicio de lectura de informacion")
    type = input.get(JsonParts.Type)
    if type == "csv":
        spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key))
        header = input.get(JsonParts.Header)
        return spark.read.option("delimiter",input.get(JsonParts.Delimiter)).option("header",header).csv(input.get(JsonParts.Path))
    elif type == "parquet":
        spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key))
        return spark.read.parquet(input.get(JsonParts.Path))
    elif type == "postgre" :
        driver = "org.postgresql.Driver"
        database_host = input.get(JsonParts.Host)
        database_port = input.get(JsonParts.Port)
        database_name = input.get(JsonParts.DBName)
        table = input.get(JsonParts.DBTable)
        user = input.get(JsonParts.DBUser)
        password = JsonParts.DBPassword
        url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"
        return spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).option("user", user).option("password", password).load()
    elif type == "mysql" : 
        driver = "org.mariadb.jdbc.Driver"
        database_host = input.get(JsonParts.Host)
        database_port = input.get(JsonParts.Port)
        database_name = input.get(JsonParts.DBName)
        table = input.get(JsonParts.DBTable)
        user = input.get(JsonParts.DBUser)
        password = input.get(JsonParts.DBPassword)
        url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"
        return spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).option("user", user).option("password", password).load()
    elif type == "teradata" :
        driver = "cdata.jdbc.teradata.TeradataDriver"
        database_host = input.get(JsonParts.Host)
        database_name = input.get(JsonParts.DBName)
        table = input.get(JsonParts.DBTable)
        user = input.get(JsonParts.DBUser)
        password = input.get(JsonParts.DBPassword)
        url = f"jdbc:teradata:RTK=5246...;User={user};Password={password};Server={database_host};Database={database_name};"
        return spark.read.format ("jdbc") \
        .option ("driver", driver) \
        .option ("url", url) \
        .option ("dbtable", table) \
        .load ()
    elif type == "synapse" :
        spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key))
        return spark.read \
        .format("com.databricks.spark.sqldw") \
        .option("url",input.get(JsonParts.Host)) \
        .option("tempDir",input.get(JsonParts.TempPath)) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbTable", input.get(JsonParts.DBTable)) \
        .load()
    elif type == "oracle" :
        driver = "cdata.jdbc.oracleoci.OracleOCIDriver"
        database_host = input.get(JsonParts.Host)
        database_port = input.get(JsonParts.Port)
        table = input.get(JsonParts.DBTable)
        user = input.get(JsonParts.DBUser)
        password = input.get(JsonParts.DBPassword)
        url = f"jdbc:oracleoci:RTK=5246...;User={user};Password={password};Server={database_host};Port={database_port};"
        return spark.read.format ( "jdbc" ) \
        .option ( "driver" , driver) \
        .option ( "url" , url) \
        .option ( "dbtable" , table) \
        .load ()
    else:
        spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key))
        header = input.get(JsonParts.Header)
        return spark.read.option("delimiter",input.get(JsonParts.Delimiter)).option("header",header).csv(input.get(JsonParts.Path))

def writeDf(object:DataFrame,output):
    header:StringType = output.get(JsonParts.Header)
    spark.conf.set(output.get(JsonParts.Account),output.get(JsonParts.Key))
    object.coalesce(One).write.mode("overwrite").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path)))
    print("Se escribio en el blob")

def writeDfappend(object:DataFrame,output,RuleId,Write):
    
    if Write == False :
        print("Se omitio escritura")
    else:
        header:StringType = output.get(JsonParts.Header)
        spark.conf.set(output.get(JsonParts.Account),output.get(JsonParts.Key))
        object.coalesce(One).write.mode("append").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path))+ RuleId)
        print("Se escribio en el blob")

#Function that validate rules going through the defined options
def validateRules(object:DataFrame,rules:dict,registerAmount:IntegerType, entity: StringType, project:StringType,country,domain,subDomain,segment,area,error):
    runTime = datetime.datetime.now()

    rulesData:List = []
    for code in rules:
        if code[0:3] == Rules.NullRule.code:
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
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de nulos: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.DuplicatedRule.code:
            print("Inicializando reglas de Duplicidad")
            t = time.time()
            testColumn = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            data, errorDf = validateDuplicates(object,testColumn,registerAmount,entity,threshold)
            errorDesc = "Duplicidad - " + str(testColumn)
            if data[-One] > 0 :
                errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                .withColumn("run_time", lit(runTime))
                writeDfappend(errorTotal,error, code,rules[code].get(JsonParts.Write))
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
                writeDfappend(errorTotal,error, code,rules[code].get(JsonParts.Write))
            
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
                        writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
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
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
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
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de rango: %s segundos" % (time.time() - t))
        
        elif code[0:3] == Rules.ForbiddenRule.code:
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
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de caracteres prohibidos: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.Type.code:
            print("Inicializando regla de tipo de dato")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            data_Type = rules[code].get(JsonParts.DataType) 

            for field in columnName :
                t = time.time()
                data, errorDf = validateType(object,data_Type,field,registerAmount,entity,threshold)
                errorDesc = "Tipo de dato error - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de caracteres tipo de dato: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.Composision.code:
            print("Inicializando regla de composicion")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            patialColumns = rules[code].get(JsonParts.Values)
            for field in columnName:
                t = time.time()
                data, errorDf = validateComposision(object,field,patialColumns,registerAmount,entity,threshold)
                errorDesc = "Composicion error - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de caracteres composicion: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.LengthRule.code:
            print("Inicializando regla de longitud")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            minRange = rules[code].get(JsonParts.MinRange)
            maxRange = rules[code].get(JsonParts.MaxRange)

            for field in columnName :
                t = time.time()
                data, errorDf = validateLength(object,field,registerAmount,entity,threshold,minRange,maxRange)
                errorDesc = "Longitud - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de longitud: %s segundos" % (time.time() - t))
        
        elif code[0:3] == Rules.DataTypeRule.code:
            print("Inicializando regla de tipo de dato parquet")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)
            data_Type = rules[code].get(JsonParts.DataType)            

            for field in columnName :
                t = time.time()
                data = validateDataType(object,field,registerAmount,entity,threshold,data_Type)
                rulesData.append(data)
                print("regla de tipo de dato parquet: %s segundos" % (time.time() - t))

        elif code[0:3] == Rules.NumericFormatRule.code:
            print("Inicializando regla de tipo de formato numerico")
            columnName = rules[code].get(JsonParts.Fields)
            threshold = rules[code].get(JsonParts.Threshold)   
            maxInt = rules[code].get(JsonParts.MaxInt)
            sep = rules[code].get(JsonParts.Sep)
            numDec = rules[code].get(JsonParts.NumDec)  

            for field in columnName :
                t = time.time()
                data, errorDf = validateFormatNumeric(object,field,registerAmount,entity,threshold,maxInt,sep,numDec)
                    
                errorDesc = "Formato Numerico - " + field
                if data[-One] > Zero:
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time",lit(runTime))
                    writeDfappend(errorTotal, error, code,rules[code].get(JsonParts.Write))
                rulesData.append(data)
                print("regla de formato numerico: %s segundos" % (time.time() - t))

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
        FailRate.value(lit(OneHundred)-SucessRate.column)
        )

#Function that valides the amount of Null registers for certain columns of the dataframe
def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType,entity: StringType,threshold):
    dataRequirement = f"El atributo {entity}.{field} debe ser obligatorio (NOT NULL)."
    errorDf = object.filter(col(field).isNull())
    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = (notNullCount/ registersAmount) * OneHundred
    return (registersAmount,Rules.NullRule.code,Rules.NullRule.name,Rules.NullRule.property,Rules.NullRule.code + "/" + entity + "/" + field,threshold,dataRequirement,field,ratio,nullCount), errorDf

#Function that valides the amount of Duplicated registers for certain columns of the dataframe
def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType,entity: StringType,threshold):
    fieldString = ','.join(fields)
    dataRequirement = f"Todos los identificadores {entity}.({fieldString}) deben ser distintos (PRIMARY KEY)."
    duplicates = object.groupBy(fields).count().filter(col("count") != One)
    errorDf = object.join(duplicates.select(fields), fields, 'inner')
    nonUniqueRegistersAmount = errorDf.count()
    uniqueRegistersAmount = registersAmount - nonUniqueRegistersAmount
    ratio = (uniqueRegistersAmount / registersAmount) * OneHundred

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
    ratio = (One - errorCount/registersAmount) * OneHundred
    return (registersAmount,Rules.IntegrityRule.code,Rules.IntegrityRule.name,Rules.IntegrityRule.property,Rules.IntegrityRule.code + "/" + entity + "/" + fieldString,threshold,dataRequirement,fieldString,ratio, errorCount), errorDf


def validateFormatDate(object:DataFrame,
    formatDate:StringType,
    columnName:StringType,
    registerAmount:IntegerType,
    entity:StringType,  
    threshold):
    dataRequirement = f"El atributo {entity}.{columnName} debe tener el formato {formatDate}."
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    errorDf = object.withColumn("output", to_date(col(columnName).cast('string'), formatDate))\
    .filter(col("output").isNull()).drop("output")
    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred
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
    ratio = (One - errorCount/registerAmount) * OneHundred

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
    
    return res[Zero],res[One]


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
    ratio = (One - errorCount/registerAmount) * OneHundred

    return (registerAmount,Rules.CatalogRule.code,Rules.CatalogRule.name,Rules.CatalogRule.property,Rules.CatalogRule.code + "/" + entity + "/" + columnName ,threshold,dataRequirement,columnName, ratio, errorCount), errorDf 


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
    ratio = (One - errorCount/registerAmount) * OneHundred

    return (registerAmount, Rules.ForbiddenRule.code,Rules.ForbiddenRule.name,Rules.ForbiddenRule.property,Rules.ForbiddenRule.code + "/" + entity + "/" + columnName ,threshold,dataRequirement, columnName, ratio, errorCount), errorDf 


def validateType(object:DataFrame,
    data_Type:StringType,
    columnName:StringType,
    registerAmount:IntegerType,
    entity:StringType,
    threshold):

    dataRequirement = f"El atributo {entity}.{columnName} debe ser de tipo {data_Type}."


    errorDf = object.filter(col(columnName).isNotNull()).withColumn("output", col(columnName).cast(data_Type))\
    .filter(col("output").isNull()).drop("output")

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred
    return (registerAmount, Rules.Type.code, Rules.Type.name + " - " + data_Type, Rules.Type.property, Rules.Type.code + "/" + entity + "/" + columnName,threshold,dataRequirement,columnName,ratio, errorCount), errorDf

def validateComposision(object: DataFrame,
    columnName:StringType,
    partialColumns:list,
    registerAmount:IntegerType,
    entity: StringType,
    threshold: StringType):

    fieldsString = ','.join(partialColumns)
    dataRequirement = f"El atributo {entity}.{columnName} en todas las tablas tiene que tener la siguiente estructura {fieldsString}"
    errorDf = object.filter(col(columnName) != concat_ws("_",*partialColumns))
    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return (registerAmount, Rules.Composision.code, Rules.Composision.name, Rules.Composision.property, Rules.Composision.code + "/" + entity + "/" + columnName,threshold,dataRequirement,columnName,ratio, errorCount), errorDf

def validateLength(object:DataFrame,
    columnName:StringType,
    registerAmount:IntegerType,
    entity,
    threshold,
    minRange:float = None,
    maxRange:float = None,):

    dataRequirement =  f"El atributo {entity}.{columnName}, debe contener este numero de caracteres {minRange} y {maxRange}"

    opel,opeg = chooseComparisonOparator(True, True, True)

    if minRange is None and maxRange is not None:
        errorDf = object.filter(opeg(length(col(columnName)), maxRange))
    elif minRange is not None and maxRange is None:
        errorDf = object.filter(opel(length(col(columnName)), minRange))
    else: 
        errorDf = object.filter(opel(length(col(columnName)), minRange) | opeg(length(col(columnName)), maxRange))       

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return (registerAmount,Rules.LengthRule.code,Rules.LengthRule.name,Rules.LengthRule.property,Rules.LengthRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount), errorDf


def validateDataType(object:DataFrame,
    columnName:StringType,
    registerAmount,
    entity,
    threshold,
    data_Type:StringType):

    dataRequirement =  f"El atributo {entity}.{columnName}, debe ser de tipo {data_Type}"

    if str(object.schema[columnName].dataType) == data_Type:
        ratio = 0.0
        errorCount = 0
        
    else:
        ratio = 100.0
        errorCount = object.count()

    return (registerAmount, Rules.DataTypeRule.code,Rules.DataTypeRule.name,Rules.DataTypeRule.property,Rules.DataTypeRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount)

def validateFormatNumeric(object:DataFrame,
    columnName:StringType,
    registerAmount:IntegerType,
    entity,
    threshold,
    maxInt=True,
    sep:StringType='.',
    numDec=True):

    dataRequirement =  f"El atributo {entity}.{columnName}, debe ser tener el siguiente formato numerico {maxInt} {sep} {numDec}"

    if(str(object.schema[columnName].dataType)!='StringType'):
        object=object.withColumn(columnName,col(columnName).cast('string'))

    if(sep == '.'):
        sep = "\\"+sep

    errorDf = object.filter(regexp_replace(col(columnName),sep, "")==col(columnName))

    if(maxInt!=True):
        errorDf=errorDf.union(object.filter(length(split(col(columnName),sep).getItem(0))>maxInt))

    if(numDec!=True):
        errorDf=errorDf.union(object.filter(length(split(col(columnName),sep).getItem(1))!=numDec))

    errorCount = errorDf.count()
    ratio = (One - errorCount/registerAmount) * OneHundred

    return (registerAmount, Rules.NumericFormatRule.code,Rules.NumericFormatRule.name,Rules.NumericFormatRule.property,Rules.NumericFormatRule.code + "/" + entity + "/" + columnName,threshold,dataRequirement, columnName, ratio, errorCount), errorDf
