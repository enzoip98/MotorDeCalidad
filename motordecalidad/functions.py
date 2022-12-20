import json
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType
from motordecalidad.constants import *
import datetime
import time
from motordecalidad.rules import *


print("Motor de Calidad Version Release 1.3")

# Main function, Invokes all the parameters from the json, Optionally filters, starts the rule validation
# Writes and returns the summary of the validation 
def startValidation(inputspark,config,dfltPath=""):
    global spark
    global dbutils
    global DefaultPath
    DefaultPath = dfltPath
    spark = inputspark
    dbutils = get_dbutils(spark)
    print("Inicio de validacion")
    object,output,country,project,entity,domain,subDomain,segment,area,rules,error,filtered = extractParamsFromJson(config)
    filteredObject = applyFilter(object,filtered)
    registerAmount = filteredObject.count()
    validationData = validateRules(filteredObject,rules,registerAmount,entity,project,country,domain,subDomain,segment,area,error)
    writeDf(validationData, output)
    return validationData

#Function to define the dbutils library from Azure Databricks
def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

# Function that extracts the information from de JSON File
# @config Variable that contains the JSON route
def extractParamsFromJson(config):
    file = open(config)
    data = json.load(file)
    input = data.get(JsonParts.Input)
    output = data.get(JsonParts.Output)
    country:str = input.get(JsonParts.Country)
    project:str = input.get(JsonParts.Project)
    entity:str = input.get(JsonParts.Entity)
    domain: str = input.get(JsonParts.Domain)
    subDomain: str = input.get(JsonParts.SubDomain)
    segment: str = input.get(JsonParts.Segment)
    area: str = input.get(JsonParts.Area)
    error = data.get(JsonParts.Error)
    filtered = data.get(JsonParts.Filter)

    entityDf = readDf(input)
    rules = data.get(JsonParts.Rules)
    print("Extraccion de JSON completada")
    return entityDf,output,country,project,entity,domain,subDomain,segment,area,rules,error,filtered

# Function that reads the input File
def readDf(input):
    print("inicio de lectura de informacion")
    type = input.get(JsonParts.Type)
    if type == "prod_csv":
        spark.conf.set("fs.azure.account.key.{account}.dfs.core.windows.net".format(account = dbutils.secrets.get(scope=input.get(JsonParts.Scope),key = input.get(JsonParts.Account))),dbutils.secrets.get(scope = input.get(JsonParts.Scope), key = input.get(JsonParts.Key)))
        header = input.get(JsonParts.Header)
        return spark.read.option("delimiter",input.get(JsonParts.Delimiter)).option("header",header).csv(str(input.get(JsonParts.Path)).format( account = dbutils.secrets.get(scope = input.get(JsonParts.Scope), key = input.get(JsonParts.Account)),
        country = dbutils.widgets.get('country'),
        year = dbutils.widgets.get('year'),
        month = dbutils.widgets.get('month'),
        day = dbutils.widgets.get('day')))
    elif type == "prod_parquet":
        spark.conf.set("fs.azure.account.key.{account}.dfs.core.windows.net".format( account = dbutils.secrets.get(scope=input.get(JsonParts.Scope),key = input.get(JsonParts.Account))),dbutils.secrets.get(scope = input.get(JsonParts.Scope), key = input.get(JsonParts.Key)))
        return spark.read.parquet(str(input.get(JsonParts.Path)).format(account = dbutils.secrets.get(scope = input.get(JsonParts.Scope), key = input.get(JsonParts.Account)),
        country = dbutils.widgets.get('country'),
        year = dbutils.widgets.get('year'),
        month = dbutils.widgets.get('month'),
        day = dbutils.widgets.get('day')))
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
        .load()
    else:
        spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key))
        header = input.get(JsonParts.Header)
        if DefaultPath == "" : 
            return spark.read.option("delimiter",input.get(JsonParts.Delimiter)).option("header",header).csv(input.get(JsonParts.Path))
        else:
            return spark.read.option("delimiter",input.get(JsonParts.Delimiter)).option("header",header).csv(DefaultPath)

# Function that writes the output dataframe with the overwrite method
def writeDf(object:DataFrame,output):
    type = output.get(JsonParts.Type)
    if type == "prod_csv":
        spark.conf.set("fs.azure.account.key.{account}.blob.core.windows.net".format(account = dbutils.secrets.get(scope = output.get(JsonParts.Scope), key = output.get(JsonParts.Account))),str(dbutils.secrets.get(scope = output.get(JsonParts.Scope), key =output.get(JsonParts.Key))))
        header:bool = output.get(JsonParts.Header)
        object.coalesce(One).write.mode("overwrite").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path).format( 
            account = dbutils.secrets.get(scope = output.get(JsonParts.Scope), key = output.get(JsonParts.Account)),
            country = dbutils.widgets.get('country'),
            year = dbutils.widgets.get('year'),
            month = dbutils.widgets.get('month'))))
    else:
        spark.conf.set(output.get(JsonParts.Account),output.get(JsonParts.Key))
        header:bool = output.get(JsonParts.Header)
        partitions:List = output.get(JsonParts.Partitions)
        try:
            if len(partitions) > Zero :
                object.coalesce(One).write.partitionBy(*partitions).mode("overwrite").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path)))
            else:
                object.coalesce(One).write.mode("overwrite").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path)))
        except:
            object.coalesce(One).write.mode("overwrite").option("delimiter",str(output.get(JsonParts.Delimiter))).option("header",header).format("com.databricks.spark.csv").save(str(output.get(JsonParts.Path)))
    print("Se escribio en el blob")

def applyFilter(object:DataFrame, filtered) :
    try:
        filteredColumn = filtered.get(JsonParts.Fields)
        filterValue = filtered.get(JsonParts.Values)
        print("Extracción de parametros de filtrado finalizada")
        return object.filter(col(filteredColumn)==filterValue)
    except:
        print("Se omite filtro")
        return object

def createErrorData(object:DataFrame) :
    columnsList = object.columns
    columnsList.extend(["error","run_time"])
    schema = StructType(
        list(map(lambda x: StructField(x,StringType()),columnsList))
        )
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
#Function that validate rules going through the defined options
def validateRules(object:DataFrame,rules:dict,registerAmount:int, entity: str, project:str,country: str,domain: str,subDomain: str,segment: str,area: str,error):
    runTime = datetime.datetime.now()
    errorData = createErrorData(object)
    rulesData:List = []
    for code in rules:
        if rules[code].get(JsonParts.Fields) not in [0,["0"],"0"] :
            if code[0:3] == Rules.Pre_Requisites.code:
                print("Inicializando regla de existencia")
                columns = rules[code].get(JsonParts.Fields)
                t = time.time()
                validateRequisites(object,columns)
                print("regla de requisitos: %s segundos" % (time.time() - t))
            if code[0:3] == Rules.NullRule.code:
                print("Inicializando reglas de Nulos")
                data:List = []
                columns = rules[code].get(JsonParts.Fields)
                threshold:int = rules[code].get(JsonParts.Threshold)
                write = rules[code].get(JsonParts.Write)
                if columns[0] == "*" :
                    for field in object.columns:
                        t = time.time()
                        data, errorDf = validateNull(object,field,registerAmount,entity,threshold)
                        errorDesc = "Nulos - " + str(field)
                        if data[-One] > Zero :
                            errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                            .withColumn("run_time", lit(runTime))
                            if write != False :
                                errorData = errorData.union(errorTotal)
                        rulesData.append(data)
                        print("regla de nulos: %s segundos" % (time.time() - t))
                else:
                    for field in columns:
                        t = time.time()
                        data, errorDf = validateNull(object,field,registerAmount,entity,threshold)
                        errorDesc = "Nulos - " + str(field)
                        if data[-One] > Zero :
                            errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                            .withColumn("run_time", lit(runTime))
                            if write != False :
                                errorData = errorData.union(errorTotal)
                        rulesData.append(data)
                        print("regla de nulos: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.DuplicatedRule.code:
                print("Inicializando reglas de Duplicidad")
                t = time.time()
                testColumn = rules[code].get(JsonParts.Fields)
                threshold:int = rules[code].get(JsonParts.Threshold)
                write = rules[code].get(JsonParts.Write)
                data, errorDf = validateDuplicates(object,testColumn,registerAmount,entity,threshold)
                errorDesc = "Duplicidad - " + str(testColumn)
                if data[-One] > 0 :
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time", lit(runTime))
                    if write != False :
                        errorData = errorData.union(errorTotal)
                rulesData.append(data)
                print("regla de duplicados: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.IntegrityRule.code:
                print("Inicializando reglas de Integridad referencial")
                t = time.time()
                referalData = rules[code].get(JsonParts.Input)
                referenceDataFrame = readDf(referalData)
                testColumn = rules[code].get(JsonParts.Fields)
                referenceColumn = referalData.get(JsonParts.Fields)
                referenceEntity = referalData.get(JsonParts.Entity)
                threshold:int = rules[code].get(JsonParts.Threshold)
                write = rules[code].get(JsonParts.Write)
                data, errorDf = validateReferentialIntegrity(object,referenceDataFrame, testColumn, referenceColumn,registerAmount,entity,referenceEntity,threshold)
                errorDesc = "Integridad referencial - " + str(testColumn) + " - "\
                + str(referenceColumn) + " - " + str(referalData)

                if data[-One] > Zero :
                    errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                    .withColumn("run_time", lit(runTime))
                    if write != False :
                        errorData = errorData.union(errorTotal)
                rulesData.append(data) 
                print("regla de IR: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.FormatDate.code:
                print("Inicializando regla de formato")
                columnName = rules[code].get(JsonParts.Fields)
                formatDate = rules[code].get(JsonParts.FormatDate)
                threshold:int = rules[code].get(JsonParts.Threshold)
                write = rules[code].get(JsonParts.Write)
                for field in columnName:
                    t = time.time()
                    if formatDate in PermitedFormatDate:
                        data, errorDf = validateFormatDate(object, formatDate, field,entity,threshold,spark)
                        errorDesc = "Formato - " + str(field)
                        if data[-One] > Zero :
                            errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                            .withColumn("run_time", lit(runTime))
                            if write != False :
                                errorData = errorData.union(errorTotal)
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
                threshold:int = rules[code].get(JsonParts.Threshold)
                write = rules[code].get(JsonParts.Write)
                for field in columnName :
                    t = time.time()
                    data, errorDf = validateCatalog(object,field,listValues,registerAmount,entity,threshold)
                    errorDesc = "Catalogo - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de catalogo: %s segundos" % (time.time() - t))
            
            elif code[0:3] == Rules.RangeRule.code:
                print("Inicializando regla de rango")
                columnName = rules[code].get(JsonParts.Fields)
                threshold:int = rules[code].get(JsonParts.Threshold)
                minRange = rules[code].get(JsonParts.MinRange)
                maxRange = rules[code].get(JsonParts.MaxRange)
                write = rules[code].get(JsonParts.Write)

                for field in columnName :
                    t = time.time()
                    data, errorDf = validateRange(object,field,registerAmount,entity,threshold,minRange,maxRange)
                    errorDesc = "Rango - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de rango: %s segundos" % (time.time() - t))
            
            elif code[0:3] == Rules.ForbiddenRule.code:
                print("Inicializando regla de caracteres prohibidos")
                columnName = rules[code].get(JsonParts.Fields)
                threshold = rules[code].get(JsonParts.Threshold)
                listValues = rules[code].get(JsonParts.Values)
                write = rules[code].get(JsonParts.Write)

                for field in columnName :
                    t = time.time()
                    data, errorDf = validateForbiddenCharacters(object,field,listValues,registerAmount,entity,threshold)
                    errorDesc = "Caracteres prohibidos - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de caracteres prohibidos: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.Type.code:
                print("Inicializando regla de tipo de dato")
                columnName = rules[code].get(JsonParts.Fields)
                threshold = rules[code].get(JsonParts.Threshold)
                data_Type = rules[code].get(JsonParts.DataType) 
                write = rules[code].get(JsonParts.Write)

                for field in columnName :
                    t = time.time()
                    data, errorDf = validateType(object,data_Type,field,registerAmount,entity,threshold)
                    errorDesc = "Tipo de dato error - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de caracteres tipo de dato: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.Composision.code:
                print("Inicializando regla de composicion")
                columnName = rules[code].get(JsonParts.Fields)
                threshold = rules[code].get(JsonParts.Threshold)
                patialColumns = rules[code].get(JsonParts.Values)
                write = rules[code].get(JsonParts.Write)
                
                for field in columnName:
                    t = time.time()
                    data, errorDf = validateComposision(object,field,patialColumns,registerAmount,entity,threshold)
                    errorDesc = "Composicion error - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de caracteres composicion: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.LengthRule.code:
                print("Inicializando regla de longitud")
                columnName = rules[code].get(JsonParts.Fields)
                threshold = rules[code].get(JsonParts.Threshold)
                minRange = rules[code].get(JsonParts.MinRange)
                maxRange = rules[code].get(JsonParts.MaxRange)
                write = rules[code].get(JsonParts.Write)

                for field in columnName :
                    t = time.time()
                    data, errorDf = validateLength(object,field,registerAmount,entity,threshold,minRange,maxRange)
                    errorDesc = "Longitud - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de longitud: %s segundos" % (time.time() - t))
            
            elif code[0:3] == Rules.DataTypeRule.code:
                print("Inicializando regla de tipo de dato parquet")
                columnName = rules[code].get(JsonParts.Fields)
                threshold = rules[code].get(JsonParts.Threshold)
                data_Type = rules[code].get(JsonParts.DataType)            
                write = rules[code].get(JsonParts.Write)

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
                write = rules[code].get(JsonParts.Write)

                for field in columnName :
                    t = time.time()
                    data, errorDf = validateFormatNumeric(object,field,registerAmount,entity,threshold,maxInt,sep,numDec)
                        
                    errorDesc = "Formato Numerico - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de formato numerico: %s segundos" % (time.time() - t))

            elif code[0:3] == Rules.OperationRule.code:
                print("Inicializando regla de tipo de operacion numerica")
                columnName = rules[code].get(JsonParts.Fields)
                threshold = rules[code].get(JsonParts.Threshold)   
                operator = rules[code].get(JsonParts.Operator)
                input_val = rules[code].get(JsonParts.Input_val)
                error_val = rules[code].get(JsonParts.Error_val)  
                write = rules[code].get(JsonParts.Write)

                for field in columnName :
                    t = time.time()
                    data, errorDf = validateOperation(object,field,registerAmount,entity,threshold,operator,input_val,error_val)
                        
                    errorDesc = "Operacion Numerica - " + field
                    if data[-One] > Zero:
                        errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                        .withColumn("run_time",lit(runTime))
                        if write != False :
                            errorData = errorData.union(errorTotal)
                    rulesData.append(data)
                    print("regla de operacion numerica: %s segundos" % (time.time() - t))
            elif code[0:3] == Rules.StatisticsResult.code:
                column = rules[code].get(JsonParts.Fields)
                if column[0] == "*" :
                    res = measuresCentralTendency(object, object.columns,spark)
                else:
                    res = measuresCentralTendency(object, column,spark)
                writeDf(res,rules[code].get(JsonParts.Output))
        else:
            pass
    if errorData.count() > Zero:
        writeDf(errorData,error)
    validationData:DataFrame = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)
    return validationData.select(
        Country.value(when(lit(country).isNull(),dbutils.widgets.get('country')).otherwise(country)),
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