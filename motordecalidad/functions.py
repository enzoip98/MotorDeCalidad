from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,when,lit
from pyspark.sql.types import StringType

##Constants
NullString = "_null"
DuplicatedString = "_duplicated"
One = 1
FalseValue = False
NullRuleCode = "101"
DuplicatedRuleCode = "102"

def validateNull(object:DataFrame,field: StringType): 
    nullColumnName = field + NullString
    nullDf = object.select(field).withColumn(nullColumnName,when(col(field).isNull(),One))
    nullCount = nullDf.filter(col(nullColumnName)==lit(FalseValue)).count()
    registersAmount = object.count()
    ratio = nullCount/ registersAmount
    return [NullRuleCode,ratio,nullCount]

def validateDuplicates(object:DataFrame,fields:List):
    uniqueRegistersAmount = object.select(fields).dropDuplicates().count()
    registersAmount = object.count()
    nonUniqueRegistersAmount = registersAmount - uniqueRegistersAmount
    ratio = nonUniqueRegistersAmount / registersAmount
    return [DuplicatedRuleCode,ratio,nonUniqueRegistersAmount]
