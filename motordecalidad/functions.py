from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,when,lit
from pyspark.sql.types import StringType, IntegerType

##Constants
NullRuleCode = "101"
DuplicatedRuleCode = "102"

def validateNull(object:DataFrame,field: StringType,registersAmount: IntegerType): 
    nullCount = object.select(field).filter(col(field).isNull()).count()
    ratio = nullCount/ registersAmount
    return [NullRuleCode,ratio,nullCount]

def validateDuplicates(object:DataFrame,fields:List,registersAmount: IntegerType):
    uniqueRegistersAmount = object.select(fields).dropDuplicates().count()
    nonUniqueRegistersAmount = registersAmount - uniqueRegistersAmount
    ratio = nonUniqueRegistersAmount / registersAmount
    return [DuplicatedRuleCode,ratio,nonUniqueRegistersAmount]
