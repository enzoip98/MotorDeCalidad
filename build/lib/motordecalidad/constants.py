##Constants

class RuleCodes:
    NullRuleCode = "101"
    DuplicatedRuleCode = "102"
    IntegrityRuleCode = "103"
    CheckStringRuleCode = "104"
    CheckBoolRuleCode = "105"
    CheckComillasDoblesRuleCode = "106"
class JsonParts:
    Input = "INPUT"
    Output = "OUTPUT"
    Rules = "RULES"
    Header= "HEADER"
    Delimiter = "DELIMITER"
    Fields = "FIELDS"
    Country = "COUNTRY_ID"
    Entity = "ENTITY_ID"
    Project = "PROJECT"
    Path = "PATH"
    Account = "ACCOUNT"
    Key = "KEY"


LeftAntiType = "leftanti"
One = 1
CountryColumn = "country"
DateColumn = "date"
EntityColumn = "entity"
ProjectColumn = "project"
AuditDateColumn = "audit_date"
TestedFieldsColumn = "tested_fields"
RuleCodeColumn = "rule_code"
SucessRateColumn = "sucess_rate"
TestedRegisterAmountColumn = "tested_registers_amount"
FailedRegistersAmountColumn = "failed_registers_amount"
ZoneColumn = "zone"
OutputDataFrameColumns = [RuleCodeColumn,TestedFieldsColumn,SucessRateColumn,FailedRegistersAmountColumn]
