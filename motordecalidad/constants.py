##Constants

class RuleCodes:
    NullRuleCode = "101"
    DuplicatedRuleCode = "102"
    IntegrityRuleCode = "103"
    FormateDateCode = "104"

    CheckStringRuleCode = "106"
    CheckBoolRuleCode = "107"
    CheckComillasDoblesRuleCode = "108"
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
    FormatDate = "FORMAT_DATE"


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
PermitedFormatDate = ['yyyy-MM-dd','yyyy/MM/dd', 'yyyyMMdd', 'yyyyMM']
