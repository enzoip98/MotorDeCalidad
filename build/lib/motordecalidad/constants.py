##Constants

class Rules:
    class NullRule:
        name = "Nulo"
        code = "101"
    class DuplicatedRule:
        name = "Duplicado"
        code = "102"
    class IntegrityRule:
        name = "Integridad Referencial"
        code = "103"
    class FormatDate:
        name = "Formato de Fecha"
        code = "104"
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
RuleColumn = "rule"
SucessRateColumn = "sucess_rate"
TestedRegisterAmountColumn = "tested_registers_amount"
FailedRegistersAmountColumn = "failed_registers_amount"
ZoneColumn = "zone"
OutputDataFrameColumns = [RuleColumn,TestedFieldsColumn,SucessRateColumn,FailedRegistersAmountColumn]
PermitedFormatDate = ['yyyy-MM-dd','yyyy/MM/dd', 'yyyyMMdd', 'yyyyMM']
