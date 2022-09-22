##Constants

class RuleCodes:
    NullRuleCode = "101"
    DuplicatedRuleCode = "102"
    IntegrityRuleCode = "103"
    CheckStringRuleCode = "104"
    CheckBoolRuleCode = "105"
    CheckComillasDoblesRuleCode = "106"
class JsonParts:
    ZoneSection = "ZONE"
    InputSection = "INPUT"
    RulesSection = "RULES"
    HeaderSection = "HEADER"
    DelimiterSection = "DELIMITER"
    FieldsSection = "FIELDS"
    KeyFieldSection = "KEY_FIELDS"
    CountrySection = "COUNTRY_ID"
    EntitySection = "ENTITY_ID"
    ProjectSection = "PROJECT"
    CutOffDateSection = "CUTOFF_DATE"
    DomainSection = "DOMAIN"
    SubDomainSection = "SUB_DOMAIN"
class Zone:
    Landing = "LANDING"
    Solution = "SOLUTION"
    DQ = "DQ"
    ObservedData = "OBSERVED_DATA"

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
