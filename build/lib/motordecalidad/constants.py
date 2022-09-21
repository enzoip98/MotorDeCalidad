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
class AzureKeys:
    class Landing:
        Account = "fs.azure.account.key.adlseuedltb2b001.dfs.core.windows.net"
        Key = "JBBJSKUjq3q/EhpuIsBFGxCpI5M5/wvs4romw6eZBrH772HucQ2y9UKlfXIWpfYikjMn0wjE+Iki+ASt5AwC1Q=="
    class Solution:
        Account = "fs.azure.account.key.adlseuedltb2b002.dfs.core.windows.net"
        Key = "gLVky28N5XlvBnJEVZnLLxY1ZKCuUIzPta4kC04CDP2uek1UxLd0YQ11UDB1dNUswQgzA1Gjq6Z8+AStiSd7SQ=="

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
OutputDataFrameColumns = [RuleCodeColumn,TestedFieldsColumn,SucessRateColumn,FailedRegistersAmountColumn]
