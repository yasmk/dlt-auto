# fields that won't change often for differnt tables can go here
#----------------------------------------
append_only_template = {
  "type": "append_only",
  "target_schema": "LIVE",
  "source_schema": "LIVE",
  "expect_all_criteria" : {},
  "expect_all_or_drop": {},
  "expect_all_or_fail": {}

}

upsert_template = {
  "type": "apply_changes",
  "target_schema": "LIVE",
  "source_schema": "LIVE",
  "expect_all_criteria" : {},
  "expect_all_or_drop": {},
  "expect_all_or_fail": {}
}

#expectations for mvs is not available on serverless yet 
sql_template = {
  "type": "sql_table",
  "sql_query":"",
  "args": {},
  "target_schema": "LIVE",
  "source_schema": "LIVE",
  "expect_all_criteria" : {},
  "expect_all_or_drop": {},
  "expect_all_or_fail": {}
}
#----------------------------------------
