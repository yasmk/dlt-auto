# dlt-auto

- pass spark to functions
- can define a template to encapsulate the commmon fields. each child can override the fields as needed. this helps with defining the common fields ony once
- type field is a must
- Can combine this with DABs to define environment dependent configs. These will be part of pipeline settings: e.g., evn: dev, source_databse: test_db, ...
- There are some predefined templates and types (append only, upsert, single sql, ...). Users can define their own functions in python which needs to be wrapped using ??? decorator. this will create a dlt table or views 
