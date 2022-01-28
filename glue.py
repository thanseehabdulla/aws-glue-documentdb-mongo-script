import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "alm-s3", table_name = "read", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "alm-s3", table_name = "read", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("date", "string", "date", "string"), ("employee code", "long", "employee code", "long"), ("employee name", "string", "employee name", "string"), ("company", "string", "company", "string"), ("department", "string", "department", "string"), ("category", "string", "category", "string"), ("degination", "string", "degination", "string"), ("grade", "string", "grade", "string"), ("team", "string", "team", "string"), ("shift", "string", "shift", "string"), ("in time", "string", "in time", "string"), ("out time", "string", "out time", "string"), ("duration", "string", "duration", "string"), ("late by", "string", "late by", "string"), ("early by", "string", "early by", "string"), ("status", "string", "status", "string"), ("punch records", "string", "punch records", "string"), ("overtime", "string", "overtime", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("date", "string", "date", "string"), ("employee code", "long", "employee code", "long"), ("employee name", "string", "employee name", "string"), ("company", "string", "company", "string"), ("department", "string", "department", "string"), ("category", "string", "category", "string"), ("degination", "string", "degination", "string"), ("grade", "string", "grade", "string"), ("team", "string", "team", "string"), ("shift", "string", "shift", "string"), ("in time", "string", "in time", "string"), ("out time", "string", "out time", "string"), ("duration", "string", "duration", "string"), ("late by", "string", "late by", "string"), ("early by", "string", "early by", "string"), ("status", "string", "status", "string"), ("punch records", "string", "punch records", "string"), ("overtime", "string", "overtime", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["date", "employee code", "employee name", "company", "department", "category", "degination", "grade", "team", "shift", "in time", "out time", "duration", "late by", "early by", "status", "punch records", "overtime"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["date", "employee code", "employee name", "company", "department", "category", "degination", "grade", "team", "shift", "in time", "out time", "duration", "late by", "early by", "status", "punch records", "overtime"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "documentdb", table_name = "read", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "documentdb", table_name = "read", transformation_ctx = "resolvechoice3")
## @type: DataSink
## @args: [database = "documentdb", table_name = "read", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
documentdb_write_uri = "mongodb://alm-docdb-2022-01-07-06-30-02.cluster-czisz1g7cnum.ap-south-1.docdb.amazonaws.com:27017"
write_uri = "mongodb+srv://cluster0.agp1e.mongodb.net"

write_mongo_options = {
    "uri": write_uri,
    "database": "organization",
    "collection": "attendance",
    "username": "abdulla",
    "password": "abdulla"
}

write_documentdb_options = {
    "uri": documentdb_write_uri,
    "database": "organization",
    "collection": "attendance",
    "username": "root",
    "password": "admin1234"
}

glueContext.write_dynamic_frame.from_options(resolvechoice3, connection_type="mongodb",
                                             connection_options=write_mongo_options)

#glueContext.write_dynamic_frame.from_options(resolvechoice3, connection_type="documentdb",
#                                             connection_options=write_documentdb_options)

job.commit()
