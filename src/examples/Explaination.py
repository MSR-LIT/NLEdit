from os import path

import sqlutils
from sqlutils.Schema_Utils import Schema_Utils
from sqlutils.SQLParse import SQLParse
from sqlutils.Val_Inference import Val_Inference
from sqlutils.SQL2NL import SQL2NL

SPIDER_ROOT = '/home/ahmed/fuse_connections/nledit/spider'
tables_file = path.join(SPIDER_ROOT, 'tables.json')
db_dir = path.join(SPIDER_ROOT, 'database')

#init
schema_utils = Schema_Utils(tables_file, db_dir)
sqlutils.SQLParse.schema = schema_utils
sql2nl = SQL2NL(schema_utils)
val_inference =  Val_Inference(tables_file, db_dir)

#Example
db_name = 'movie_1'
gold_query = 'SELECT rID FROM Rating WHERE stars != 4' 
predicted_query = 'SELECT T1.rID FROM Reviewer AS T1 EXCEPT SELECT T2.rID FROM Rating AS T2 WHERE T2.stars = "terminal"'

#Explain
db_path = path.join(db_dir, db_name, db_name + ".sqlite")
try:
    gold_parsed = SQLParse.from_str(gold_query, db_name, db_path)
    predicted_parsed = SQLParse.from_str(predicted_query, db_name, db_path)
except:
    assert False, 'error parsing sql'

if sql2nl.is_supported(predicted_parsed):
    val_inference.infer(predicted_parsed, gold_parsed.list_cols_vals()[1])
else:
    assert False, 'explaination not supported for the given sql'

query_nl = sql2nl.get_nl(predicted_parsed)
for i, step in enumerate(query_nl):
	print('step {}: {}'.format(i+1,step))
