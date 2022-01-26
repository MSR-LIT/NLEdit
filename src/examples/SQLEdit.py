from os import path

import sqlutils
from sqlutils.Schema_Utils import Schema_Utils
from sqlutils.SQLParse import SQLParse
from sqlutils.Val_Inference import Val_Inference
from sqlutils.SQL2NL import SQL2NL

SPIDER_ROOT = '/home/ahmed/fuse_connections/nledit/spider'
tables_file = path.join(SPIDER_ROOT, 'tables.json')
db_dir = path.join(SPIDER_ROOT, 'database')

#Example
db_name = 'wta_1'
initial_sql = 'SELECT first_name, last_name FROM players ORDER BY birth_date ASC' 
gold_sql = 'SELECT first_name, last_name FROM players WHERE hand = "left" ORDER BY birth_date ASC' 
schema_utils = Schema_Utils(tables_file, db_dir)
sqlutils.SQLParse.schema = schema_utils
sqlutils.SQLDiffBertwithSub.schema = schema_utils

db_path = path.join(db_dir, db_name, db_name + ".sqlite")
try:
    initial_sql = SQLParse.from_str(initial_sql, db_name, db_path)
    gold_sql = SQLParse.from_str(gold_sql, db_name, db_path)

    diff = initial_sql.diff(gold_sql)
    print('SQLEdit:', diff.str_tokens())
except:
    assert False, 'error parsing sql'
