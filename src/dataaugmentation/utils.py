from sqlutils import SQL2NL
from sqlutils.SQLParse import SQLParse, ValUnit, ColUnit,CondUnit
from sqlutils.process_sql import get_schema, Schema, get_sql
from sqlutils.Val_Inference import Val_Inference
from sqlutils.Schema_Utils import Schema_Utils

from numpy.random import choice, rand

USE_NATURAL_NAMES = True
schema_utils = None

def column_name(col_id, db, fullname=False):    
    if fullname:
        if USE_NATURAL_NAMES:
            return schema_utils.get_natural_column_fullname(db, col_id)        
        name =  schema_utils.get_column_fullname(db, col_id)
        return name.replace('.',"'s ")
    else:
        return schema_utils.get_natural_column_name(db, col_id) if USE_NATURAL_NAMES else schema_utils.get_column_name(db, col_id)

def table_name(tab_id, db):
    return  schema_utils.get_natural_table_name(db, tab_id) if USE_NATURAL_NAMES else schema_utils.get_table_name(db, tab_id)

def select_column_names(select, fullname=False):
    cols = select.list_cols()
    return ', '.join([column_name(col, select.db, fullname=fullname) for col in cols])

def order_column_names(orderby, fullname=False):
    cols = orderby.get_cols()
    return ', '.join([column_name(col, orderby.db, fullname=fullname) for col in cols])

def valunit_from_colid(col_id, db):
    return ValUnit([0,[0,col_id,False], None], db=db)

def remove_index_from_list(lst, ix):
    return lst[:ix]+lst[ix+1:]

def sample_column_from_current_tables(sql, used_tables=False, exclude=[]):
    if used_tables:
        used_cols = sql.list_columns(skipjoin=True, skipsub=True)
        if list(used_cols) == [0]:
            tables = sql.from_.list_tables()
        else:
            tables = set([schema_utils.get_table_of_col(sql.db, c) for c in used_cols if c!=0])
    else:
        tables = sql.from_.list_tables()
    
    if len(tables) == 0: raise ValueError('Empty Tables')
    all_cols = []
    for tab in tables: all_cols.extend(schema_utils.get_columns_of_table(sql.db, tab))
    valid_cols = set(all_cols).difference(set(exclude))
    if len(valid_cols) == 0: return None
    return int(choice(list(valid_cols)))

def explain_colunit(colunit, agg=None):
    if agg is None: agg=0
    agg = max(agg, colunit.agg_id)
    output = SQL2NL.AGG_OPS[agg]
    if colunit.distinct:
        output += ' unique'
    output += ' {}'.format(column_name(colunit.col_id, colunit.db))
    return output.strip()     

def explain_valunit(valunit, agg=None):
    output = explain_colunit(valunit.unit1, agg=agg)
    if valunit.op != 0:
        output += ' {} {}'.format(SQL2NL.UNIT_OPS[valunit.op], valunit.unit2)
    return output

def get_joinonly_tables(sql):
    if all([col == 0 for col in sql.list_columns(skipjoin=True, skipsub=True)]): return []

    all_tabs = set([tbl.table for tbl in sql.from_.tables])
    used_cols = sql.list_columns(skipjoin=True, skipsub=True)
    used_tab = set([schema_utils.get_table_of_col(sql.db, c) for c in used_cols if c!=0])
    
    return list(all_tabs.difference(used_tab))


def sample_table(db, exclude=[]):
    valid_tables = set(range(len(schema_utils.get_tables(db)))).difference(set(exclude))
    return int(choice(list(valid_tables)))

def remove_table(sql, tab_id):
    #only remove from list of tables and remove join condition.
    db = sql.db
    updated_tabs = [tbl for tbl in sql.from_.tables if tbl.table != tab_id]
    if len(updated_tabs) == 0: raise ValueError('Empty Tables!')
    sql.from_.tables = updated_tabs
    #updating join conds
    def _keep_joincond(joincond):
        col1 = joincond.valunit.unit1.col_id
        col2 = joincond.val1.val
        tab1 = schema_utils.get_table_of_col(db, col1)
        tab2 = schema_utils.get_table_of_col(db, col2)
        return tab1 != tab_id and tab2 != tab_id
        
    sql.from_.cond.conds = [cond for cond in sql.from_.cond.conds if _keep_joincond(cond)]
    sql.from_.cond.andor = [] if len (sql.from_.cond.conds) == 0 else sql.from_.cond.andor[:len(sql.from_.cond.conds)-1]

def update_tables_removedcol(sql, removed_col_id):
    tab = schema_utils.get_table_of_col(sql.db, removed_col_id)
    used_cols = sql.list_columns(skipjoin=True, skipsub=True)
    if list(used_cols) == [0]: return
    for c in used_cols:
        if c != 0 and tab == schema_utils.get_table_of_col(sql.db, c):
                return #still used
    remove_table(sql, tab)


def sample_agg(exclude=[]):
    valid_aggs = set([1,2,3,4,5]).difference(exclude)
    return int(choice(list(valid_aggs)))

def clean_value(val):
    if isinstance(val, SQLParse): raise ValueError('Invalid value to explain {}'.format(val))
    try:
        float(val)
        return val
    except ValueError:
        pass
    if len(val) > 0 and val[0] in ['"',"'"]:
        val = val[1:]
    if len(val) > 0 and val[-1] in ['"',"'"]:
        val = val[:-1]
    if len(val) > 0 and val[0] == '%':
        val = val[1:]
    if len(val) > 0 and val[-1] == '%':
        val = val[:-1]
    return val

def explain_condition(condunit):
    col = column_name(condunit.valunit.unit1.col_id, condunit.db)
    op = SQL2NL.WHERE_OPS[condunit.op_id]
    if condunit.not_: op = f'not {op}'
    output =  '{} {} {}'.format(col, op, clean_value(condunit.val1.val))
    if condunit.op_id == 1: #between
        output += ' and {}'.format(clean_value(condunit.val2.val))
    return output

def get_condunit(col_id, op_id, db, agg_id=None):
    if agg_id is None: agg_id = 0
    if op_id == 1:
        return CondUnit([False, op_id, [0,[agg_id,col_id,False], None],'1','2'],db=db)
    elif op_id == 9:
        return CondUnit([False, op_id, [0,[agg_id,col_id,False], None],'%1%',None],db=db)
    else:
        return CondUnit([False, op_id, [0,[agg_id,col_id,False], None],'1',None],db=db)

def limit_in_subquery(sql):
    subquery = sql.find_subquery()
    if subquery is None: return False
    return subquery[0].limit is not None

def order_in_subquery(sql):
    subquery = sql.find_subquery()
    if subquery is None: return False
    return not subquery[0].orderby.empty

def distinct_in_subquery(sql):
    subquery = sql.find_subquery()
    if subquery is None: return False
    return subquery[0].select.distinct

def col_used_outside_select(sql, col_id):
    subquery = sql.find_subquery()
    if subquery is not None and col_id in subquery[0].list_columns(): return True
    other_cols = set()
    sql.where._list_columns(other_cols)
    sql.groupby._list_columns(other_cols)
    sql.orderby._list_columns(other_cols)
    sql.having._list_columns(other_cols)
    if col_id in other_cols: return True
    return False

def col_used_outside_groupby(sql, col_id):
    subquery = sql.find_subquery()
    if subquery is not None and col_id in subquery[0].list_columns(): return True
    other_cols = set()
    sql.where._list_columns(other_cols)
    sql.select._list_columns(other_cols)
    sql.orderby._list_columns(other_cols)
    sql.having._list_columns(other_cols)
    if col_id in other_cols: return True
    return False

def col_used_outside_condition(sql, col_id, type_='where'):
    subquery = sql.find_subquery()
    if subquery is not None and col_id in subquery[0].list_columns(): return True
    other_cols = set()    
    sql.select._list_columns(other_cols)
    sql.orderby._list_columns(other_cols)
    sql.groupby._list_columns(other_cols)    
    if type_ == 'where':
        sql.having._list_columns(other_cols)
    elif type_ == 'having':
        sql.where._list_columns(other_cols)
    else:
        assert False
    
    if col_id in other_cols: return True
    return False    

def nested_in_cond(condunit):
    return condunit.val1.type == 'sql'

def weights_to_probs(weights):
    sum_ = sum(weights)
    return [w/sum_ for w in weights]