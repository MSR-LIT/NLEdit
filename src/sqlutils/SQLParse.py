from .process_sql import get_schema, Schema, get_sql
from .SQLDiffBertwithSub import SQLDiff

from .Schema_Utils import Schema_Utils
from .SQL2NL import (WHERE_OPS, AGG_OPS, UNIT_OPS)

schema = None #Set to an instance of Schema_Utils. TODO: Clean!

clean_name = lambda name: name[2:-2] \
            if name.startswith('__') and name.endswith('__') else name

def table_name2id(db, table_name):
    cleaned_name = clean_name(table_name)
    return schema.get_table_id_by_name(db, cleaned_name)
    
def column_name2id(db, column_name):
    cleaned_name = clean_name(column_name[2:-2])
    if cleaned_name == 'all': return 0
    table_name, col_name = cleaned_name.split('.')
    table_id = table_name2id(db, table_name)
    return schema.get_column_id_by_name(db, table_id, col_name)
    
def table_id2name(db, table_id):
    return '__{}__'.format(schema.get_table_name(db, table_id)).lower()

def column_id2name(db, col_id):
    colname = schema.get_column_fullname(db, col_id)
    if colname == 'rows': colname = 'all'
    return '__{}__'.format(colname).lower()
    
class Value:
    def __init__(self, val, db=None, parsed_sql=False):
        assert db is not None
        self.db = db
        self.type = None
        self.empty = False
        if val is None:
            self.empty = True
            return
        if parsed_sql:
            self.val = val
            self.type = 'sql'
            return        
        if isinstance(val, dict):
            self.type = 'sql'
            self.val = SQLParse(val, db=db)
        elif isinstance(val,str) or isinstance(val,float):
            self.type = 'litral'
            self.val = val
        elif isinstance(val, list) or isinstance(val, tuple):            
            assert len(val) == 3 and val[0] == 0 and not val[2]   
            self.type = 'joincol'
            self.val = val[1]
            if isinstance(self.val, str):
                self.val = column_name2id(db, self.val)
        else:
            assert False

    def to_spider_parse(self):
        if self.empty: return None
        if self.type == 'sql': return self.val.to_spider_parse()
        if self.type == 'litral': return self.val
        if self.type == 'joincol':
            return (0,column_id2name(self.db, self.val) ,False)
        assert False

    def __eq__(self, other):
        if other is None: return
        if self.empty  and other.empty: return True
        return self.type == other.type and self.val == other.val
    def __str__(self):
        if self.type == 'sql':
            return str(self.val)
        elif self.type == 'litral':
            return '_val_'
        else:
            return '_joincol_'

    def _list_cols_vals(self, cols, vals):
        if self.type == 'litral':
            vals.append(self.val)
        elif self.type == 'sql':
            self.val._list_cols_vals(cols, vals)

    def _list_tables(self, tables):
        if self.type == 'sql':
            self.val._list_tables(tables)

    def _list_columns(self, columns, skipsub=False):
        if not skipsub and self.type == 'sql':
            self.val._list_columns(columns)
        elif self.type == 'joincol':
            columns.add(self.val)

class ColUnit:
    def __init__(self, colunit, db):
        self.db = db
        self.agg_id, self.col_id, self.distinct = colunit
        if isinstance(self.col_id, str):
            self.col_id = column_name2id(db, self.col_id)
            assert isinstance(self.col_id, int)
    
    def to_spider_parse(self):
        return (self.agg_id, column_id2name(self.db,self.col_id), self.distinct)

    def __eq__(self, other):
        if other is None: return False
        return self.agg_id == other.agg_id and self.col_id == other.col_id and self.distinct == other.distinct

    def __str__(self):
        if self.agg_id == 0: #no aggr
            return '_col_'
        else:
            return '_agg_ _col_'

    def _list_columns(self, columns):
        columns.add(self.col_id)
    
    def print(self):
        output = str(self.col_id)
        if self.distinct:
            output = 'distinct '+ output
        if self.agg_id != 0:
            output = 'agg: {} '.format(self.agg_id)+output
        return output
    
    def toks(self):
        """
        NL here depends on SQL2NL. refactor to call methods in one file.
        """
        output = []
        if self.agg_id != 0: output.append(AGG_OPS[self.agg_id])
        if self.distinct: output.append('different')
        output.append('col:'+schema.get_column_fullname(self.db,self.col_id))
        return output

    @classmethod
    def from_toks(cls, toks, db):
        #column is given by id.
        distinct = False
        agg_id = 0
        assert len(toks) > 0, 'empty toks in col unit, {}'.format(' '.join(toks))
        assert toks[-1].startswith('col:')
        col_id = int(toks[-1][4:])
        if len(toks) > 1 and toks[-2] == 'different': distinct = True
        if len(toks) > 1:
            toks_ = ' '.join(toks)
            for i, agg in enumerate(AGG_OPS[1:]):
                if toks_.startswith(agg):
                    agg_id = i+1
                    break

        return ColUnit((agg_id, col_id, distinct), db)

class ValUnit:
    def __init__(self, valunit, db, parsed=False):
        self.op, unit1, unit2 = valunit
        if parsed:
            self.unit1 = unit1
            self.unit2 = unit2
        else:
            self.unit1 = ColUnit(unit1, db)
            self.unit2 = None if unit2 is None else ColUnit(unit2, db)

    def to_spider_parse(self):
        return (self.op, self.unit1.to_spider_parse(), (None if self.unit2 is None else self.unit2.to_spider_parse()))

    def __eq__(self, other):
        if other is None: return False
        return self.op == other.op and self.unit1 == other.unit1 and self.unit2 == other.unit2

    def __str__(self):
        if self.op == 0:
            if self.unit2:
                return '{}, {}'.format(str(self.unit1),str(self.unit2))
            else:
                return str(self.unit1)
        return '_op_ {} {}'.format(str(self.unit1),str(self.unit2))

    def _list_columns(self, columns):
        self.unit1._list_columns(columns)

        if self.unit2: self.unit2._list_columns(columns)

    def print(self):
        if self.op == 0:
            return self.unit1.print()
        else:
            return '{} {} {}'.format(self.unit1.print(), self.op, self.unit2.print())

    def toks(self):
        """
        NL here depends on SQL2NL. refactor to call methods in one file.
        """
        if self.op == 0:
            return self.unit1.toks()
        return self.unit1.toks() + [UNIT_OPS[self.op]] + self.unit2.toks()

    @classmethod
    def from_toks(cls, toks, db):
        toks_ = ' '.join(toks)
        unit_op = 0        
        for i, op in enumerate(UNIT_OPS[1:]):
            if op in toks_:
                unit_op = i+1
                break
        if unit_op == 0:
            unit1 = ColUnit.from_toks(toks, db)
            unit2 = None
        else:
            op_parts = UNIT_OPS[unit_op].split()
            op_tok_ix = toks.index(op_parts[0])
            unit1 = ColUnit.from_toks(toks[:op_tok_ix], db)
            unit2 = ColUnit.from_toks(toks[op_tok_ix+len(op_parts):], db)
        return ValUnit((unit_op, unit1, unit2), db, parsed=True)
    
class TblUnit:
    def __init__(self, tblunit, db=None):
        self.db = db
        self.type, tbl = tblunit
        if self.type == 'table_unit':
            self.table = table_name2id(db, tbl) if isinstance(tbl, str) else tbl
        else:            
            self.table = SQLParse(tbl, db=db)

    def to_spider_parse(self):
        return (self.type, table_id2name(self.db,self.table) if self.type == 'table_unit' else self.table.to_spider_parse())

    def __str__(self):
        if self.type == 'table_unit':
            return '_table_'
        return str(self.table)

    def _list_cols_vals(self, cols, vals):
        if self.type != 'table_unit':
            self.table._list_cols_vals(cols, vals)

    def _list_tables(self, tables):
        if self.type == 'table_unit':
            tables.add(self.table)
        else:
            self.table._list_tables(tables)

    def _list_columns(self, columns, skipsub=False):
        if not skipsub and self.type != 'table_unit':
            self.table._list_columns(columns)

class CondUnit:
    def __init__(self, condunit, db=None, parsed=False):
        self.db = db
        self.not_, self.op_id, valunit, val1, val2 = condunit
        if parsed:
            self.valunit = valunit
            self.val1 = val1
            self.val2 = val2
        else:
            self.valunit = ValUnit(valunit, db)
            self.val1 = Value(val1, db=db)
            self.val2 = Value(val2, db=db)
            

    def to_spider_parse(self):
        return (self.not_, self.op_id, self.valunit.to_spider_parse(), self.val1.to_spider_parse(), self.val2.to_spider_parse())

    def __eq__(self, other):
        if other is None: return False
        return self.not_ == other.not_ and self.op_id == other.op_id and self.valunit == other.valunit
    
    def eq_novalues(self, other):
        if other is None: return False
        return self.not_ == other.not_ and self.op_id == other.op_id and self.valunit == other.valunit 

    def __str__(self):
        if self.val2.empty:
            return '_op_, {}, {}'.format(str(self.valunit), str(self.val1))
        
        return '_op_, {}, {}, {}'.format(str(self.valunit), str(self.val1), str(self.val2))

    def print(self):
        assert self.val1.type != 'sql'
        if self.val1.type == 'litral':
            output = '{} {} {} {}'.format(self.not_, self.op_id, self.valunit.print(), self.val1.val)
        elif self.val1.type == 'joincol':
            output = '{} {} {} col {}'.format(self.not_, self.op_id, self.valunit.print(), self.val1.val)
        if self.val2.type is None: return output
        assert self.val2.type == 'litral'
        output += ' {}'.format(self.val2.val)
        return output

    def toks(self):
        """
        NL here depends on SQL2NL. refactor to call methods in one file.
        """            
        output = self.valunit.toks()
        if self.not_: output.append('not')
        output.append(WHERE_OPS[self.op_id])
        if self.val1.type == 'joincol':
            output.append('col:'+schema.get_column_fullname(self.db,self.val1.val))
        else: 
            pass

        if self.val2.type is None: return output
        assert self.val2.type == 'litral'
        return output

    @classmethod
    def from_toks(cls, toks, db):
        longest_match = 0
        toks_ = ' '.join(toks)
        found = False
        for i, op in enumerate(WHERE_OPS):
            if op in toks_:
                found = True
                if len(op) > longest_match:
                    longest_match = len(op)
                    op_id = i
                    toks_before = toks_[:toks_.index(op)].split()
                    toks_after = toks_[toks_.index(op)+len(op):].split()

        if not found: assert False, 'where diff without where op!'
        if len(toks_before) == 0: assert False, 'No tokens before where op: {}'.format(toks_)
        if toks_before[-1] == 'not':
            not_ = True
            valunit = ValUnit.from_toks(toks_before[:-1], db)
        else:
            not_ = False
            valunit = ValUnit.from_toks(toks_before, db)

        if len(toks_after) > 0:
            assert toks_after[0].startswith('col:'), 'invalid cond format: {}'.format(toks_)
            val1 = (0, int(toks_after[0][4:]), False)            
        else:
            val1 = 'value'
        if WHERE_OPS[op_id] == 'is between':
            val2 = 'value'
        else:
            val2 = None

        return CondUnit((not_, op_id, valunit, Value(val1,db=db), Value(val2,db=db)), db=db, parsed=True)
        
    def _list_cols_vals(self, cols, vals):
        if self.val1:
            if self.val1.type == 'litral':
                cols.append([self.valunit.unit1.col_id, self.val1])
            self.val1._list_cols_vals(cols, vals)
        if self.val2:
            if self.val2.type == 'litral':
                cols.append([self.valunit.unit1.col_id, self.val2])
            self.val2._list_cols_vals(cols, vals)

    def _list_tables(self, tables):
        self.val1._list_tables(tables)
        self.val2._list_tables(tables)

    def _list_columns(self, columns, skipsub=False):
        self.valunit._list_columns(columns)
        self.val1._list_columns(columns, skipsub=skipsub)
        self.val2._list_columns(columns, skipsub=skipsub)

class Select:

    def __init__(self, sel, db=None):
        self.db = db
        self.distinct, cols = sel
        self.aggs = []
        self.valunits = []

        for agg, valunit in cols:
            self.aggs.append(agg)
            self.valunits.append(ValUnit(valunit, db))          

    def to_spider_parse(self):
        return (self.distinct, [(agg, valunit.to_spider_parse()) \
                                        for agg, valunit in zip(self.aggs, self.valunits)] )

    def __str__(self):
        if sum(self.aggs) == 0:
            if all('_col_' == str(valunit) for valunit in self.valunits):
                return '_cols_'
        output = []
        for agg, valunit in zip(self.aggs, self.valunits):
            if agg == 0:
                output.append(str(valunit))
            else:
                output.append('_agg_ {}'.format(str(valunit)))
        return ', '.join(output)

    def list_cols(self):
        return [valunit.unit1.col_id for valunit in self.valunits]

    def _list_columns(self, columns):
        for vu in self.valunits:
            vu._list_columns(columns)


class From:
    def __init__(self, from_, db=None):
        #join condition
        self.db = db
        self.cond = Condition(from_['conds'], db=db)
        self.tables = [TblUnit(tbl, db=db) for tbl in from_['table_units']]

    def list_nested_sqls(self):
        return [tbl.table for tbl in self.tables if tbl.type != 'table_unit']

    def to_spider_parse(self):

        return {'table_units': [tab.to_spider_parse() for tab in self.tables], 'conds': self.cond.to_spider_parse()}

    def __str__(self):
        """ ignoring join condition from pattern as not used in sql2nl
        if self.cond.empty:
            return ', '.join([str(tbl) for tbl in self.tables])
        return '{} with join cond'.format(', '.join([str(tbl) for tbl in self.tables]))
        """
        return ', '.join([str(tbl) for tbl in self.tables])

    def list_tables(self):      
        #for tbl in self.tables:
        #    if tbl.type != 'table_unit': raise ValueError('From is not table_unit.')
        
        return [tbl.table for tbl in self.tables if tbl.type == 'table_unit']

    def list_join_conds(self):
        #join cond is col1 = col2 .. reurn list of [col1,col2]
        ret = []
        if self.cond.empty: return []
        #some join conditions get repeated TODO fix in evaluation script
        for cond in self.cond.conds:
            to_add = (cond.valunit.unit1.col_id, cond.val1.val)
            if to_add not in ret and (to_add[1],to_add[0]) not in ret:
                ret.append(to_add)
        return ret

    def _list_cols_vals(self, cols, vals):
        for tbl in self.tables:
            tbl._list_cols_vals(cols, vals)

    def _list_tables(self, tables):
        for table in self.tables:
            table._list_tables(tables)
        self.cond._list_tables(tables)

    def _list_columns(self, columns, skipjoin=False, skipsub=False):
        if not skipjoin:
            self.cond._list_columns(columns)
        for table in self.tables:
            table._list_columns(columns, skipsub=skipsub)

class Condition:

    def __init__(self, cond,db=None):
        assert db is not None
        self.andor = []
        self.conds = []
        if len(cond) == 0:
            self.empty = True
            return
        self.empty = False

        for i, condunit in enumerate(cond):
            if i%2 == 1:
                self.andor.append(condunit)
            else:
                self.conds.append(CondUnit(condunit,db))

    def list_nested_sqls(self):
        nested = []
        for cond in self.conds:
            if cond.val1 is not None and cond.val1.type == 'sql':
                nested.append(cond.val1.val)
            if cond.val2 is not None and cond.val2.type == 'sql':
                nested.append(cond.val2.val)
        return nested

    def to_spider_parse(self):
        if self.empty: return []
        ret = []
        for i in range(len(self.andor) + len(self.conds)):
            if i %2 == 1:
                ret.append(self.andor[i//2])
            else:
                ret.append(self.conds[i//2].to_spider_parse())
        return ret

    def __str__(self):
        return ', '.join([str(condunit) for condunit in self.conds])

    def _list_cols_vals(self, cols, vals):
        if not self.empty:
            for cond in self.conds:
                cond._list_cols_vals(cols, vals)    

    def _list_tables(self, tables):
        if self.empty: return
        for cond in self.conds:
            cond._list_tables(tables)

    def _list_columns(self, columns, skipsub=False):
        if self.empty: return
        for cond in self.conds:
            cond._list_columns(columns, skipsub=skipsub)

class Groupby:

    def __init__(self, groupby, db):
        self.db = db
        if not groupby:
            self.empty = True
            return
        self.empty = False
        self.cols = [ColUnit(colunit, db) for colunit in groupby]

    def to_spider_parse(self):
        if self.empty: return []
        return [colunit.to_spider_parse() for colunit in self.cols]

    def __str__(self):          
        for col in self.cols:
            if col.agg_id >0:
                return ', '.join([str(col) for col in self.cols])
        return '_cols_'

    def getcols(self):
        assert str(self) == '_cols_'
        return [col.col_id for col in self.cols]

    def _list_columns(self, columns):
        if self.empty: return
        for col in self.cols:
            col._list_columns(columns)

class Orderby:

    def __init__(self, orderby, db=None):
        self.db = db
        self.dir_ = 'asc' #TODO: by default. added to avoid error in to_spider_parse()
        if orderby is None or len(orderby) == 0:
            self.empty = True
            return
        self.empty = False
        self.dir_ = orderby[0]
        self.vals = [ValUnit(val, db) for val in orderby[1]]

    def to_spider_parse(self):
        if self.empty: return []
        return(self.dir_ , [val.to_spider_parse() for val in self.vals])

    def __str__(self):
        return ', '.join([str(val) for val in self.vals])
    
    def get_cols(self):
        return [val.unit1.col_id for val in self.vals]

    def _list_columns(self, columns):
        if self.empty: return
        for val in self.vals:
            val._list_columns(columns)

class SQLParse:

    def __init__(self, sql, schema=None, db=None):        
        if isinstance(sql, str):
            sql = get_sql(schema, sql)
        self.limit = sql['limit'] #None/int
        self.intersect = None if sql['intersect'] is None\
                                 else SQLParse(sql['intersect'], db=db)
        self.except_ = None if sql['except'] is None \
                                else SQLParse(sql['except'], db=db)
        self.union = None if sql['union'] is None else SQLParse(sql['union'], db=db)
        self.select = Select(sql['select'], db=db)
        self.from_ = From(sql['from'], db=db)
        self.where = Condition(sql['where'],db=db)
        self.groupby = Groupby(sql['groupBy'], db=db)
        self.orderby = Orderby(sql['orderBy'], db=db)
        self.having = Condition(sql['having'],db=db)
        self.db = db

    @classmethod
    def get_empty_parse(cls, db):
        sql = {'limit': None, 'intersect':None, 'except':None,'union':None, 
                'select': (False, []),
                'from': {'conds':[], 'table_units':[]},
                'where':[],
                'groupBy':None,
                'orderBy':None,
                'having':[]}
        return SQLParse(sql,db=db)

    def diff(self,target_sql):
        return SQLDiff(self, target_sql, db=self.db)
    
    def apply_diff(self, diff_str):
        #In-place. return a score (the higher the worse) of number of skipped ops (due to parsing errors)
        #and number of application warnings.
        return SQLDiff(diff_str=diff_str, db=self.db).apply(self)

    def to_query_str(self):
        raise NotImplemented()

    def to_spider_parse(self):
        return {
            'except': None if self.except_ is None else self.except_.to_spider_parse(),
            'from': self.from_.to_spider_parse(),
            'groupBy': self.groupby.to_spider_parse(),
            'having': self.having.to_spider_parse(),
            'intersect': None if self.intersect is None else self.intersect.to_spider_parse(),
            'limit': None if self.limit is None else 1, #when limit is nested, evaluation script does not ignore values!!
            'orderBy': self.orderby.to_spider_parse(),
            'select': self.select.to_spider_parse(),
            'union': None if self.union is None else self.union.to_spider_parse(),
            'where':self.where.to_spider_parse()
        }

    def __str__(self):
        #intersect, union, except, limit are easy to explain .. not part of the pattern
        output = ['select: {}, from: {}'.format(\
                        *[str(o) for o in [self.select, self.from_]])]
        if not self.where.empty:
            output.append('where: {}'.format(str(self.where)))
        if not self.groupby.empty:
            output.append('groupby: {}'.format(str(self.groupby)))
        if not self.orderby.empty:
            output.append('orderby: {}'.format(str(self.orderby)))      
        if not self.having.empty:
            output.append('having: {}'.format(str(self.having)))
        return ', '.join(output)

    def get_db(self):
        return self.db

    def _list_tables(self, tables):
        if self.intersect:
            self.intersect._list_tables(tables)
        if self.except_:
            self.except_._list_tables(tables)
        if self.union:
            self.union._list_tables(tables)
        if self.from_:
            self.from_._list_tables(tables)
        if self.where:
            self.where._list_tables(tables)        
        if self.having:
            self.having._list_tables(tables)        


    def _list_columns(self, columns, skipjoin=False, skipsub=False):
        if not skipsub:
            if self.intersect:
                self.intersect._list_columns(columns)
            if self.except_:
                self.except_._list_columns(columns)
            if self.union:
                self.union._list_columns(columns)
        if self.from_:
            self.from_._list_columns(columns,skipjoin=skipjoin, skipsub=skipsub)
        if self.where:
            self.where._list_columns(columns, skipsub=skipsub)            
        if self.having:
            self.having._list_columns(columns, skipsub=skipsub) 
        if self.groupby:
            self.groupby._list_columns(columns)
        if self.orderby:
            self.orderby._list_columns(columns) 
        self.select._list_columns(columns) 


    def _list_cols_vals(self, cols, vals):
        if self.limit:
            cols.append(['limit', self])
            vals.append(self.limit)
        if self.intersect:
            self.intersect._list_cols_vals(cols, vals)
        if self.except_:
            self.except_._list_cols_vals(cols, vals)
        if self.union:
            self.union._list_cols_vals(cols, vals)
        if self.where:
            self.where._list_cols_vals(cols, vals)
        if self.having:
            self.having._list_cols_vals(cols, vals)
        self.from_._list_cols_vals(cols, vals)

    def list_cols_vals(self):
        vals = []
        cols = []
        self._list_cols_vals(cols, vals)
        return cols, vals
        

    def list_tables(self):
        """
        returns a set of tables used in the query
        """
        tables = set()
        self._list_tables(tables)
        return tables

    def list_columns(self, skipjoin=False, skipsub=False):
        """
        returns a set of columns used in the query
        """
        columns = set()
        self._list_columns(columns, skipjoin=skipjoin, skipsub=skipsub)
        return columns


    @classmethod
    def from_str(cls, sql_str, db_name, db_path):    
        schema = Schema(get_schema(db_path))    
        sql = get_sql(schema, sql_str)
        return SQLParse(sql, db=db_name)

    def find_subquery(self, only_nested=False):
        
        if not only_nested:
            #Searching IEU
            if self.intersect is not None: return self.intersect, 'intersect'
            if self.except_ is not None: return self.except_, 'except'
            if self.union is not None: return self.union, 'union'
        #Searching Nested
        if self.where is not None:
            where_nested = self.where.list_nested_sqls()
            if len(where_nested) > 0: return where_nested[0], 'where'
        if self.having is not None:
            having_nested = self.having.list_nested_sqls()
            if len(having_nested) > 0: return having_nested[0], 'having'
        return None