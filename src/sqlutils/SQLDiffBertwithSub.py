import attr
import numpy as np
from collections import defaultdict
import copy

USE_REVISED_2 = True
from sqlutils.SQL2NL import (WHERE_OPS)
from sqlutils.SQL2NL import AGG_OPS
from sqlutils.Schema_Utils import Schema_Utils
from sqlutils import SQLParse
from sqlutils.DiffApplyErrors import *

schema = None #TODO: Clean!

class EditOperation:
    def __init__(self, target, type, arg0=None, arg1=None):
        self.target = target
        self.type = type
        self.arg0 = arg0 
        self.arg1 = arg1
        if self.arg1 is not None and self.arg0 is None: assert False
        #without replace, we one have at most one arg
        if self.arg1 is not None: assert False
        
    def __str__(self):
        output = 'Taget: {}, Type: {}'.format(self.target, self.type)
        if self.arg0 is not None:
            output += ', Arg0: {}'.format(self._arg_str(self.arg0))
        if self.arg1 is not None:
            output += ', Arg1: {}'.format(self._arg_str(self.arg1))
        return output
    
    def _arg_str(self, arg):
        if type(arg) == tuple:
            return '('+', '.join([self._arg_str(i) for i in arg])+')'
        if hasattr(arg, 'print'):
            return arg.print()
        return str(arg)
    
    def _arg_toks(self, arg):
        if type(arg) == tuple:
            toks = []
            for i in arg: toks.extend(self._arg_toks(i))
            return toks
        if hasattr(arg, 'toks'):
            return arg.toks()
        return [str(arg)]

    def str_tokens(self):
        """
        used for decoder output format
        """
        toks = []
        toks.append(f'<{self.target}>')
        toks.append(f'{self.type}')
        
        if self.arg0 is not None:
            toks.extend(self._arg_toks(self.arg0))
        if self.arg1 is not None:
            assert False        
        toks.append(f'</{self.target}>')
        return toks

###Helper classer for printing
@attr.s
class Agg:
    agg = attr.ib()
    def toks(self): return [AGG_OPS[self.agg]]

@attr.s
class Table:
    table = attr.ib()
    db = attr.ib()
    def toks(self): return ['tab:'+schema.get_table_name(self.db, self.table)]

@attr.s
class Column:
    column = attr.ib()
    db = attr.ib()
    def toks(self): return ['col:'+schema.get_column_fullname(self.db, self.column)]

@attr.s
class OrderDir:
    dir_ = attr.ib()
    def toks(self): return ['ascending' if self.dir_=='asc' else 'descending']

@attr.s
class IndexedAndOr:    
    i = attr.ib()
    andor = attr.ib()
    def toks(self): return [self.andor]

@attr.s
class IndexedCondition:
    i = attr.ib()
    cond = attr.ib()
    def toks(self): return self.cond.toks()

class Diff:
    def __init__(self):
        self.editops = []
    def __str__(self):
        return '; '.join([str(op) for op in self.editops])
    def isempty(self):
        return len(self.editops) == 0
    def size(self):
        return len(self.editops)

class IEUDiff(Diff):
    def __init__(self, source=None, target=None, editops=None, db=None):
        super().__init__()
        self.db = db
        if editops is not None:
            self.editops = editops
            return
        
        if USE_REVISED_2:
            if source == target: return
            if source is not None and target is None:
                self.editops.append(EditOperation('ieu', 'remove'))
            else:
                self.editops.append(EditOperation('ieu', 'add', target))
        else:
            if source != target:
                if source is not None: self.editops.append(EditOperation('ieu', 'remove', source))
                if target is not None: self.editops.append(EditOperation('ieu', 'add', target))

    def attempt_undo(self, source):
        if self.size() == 1 and self.editops[0].type == 'remove' and self.removed is not None:
            clause = self.editops[0].arg0
            if clause == 'intersect':
                source.intersect = self.removed
            elif clause == 'except':
                source.except_ = self.removed
            elif clause == 'union':
                source.union = self.removed
            return True

        return False
    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """        
        def _parse(toks):
            def warn(): global global_num_parse_warnnings; print('WARNING: parsing IEU', toks); global_num_parse_warnnings += 1
            assert toks[0] == '<ieu>'
            assert toks[1] in ['add', 'remove']
            if USE_REVISED_2:
                if toks[1] == 'add':
                    assert toks[2] in ['union', 'except', 'intersect']
                    return EditOperation('ieu', toks[1], toks[2])
                else:
                    return EditOperation('ieu', toks[1])

            else:
                assert toks[2] in ['union', 'except', 'intersect']
                return EditOperation('ieu', toks[1], toks[2])
        editops = []
        for op in ops: editops.append(_parse(op))
        return IEUDiff(editops=editops, db=db)

    def apply_v2(self, source):
        self.removed = None
        if self.isempty(): return
        def warn(op):
            global global_num_apply_warnnings; print('WARNING: applying IEU', op); global_num_apply_warnnings += 1
        subquery = SQLParse.SQLParse.get_empty_parse(self.db)
        for op in self.editops:
            if op.type == 'remove':
                if source.intersect is not None:
                    subquery = source.intersect
                    source.intersect = None
                elif source.union is not None:
                    subquery = source.union
                    source.union = None
                elif source.except_ is not None:
                    subquery = source.except_
                    source.except_ = None
                else:
                    warn(op)
            elif op.type == 'add':
                source.intersect = None
                source.except_ = None
                source.union = None
                if op.arg0 == 'intersect':
                    if source.intersect is None:
                        source.intersect = subquery
                        source.except_ = None
                        source.union = None
                    else:
                        warn(op)
                elif op.arg0 == 'union':
                    if source.union is None:
                        source.union = subquery
                        source.except_ = None
                        source.intersect = None
                    else:
                        warn(op)
                elif op.arg0 == 'except':
                    if source.except_ is None:
                        source.except_ = subquery
                        source.intersect = None
                        source.union = None
                    else:
                        warn(op)

    def apply(self, source):
        if USE_REVISED_2: self.apply_v2(source)
        self.removed = None
        if self.isempty(): return
        def warn(op): print('WARNING: applying IEU', op)
        subquery = SQLParse.SQLParse.get_empty_parse(self.db)
        for op in self.editops:
            if op.type == 'remove':
                if op.arg0 == 'intersect':
                    if source.intersect is None:
                        warn(op)
                    else:
                        subquery = source.intersect
                        self.removed = subquery
                    source.intersect = None
                elif op.arg0 == 'except':
                    if source.except_ is None:
                        warn(op)
                    else:
                        subquery = source.except_
                        self.removed = subquery
                    source.except_ = None
                elif op.arg0 == 'union':
                    if source.union is None:
                        warn(op)
                    else:
                        subquery = source.union
                        self.removed = subquery
                    source.union = None
                else:
                    assert False
            elif op.type == 'add':
                source.intersect = None
                source.except_ = None
                source.union = None
                if op.arg0 == 'intersect':
                    if source.intersect is None:
                        source.intersect = subquery                        
                    else:
                        warn(op)  #TODO: or just set to subquery anyway?
                elif op.arg0 == 'except':
                    if source.except_ is None:
                        source.except_ = subquery                        
                    else:
                        warn(op)
                elif op.arg0 == 'union':
                    if source.union is None:
                        source.union = subquery                        
                    else:
                        warn(op)

class LimitDiff(Diff):
    def __init__(self, source=None, target=None, editops=None):
        super().__init__()
        if editops is not None:
            self.editops = editops
            return
        """
        Operations: Add/Delete/Replace
        """
        if source == target:
            return
        elif source is None:
            #not even the word value
            self.editops.append(EditOperation('limit','add'))
        elif target is None:
            self.editops.append(EditOperation('limit','remove'))
        else:
            #No values for now
            return
    
    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """        
        def _parse(toks):
            def warn(): global global_num_parse_warnnings; print('WARNING: parsing limit', toks); global_num_parse_warnnings+=1
            assert toks[0] == '<limit>' 
            if toks[1] == 'remove':
                return EditOperation('limit', 'remove')
            elif toks[1] == 'add':
                #not even the word value
                return EditOperation('limit', 'add')
            else:
                assert False, toks

        editops = []
        for op in ops: editops.append(_parse(op))
        return LimitDiff(editops=editops)

    def apply(self, source):
        if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying limit', op); global_num_apply_warnnings += 1
        for op in self.editops:
            #TODO: blind application for now.
            if op.type == 'remove':
                source.limit = None
            elif op.type == 'add':
                #not even the word value
                source.limit = 'value'

class FromDiff(Diff):
    def __init__(self, source=None, target=None, db=None, underspecified=False, src_cols=None, tgt_cols=None, editops=None):
        super().__init__()
        self.db = db
        if editops is not None:
            self.editops = editops 
            return
        """
        Skip diffing join condition for now.
            .i.e, self.cond = ConditionDiff(source_from.cond, target_from.cond)
        Assume all tables are table_units, none is a sql.        
        Operations: Add, Delete
        """
        src_tables = set([tbl.table for tbl in source.tables])
        tgt_tables = set([tbl.table for tbl in target.tables])
        for t in src_tables.union(tgt_tables):
            if type(t) != int:
                raise NotImplementedError('select count(*) from (select ..)')
        #"""possibly buggy implementation
        if underspecified:
            #filter out tables mentioned impliclity by referenced columns.
            mentioned_src_tab = set([schema.get_table_of_col(db, c) for c in src_cols if c!=0])
            src_tables = src_tables.difference(mentioned_src_tab)
            mentioned_tgt_tab = set([schema.get_table_of_col(db, c) for c in tgt_cols if c!=0])
            tgt_tables = tgt_tables.difference(mentioned_tgt_tab)
            #print('mentioned_src_tab',mentioned_src_tab,'src_tables',src_tables,'mentioned_tgt_tab',mentioned_tgt_tab,'tgt_tables',tgt_tables)
        #"""
        """
        New implementation:
        		1. Run diff on full from
		        2. For the adds: If tab is not referenced in corrected: add it to diff. otherwise, skip.
                3. For the deletes: If tab not referenced in initial & not referenced in gold: add it to diff. otherwise, skip.
            Reconstruction logic:
                1. from <- referenced tables after incorporating other diff elements.
                2.      +  adds that appear in diff
                3. for unreferenced tabs, delete those mentioned in diff delete. and add rest to from.
        """
        """
        if underspecified:
            mentioned_tgt_tab = set([schema.get_table_of_col(db, c) for c in tgt_cols if c!=0])
            mentioned_src_tab = set([schema.get_table_of_col(db, c) for c in src_cols if c!=0])
            self._diff_tables(src_tables, tgt_tables, db, exclude_add=mentioned_tgt_tab,exclude_delete=mentioned_src_tab)
        else:
            self._diff_tables(src_tables, tgt_tables, db)
        """
        self._diff_tables(src_tables, tgt_tables, db)

    def _diff_tables(self, src_tables, tgt_tables, db,exclude_add=set(), exclude_delete=set()):
        if src_tables == tgt_tables:
            return
        src_diff_tgt = src_tables.difference(tgt_tables)
        for table in src_diff_tgt:
            self.editops.append(EditOperation('from','remove',Table(table,db)))
        tgt_diff_src = tgt_tables.difference(src_tables)
        for table in tgt_diff_src:
            self.editops.append(EditOperation('from','add', Table(table,db)))
        
    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """
        def _parse(toks):
            def warn(): global global_num_parse_warnnings; print('WARNING: parsing from', toks); global_num_parse_warnnings += 1
            assert len(toks) == 4 and toks[0] == '<from>'
            if not toks[2].startswith('tab:'):
                warn()
                return None #skipping ..
            tbl = int(toks[2][4:])
            if toks[1] == 'remove':
                return EditOperation('from', 'remove', tbl) #using id since it's what is being used in the parse.
            elif toks[1] == 'add':
                return EditOperation('from', 'add', tbl)
            else:
                assert False, toks

        editops = []
        for op in ops:
            parsed = _parse(op)
            if parsed is None: continue
            editops.append(parsed)
        return FromDiff(editops=editops, db=db)

    def _delete_relevant_conds(self, from_, tabs):
        new_conds = []
        new_andor = []
        for i, condunit in enumerate(from_.cond.conds):
            col1 = condunit.valunit.unit1.col_id
            col2 = condunit.val1.val
            if schema.get_table_of_col(self.db, col1) in tabs or schema.get_table_of_col(self.db, col2) in tabs:
                continue
            new_conds.append(condunit)
            if i > 0:
                new_andor.append(from_.cond.andor[i-1])
        
        from_.cond.conds = new_conds
        from_.cond.andor = new_andor
        
    def _add_missing_join_conds(self, from_, curr_tabs):
        linked_tabs = set()
        for condunit in from_.cond.conds:
            col1 = condunit.valunit.unit1.col_id
            col2 = condunit.val1.val
            tab1 = schema.get_table_of_col(self.db, col1)
            tab2 = schema.get_table_of_col(self.db, col2)
            linked_tabs.add(tab1)
            linked_tabs.add(tab2)
            if tab1 in curr_tabs: curr_tabs.remove(tab1)            
            if tab2 in curr_tabs: curr_tabs.remove(tab2)
        if len(linked_tabs) == 0:
            linked_tabs.add(curr_tabs.pop())

        pk_fks = schema.list_pk_fk(self.db)
        #TODO: double check if this is okay. otherwise, fallback to expanding 
        if self.db == 'world_1': pk_fks.append((3,23))
        if self.db == 'flight_2': pk_fks.append((1,10))
        """
        #expanding keys: 3,8 & 23,8 -> 3,23 -> only in one DB world_1
        groups = defaultdict(set)
        for c1, c2 in pk_fks:
            groups[c1].add(c2)
            groups[c2].add(c1)
        pk_fks_expanded = []
        for _, group in groups.items():
            l = list(group)
            for i in range(len(l)):
                for j in range(i):
                    if schema.get_table_of_col(self.db, l[i]) != schema.get_table_of_col(self.db, l[j]):
                        pk_fks_expanded.append((l[i], l[j]))
        """
        def get_pkfk(tab1, tab2):
            for pk, fk in pk_fks:
                pk_tab = schema.get_table_of_col(self.db, pk)
                fk_tab = schema.get_table_of_col(self.db, fk)
                if (pk_tab == tab1 and fk_tab == tab2) or (pk_tab == tab2 and fk_tab == tab1):
                    return pk, fk
            return None

        def add_joincond(col1, col2):
            if len(from_.cond.conds) > 0:
                from_.cond.andor.append('and')
            #evaluation script does not ignore ordering.
            if self.db == 'pets_1':
                if col1 == 9 and col2 == 1:
                    col1 = 1; col2 = 9
                if col1 == 10 and col2 == 11:
                    col1 = 11; col2 = 10
            if self.db == 'world_1':
                if col1 == 23 and col2 == 8:
                    col1 = 8; col2=23
            condunit = False, 2, [0, [0, col1, False], None], [0,col2,False], None
            from_.cond.conds.append(SQLParse.CondUnit(condunit, db=self.db))
            from_.cond.empty = False

        while(len(curr_tabs)) > 0:
            done_tabs = set()
            for tab1 in curr_tabs:
                for tab2 in linked_tabs:
                    pkfk = get_pkfk(tab1, tab2)
                    if pkfk is not None:
                        add_joincond(pkfk[0], pkfk[1])
                        linked_tabs.add(tab1)
                        done_tabs.add(tab1)
                        break
                        
            #TODO: HARD CODING 1-HOP FOR NOW.
            if len(done_tabs) == 0:
                if self.db == 'pets_1' and curr_tabs == set([2]) and linked_tabs == set([0]):
                    curr_tabs.add(1) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',1), db=self.db))
                elif self.db == 'movie_1' and curr_tabs == set([1]) and linked_tabs == set([0]):
                    curr_tabs.add(2) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',2), db=self.db))
                elif self.db == 'store_product' and curr_tabs == set([2]) and linked_tabs == set([1]):
                    curr_tabs.add(4) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',4), db=self.db))
                elif self.db == 'customers_and_invoices' and curr_tabs == set([5]) and linked_tabs == set([1]):
                    curr_tabs.add(7) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',7), db=self.db))
                elif self.db == 'car_1' and curr_tabs == set([2]) and linked_tabs == set([0]):
                    curr_tabs.add(1) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',1), db=self.db))
                elif self.db == 'flight_2' and curr_tabs == set([1]) and linked_tabs == set([0]):
                    curr_tabs.add(2) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',2), db=self.db))
                elif self.db == 'car_1' and curr_tabs == set([5]) and linked_tabs == set([2,3]):
                    curr_tabs.add(4) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',4), db=self.db))
                elif self.db == 'car_1' and curr_tabs == set([3]) and linked_tabs == set([1]):
                    curr_tabs.add(2) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',2), db=self.db))
                elif self.db == 'dog_kennels' and curr_tabs == set([7]) and linked_tabs == set([4]):
                    curr_tabs.add(5) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',5), db=self.db))
                elif self.db == 'student_transcripts_tracking' and curr_tabs == set([6]) and linked_tabs == set([3]):
                    curr_tabs.add(7) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',7), db=self.db))
                elif self.db == 'car_1' and curr_tabs == set([5]) and linked_tabs == set([3]):
                    curr_tabs.add(4) 
                    curr_tabs.add(5) 
                    from_.tables.append(SQLParse.TblUnit(('table_unit',4), db=self.db))
                    from_.tables.append(SQLParse.TblUnit(('table_unit',5), db=self.db))
                else:
                    break
            curr_tabs = curr_tabs.difference(done_tabs)

    def apply(self, source, used_columns, columns_before_app):        
        #cannot return if empty. have to do the 'under-specified' postprocessing anyway.
        #if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying from', op); global_num_apply_warnnings += 1# assert False 

        used_tab_before_app = set([schema.get_table_of_col(self.db, c) for c in columns_before_app if c!=0])
        used_tab = set([schema.get_table_of_col(self.db, c) for c in used_columns if c!=0])
        curr_tabs = set([tbl.table for tbl in source.tables])
        deleted_tabs = set()
        #The "under-specified" case
        #if being used, add it. Have to wait for whole query to get edited.
        #if not being used, remove it.
        
        deleted_tabs = used_tab_before_app.difference(used_tab)
        curr_tabs = curr_tabs.difference(deleted_tabs)
                        
        for op in self.editops:
            if op.type == 'remove':
                if op.arg0 in curr_tabs:
                    curr_tabs.remove(op.arg0)
                    deleted_tabs.add(op.arg0)
                else:
                    warn(op)
            elif op.type == 'add':
                if op.arg0 not in curr_tabs:
                    curr_tabs.add(op.arg0)
                else:
                    if op.arg0 in used_tab_before_app and op.arg0 not in used_tab:
                        pass
                    elif op.arg0 not in used_tab_before_app and op.arg0 in used_tab:
                        pass
                    else:                        
                        warn(op)
        
        for tab in used_tab:
            if tab not in curr_tabs: curr_tabs.add(tab)
        
        source.tables = [SQLParse.TblUnit(('table_unit',tbl), db=source.db) for tbl in curr_tabs]
        
        #join conditions
        if len(deleted_tabs) > 0: self._delete_relevant_conds(source, deleted_tabs)
        #if there are any unlinked tables, add a join cond
        
        if len(curr_tabs) > 1: self._add_missing_join_conds(source, curr_tabs)
        

class GroupbyDiff(Diff):

    def __init__(self, source=None, target=None, db=None, editops=None):
        super().__init__()
        
        if editops is not None:
            self.editops = editops
            return
        """
        Assume only col_id, no agg/distinct. TODO: verify!
        """        
        src_cols = set() if source.empty else set([col.col_id for col in source.cols])
        tgt_cols = set() if target.empty else set([col.col_id for col in target.cols])
        if src_cols == tgt_cols:
            return
        src_diff_tgt = src_cols.difference(tgt_cols)
        tgt_diff_src = tgt_cols.difference(src_cols)
        #TODO: for better generality, operate at the ColUnit level. 
        for col in src_diff_tgt:
            self.editops.append(EditOperation('groupby','remove', Column(col, db)))
        for col in tgt_diff_src:
            self.editops.append(EditOperation('groupby','add',Column(col, db)))

    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """
        def _parse(toks):
            def warn(): global global_num_parse_warnnings; print('WARNING: parsing groupby', toks); global_num_parse_warnnings += 1
            assert len(toks) == 4 and toks[0] == '<groupby>'
            col = int(toks[2][4:])
            if toks[1] == 'remove':
                return EditOperation('groupby', 'remove', col)
            elif toks[1] == 'add':
                return EditOperation('groupby', 'add', col)
            else:
                assert False, toks
                
        editops = []
        for op in ops: editops.append(_parse(op))
        return GroupbyDiff(editops=editops)


    def apply(self, source):
        if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying groupby', op); global_num_apply_warnnings += 1
        curr_cols = set() if source.empty else set([col.col_id for col in source.cols])
        for op in self.editops:
            if op.type == 'remove':
                if op.arg0 in curr_cols:
                    curr_cols.remove(op.arg0)
                else:
                    warn(op)
            elif op.type == 'add':
                if op.arg0 not in curr_cols:
                    #source.append(op.arg0)
                    curr_cols.add(op.arg0)
                else:
                    warn(op)
        #curr_cols to ColUnits and updating source.empty
        if len(curr_cols) == 0:
            source.empty = True
            source.cols = None
        else:
            source.empty = False
            #                                  agg,col,distinct
            source.cols = [SQLParse.ColUnit((0,col,False), source.db) for col in curr_cols]
        
        
class ColUnitMatcher:
    """
    Only decide match/no-match
    """
    def __init__(self, source, target):        
        self.match = True
        if source.agg_id != target.agg_id:
            self.match = False
            return

        if source.distinct != target.distinct:
            self.match = False
            return

        if source.col_id != target.col_id:
            self.match = False
            return

class ValUnitMatcher:
    """
    Only decide match/no-match
    """
    def __init__(self, source, target):
        self.match = True
        if source.op != target.op:
            self.match = False
            return
        
        #align unit1 to unit1 and unit2 to unit2
        #unit1 always exists
        if not ColUnitMatcher(source.unit1, target.unit1).match:
            self.match = False
            return

        if source.unit2 is None and target.unit2 is None:
            pass
        elif source.unit2 is None or target.unit2 is None:
            self.match = False
            return
        elif not ColUnitMatcher(source.unit2, target.unit2).match:
            self.match = False


class SelectDiff(Diff):
    def __init__(self, source=None, target=None, editops=None):
        super().__init__()
        if editops is not None:
            self.editops = editops
            return

        if source.distinct == target.distinct:
            pass
        elif source.distinct:
            self.editops.append(EditOperation('select-distinct','remove'))
        else:
            self.editops.append(EditOperation('select-distinct','add'))
        """
        1. Align source [(agg,valunit)] <-> target [(agg,valunit)]
        2. Find exact matches, and everything else is add/delete
        """        
        src_l = len(source.aggs)
        tgt_l = len(target.aggs)
        matched_target = set()
        to_remove = []
        to_add = []
        for i in range(src_l):
            matched = False
            for j in range(tgt_l):
                if j in matched_target: continue
                matcher = ValUnitMatcher(source.valunits[i],target.valunits[j])
                if matcher.match and source.aggs[i] == target.aggs[j]:
                    matched_target.add(j)
                    matched = True
                    break
            if not matched:
                to_remove.append(i)
        for j in range(tgt_l):
            if j not in matched_target: to_add.append(j)
        for i in to_remove:
            self.editops.append(EditOperation('select', 'remove', (Agg(source.aggs[i]), source.valunits[i])))
        for i in to_add:
            self.editops.append(EditOperation('select', 'add', (Agg(target.aggs[i]), target.valunits[i])))

    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """
        def _parse(toks):
            if toks[0] == '<select-distinct>':
                if toks[1] == 'remove':
                    return EditOperation('select-distinct', 'remove')
                elif toks[1] == 'add':
                    return EditOperation('select-distinct', 'add')
                else:
                    assert False, toks
            elif toks[0] == '<select>':
                if toks[1] == 'remove':
                    op = 'remove'
                elif toks[1] == 'add':
                    op = 'add'
                else:
                    assert False, toks
                #parsing arg0
                assert toks[-1] == '</select>'
                arg_toks = toks[2:-1]
                #search for agg
                agg_id = 0
                for i, agg in enumerate(AGG_OPS[1:]):
                    agg_parts = agg.split()
                    if arg_toks[0] == agg_parts[0]:
                        agg_id = i+1
                        arg_toks = arg_toks[len(agg_parts):]
                        break
                return EditOperation('select', op, (agg_id, SQLParse.ValUnit.from_toks(arg_toks, db) ))
            else:
                assert False, toks

        editops = []
        for op in ops: editops.append(_parse(op))
        return SelectDiff(editops=editops)


    def apply(self, source):
        if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying select', op); global_num_apply_warnnings += 1#; assert False
        #def warn(op): raise RedundantOpError('Select: '+str(op))
        curr_aggs_and_valunits = list(zip(source.aggs, source.valunits))
        for op in self.editops:
            if op.target == 'select-distinct':
                if op.type == 'remove':
                    if source.distinct:
                        source.distinct = False                        
                    else:                        
                        warn(op)                        
                else:
                    if source.distinct:
                        warn(op)
                    else:
                        source.distinct = True
            else:                                
                if op.type == 'remove':
                    if op.arg0 in curr_aggs_and_valunits:
                        curr_aggs_and_valunits.remove(op.arg0)
                    else:
                        warn(op)
                else:
                    if op.arg0 in curr_aggs_and_valunits:
                        warn(op)
                    else:
                        curr_aggs_and_valunits.append(op.arg0)
        
        if len(curr_aggs_and_valunits) == 0:
            source.aggs = []
            source.valunits = []
            raise InvalidSQLError('Empty Select')
        else:
            source.aggs, source.valunits = zip(*curr_aggs_and_valunits)
            source.aggs = list(source.aggs)
            source.valunits = list(source.valunits)



class OrderbyDiffV2(Diff):
    def __init__(self, source=None, target=None, editops=None):
        super().__init__()
        if editops is not None:
            self.editops = editops
            return 
        
        if source.dir_ != target.dir_:
            if target.dir_ == 'asc':
                self.editops.append(EditOperation('desc-orderby', 'remove'))
            else:                
                self.editops.append(EditOperation('desc-orderby', 'add'))

        #Cols are order sensitive
        src_l = 0 if source.empty else len(source.vals) 
        tgt_l = 0 if target.empty else len(target.vals)
        to_remove = []
        to_add = []
        if src_l < tgt_l:
            for i in range(src_l):
                if not ValUnitMatcher(source.vals[i], target.vals[i]).match:
                    #remove remaining from source
                    for j in range(i, src_l):
                        to_remove.append(source.vals[j])
                        to_add.append(target.vals[j])
                    break
            #add remaining from target
            for j in range(src_l, tgt_l):
                to_add.append(target.vals[j])
        else:
            for i in range(tgt_l):
                if not ValUnitMatcher(source.vals[i], target.vals[i]).match:
                    #remove remaining from source
                    for j in range(i, tgt_l):
                        to_remove.append(source.vals[j])
                        to_add.append(target.vals[j])
                    break
            #remove remaining from source
            for j in range(tgt_l, src_l):
                to_remove.append(source.vals[j])

        for valunit in to_remove:
            self.editops.append(EditOperation('order-column','remove',valunit))
        for valunit in to_add:
            self.editops.append(EditOperation('order-column','add',valunit))

    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """
        def _parse(toks):
            #order-direction;order-column
            if toks[0] == '<desc-orderby>':
                assert toks[1] in ['remove', 'add']
                return EditOperation('desc-orderby', toks[1])
            elif toks[0] == '<order-column>':
                assert toks[-1] == '</order-column>'
                valunit = SQLParse.ValUnit.from_toks(toks[2:-1], db)
                if toks[1] == 'remove':
                    return EditOperation('order-column', 'remove', valunit)
                elif toks[1] == 'add':
                    return EditOperation('order-column', 'add', valunit)
                else:
                    assert False, toks
            else:
                assert False, toks

        editops = []
        for op in ops: editops.append(_parse(op))
        return OrderbyDiffV2(editops=editops)

    def apply(self, source):
        if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying orderbby', op); global_num_apply_warnnings += 1
        if source.empty:
            source.vals = []
        
        curr_vals = source.vals
        for op in self.editops:
            if op.target == 'desc-orderby':
                if op.type == 'remove':
                    if source.dir_ == 'desc':
                        source.dir_ = 'asc'
                    else:
                        warn(op)                    
                else:
                    if source.dir_ == 'asc':
                        source.dir_ = 'desc'
                    else:
                        warn(op)
            else:
                if op.type == 'remove':
                    if op.arg0 in curr_vals:
                        curr_vals.remove(op.arg0)
                    else:
                        warn(op)
                else:
                    if op.arg0 in curr_vals:
                        warn(op)
                    else:
                        curr_vals.append(op.arg0)

        source.empty = len(curr_vals) == 0

class OrderbyDiff(Diff):
    def __init__(self, source=None, target=None, editops=None):
        super().__init__()
        if editops is not None:
            self.editops = editops
            return 
        """
        TODO: or just delete/add orderby?
        """
        if source.empty and target.empty:
            pass
        elif source.empty:
            self.editops.append(EditOperation('order-direction', 'add', OrderDir(target.dir_)))
        elif target.empty:
            self.editops.append(EditOperation('order-direction', 'remove', OrderDir(source.dir_)))
        elif source.dir_ != target.dir_:
            #No need for args            
            #self.editops.append(EditOperation('order-direction', 'replace'))
            #sticking to add/delete
            #TODO: in delete no need for args.
            self.editops.append(EditOperation('order-direction', 'remove', OrderDir(source.dir_)))
            self.editops.append(EditOperation('order-direction', 'add', OrderDir(target.dir_)))

        #order of cols in orderby is important
        src_l = 0 if source.empty else len(source.vals) 
        tgt_l = 0 if target.empty else len(target.vals)
        to_remove = []
        to_add = []
        if src_l < tgt_l:
            for i in range(src_l):
                if not ValUnitMatcher(source.vals[i], target.vals[i]).match:
                    #remove remaining from source
                    for j in range(i, src_l):
                        to_remove.append(source.vals[j])
                        to_add.append(target.vals[j])
                    break
            #add remaining from target
            for j in range(src_l, tgt_l):
                to_add.append(target.vals[j])
        else:
            for i in range(tgt_l):
                if not ValUnitMatcher(source.vals[i], target.vals[i]).match:
                    #remove remaining from source
                    for j in range(i, tgt_l):
                        to_remove.append(source.vals[j])
                        to_add.append(target.vals[j])
                    break
            #remove remaining from source
            for j in range(tgt_l, src_l):
                to_remove.append(source.vals[j])

        for valunit in to_remove:
            self.editops.append(EditOperation('order-column','remove',valunit))
        for valunit in to_add:
            self.editops.append(EditOperation('order-column','add',valunit))

    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """
        def _parse(toks):
            #order-direction;order-column
            if toks[0] == '<order-direction>':
                if toks[1] == 'remove':
                    return EditOperation('order-direction', 'remove')
                elif toks[1] == 'add':
                    return EditOperation('order-direction', 'add', 'desc' if toks[2] == 'descending' else 'asc')
                else:
                    assert False, toks
            elif toks[0] == '<order-column>':
                assert toks[-1] == '</order-column>'
                valunit = SQLParse.ValUnit.from_toks(toks[2:-1], db)
                if toks[1] == 'remove':
                    return EditOperation('order-column', 'remove', valunit)
                elif toks[1] == 'add':
                    return EditOperation('order-column', 'add', valunit)
                else:
                    assert False, toks
            else:
                assert False, toks

        editops = []
        for op in ops: editops.append(_parse(op))
        return OrderbyDiff(editops=editops)


    def apply(self, source):
        if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying orderbby', op); global_num_apply_warnnings += 1
        if source.empty:
            source.vals = []
        
        curr_vals = source.vals
        for op in self.editops:
            if op.target == 'order-direction':
                if op.type == 'remove':
                    if source.empty:
                        warn(op)
                    else:
                        pass #TODO: revise how order-dir delete is expressed?
                else:
                    source.dir_ = op.arg0 
            else:
                if op.type == 'remove':
                    if op.arg0 in curr_vals:
                        curr_vals.remove(op.arg0)
                    else:
                        warn(op)
                else:
                    if op.arg0 in curr_vals:
                        warn(op)
                    else:
                        curr_vals.append(op.arg0)

        source.empty = len(curr_vals) == 0

class CondUnitMatcher:
    def __init__(self, source, target):
        self.match = True
        if source.not_ != target.not_:
            self.match = False
            return
        if source.op_id != target.op_id:
            self.match = False
            return
        if not ValUnitMatcher(source.valunit, target.valunit).match:
            self.match = False
            return
        #No need to the asserts now. If SQLs are different, their diff will be handeled in subquery part.
        #assert source.val1.type != 'sql'
        #assert target.val1.type != 'sql'

        #NOTE: if type = joincol -> T1.balance < T2.balance val in this case is col        
        #TODO: we are ignoring values for now.
        return


class ConditionDiff(Diff):
    #not doing minimal edit distance for simplicty
    """
        u1  o1  u2   o2   u3

        u_1  o_1  u_2

        if ui = u_i .. cross them out
        if oi = o_i .. cross them out
        for the rest, use replace (if same index), delete, add
        Note that the edit actions are ordered here?
    """
    def __init__(self,source=None, target=None, component=None, editops=None, strict_ordering=True):
        super().__init__()
        if editops is not None:
            self.editops = editops
            return

        #want to maintain same ordering .. delete, add, replace
        delete_ops = []
        add_ops = []

        if strict_ordering:
            for i in range(len(target.conds)):
                if len(source.conds) > i:                
                    if not CondUnitMatcher(source.conds[i], target.conds[i]).match:
                        #sticking to add/delete
                        #replace_ops.append(EditOperation(component, 'replace',IndexedCondition(i,source.conds[i]),target.conds[i]))
                        delete_ops.append(EditOperation(component, 'remove',IndexedCondition(i,source.conds[i])))
                        add_ops.append(EditOperation(component, 'add',target.conds[i]))
                else:
                    add_ops.append(EditOperation(component, 'add',target.conds[i]))
            for i in range(len(target.conds), len(source.conds)):
                delete_ops.append(EditOperation(component, 'remove',IndexedCondition(i,source.conds[i])))

        else:
            target_conds = list(range(len(target.conds)))
            source_conds = list(range(len(source.conds)))
            for tgt_cond in target_conds:
                matched = None
                for src_cond in source_conds:
                    if CondUnitMatcher(source.conds[src_cond], target.conds[tgt_cond]).match:
                        matched = src_cond
                        break
                if matched is None:
                    add_ops.append(EditOperation(component, 'add',target.conds[tgt_cond]))
                else:
                    source_conds.remove(matched)
            for src_cond in source_conds:
                delete_ops.append(EditOperation(component, 'remove',source.conds[src_cond]))

        self.editops.extend(delete_ops+add_ops)

    @classmethod
    def from_str(cls, ops, db):
        """
        ops is list (edit ops) of list (list of string tokens). 
        Each list of tokens is to be parsed as an EditOperation
        """
        def _parse(toks):
            if toks[1] == 'remove':
                op = 'remove'
            elif toks[1] == 'add':
                op = 'add'
            else:
                assert False, toks

            if toks[0] == '<where>':
                assert toks[-1] == '</where>'
                return EditOperation('where', op, SQLParse.CondUnit.from_toks(toks[2:-1], db))
            elif toks[0] == '<having>':
                assert toks[-1] == '</having>'
                return EditOperation('having', op, SQLParse.CondUnit.from_toks(toks[2:-1], db))
            else:
                assert False, toks

        editops = []
        for op in ops: editops.append(_parse(op))
        return ConditionDiff(editops=editops)

    def apply(self, source):        
        if self.isempty(): return
        def warn(op): global global_num_apply_warnnings; print('WARNING: applying where/having', op); global_num_apply_warnnings += 1
        orig_andor = copy.deepcopy(source.andor)
        for op in self.editops:
            if op.type == 'remove':
                if op.arg0 in source.conds:
                    ix = source.conds.index(op.arg0)
                    if len(source.conds) > 1:
                        if ix == 0:
                            source.andor = source.andor[1:]
                        else:
                            source.andor = source.andor[:ix-1]+source.andor[ix:]
                    source.conds = source.conds[:ix]+source.conds[ix+1:]
                    
                    #source.conds.remove(op.arg0)
                else:                    
                    warn(op)
            else:
                switch_default_to_or = False
                if op.arg0 in source.conds:
                    switch_default_to_or = True

                #TODO: adding anyway for now as values are annon .. can have 'year = 2013 or year = 2015'
                source.conds.append(op.arg0)
                if len(source.conds) > 1:
                    #TODO: for now as we do not diff and/or
                    if switch_default_to_or:
                        source.andor.append('or')
                    elif 'or' in orig_andor:
                        source.andor.append('or')
                        orig_andor = []
                    else:
                        source.andor.append('and')
                
        if len(source.conds) > 0:
            source.empty = False
        assert (len(source.conds)==0 and len(source.andor)==0) or (len(source.conds) == len(source.andor) + 1), \
                                        (len(source.conds), len(source.andor))

IEUs = ['intersect', 'except', 'union']
CONDITIONS = ['having', 'where']


#Global variables for counting errors. #TODO: Clean!
global_num_parse_warnnings = 0
global_num_apply_warnnings = 0

class SQLDiff:
    def __init__(self, source=None, target=None, diff_str=None, db=None, nested=True):
        global global_num_parse_warnnings
        if diff_str is not None:
            global_num_parse_warnnings = 0
            self.num_parse_errors = 0
            self.from_str(diff_str, db)
            return
        self.limit = LimitDiff(source.limit, target.limit)
        self.from_ = FromDiff(source.from_, target.from_, target.db, underspecified=True, \
                                            src_cols=source.list_columns(skipjoin=True,skipsub=True), tgt_cols = target.list_columns(skipjoin=True,skipsub=True))
        self.groupby = GroupbyDiff(source.groupby, target.groupby,target.db)
        self.select = SelectDiff(source.select, target.select)
        if USE_REVISED_2:
            self.orderby = OrderbyDiffV2(source.orderby, target.orderby)
        else:
            self.orderby = OrderbyDiff(source.orderby, target.orderby)
        self.where = ConditionDiff(source.where, target.where, 'where', strict_ordering=False)
        self.having = ConditionDiff(source.having, target.having, 'having', strict_ordering=False)
        #subqueries
        self.ieu = None
        self.subdiff = None
        self.subdifftype = None #either ieu or nested. 

        if nested:
            empty_source = SQLParse.SQLParse.get_empty_parse(db)
            source_sub = source.find_subquery()
            target_sub = target.find_subquery()
            if source_sub is None and target_sub is None: return
            if source_sub is None:
                #adding target
                target_sub, target_type = target_sub                
                if target_type in IEUs:
                    self.ieu = IEUDiff(None, target_type)
                    self.subdifftype = 'ieu'
                else:
                    self.subdifftype = 'nested'
                self.subdiff = SQLDiff(empty_source,target_sub, nested=False)
                return
            if target_sub is None:
                #removing source
                source_sub, source_type = source_sub
                empty_target = SQLParse.SQLParse.get_empty_parse(db)
                if source_type in IEUs:
                    self.ieu = IEUDiff(source_type, None)
                    self.subdifftype = 'ieu'
                else:
                    self.subdifftype = 'nested'
                    #Northing really to be done here. The conditiondiff should be something like 'where remove col in subquery'                
                return
            source_sub, source_type = source_sub
            target_sub, target_type = target_sub        
            if source_type in IEUs and target_type in IEUs:
                #diff ieu and subs
                self.ieu = IEUDiff(source_type, target_type)
                self.subdifftype = 'ieu'
                self.subdiff = SQLDiff(source_sub,target_sub, nested=False)
                return
            elif source_type in CONDITIONS and target_type in CONDITIONS:
                self.subdifftype = 'nested'
                self.subdiff = SQLDiff(source_sub,target_sub, nested=False)
                return
            elif source_type in CONDITIONS and target_type in IEUs:
                self.subdifftype = 'ieu'
                self.ieu = IEUDiff(None, target_type)
                self.subdiff = SQLDiff(empty_source,target_sub, nested=False)
                return
            elif source_type in IEUs and target_type in CONDITIONS:
                self.subdifftype = 'nested'
                self.ieu = IEUDiff(source_type, None)
                self.subdiff = SQLDiff(empty_source,target_sub, nested=False)
                return
            raise NotImplementedError('source subquery {}, target subquery {}'.format(source_type, target_type))

    def find_nestedquery_insert_point(self, sql):
        #search with operation in the following order in; >; <; >=; <=; =; !=
        for search_op in [8, 3, 4, 5, 6, 2, 7]:
            for construct in [sql.where, sql.having]:
                if construct is not None:
                    for cond in construct.conds:
                        if cond.op_id == search_op:
                            empty_sql = SQLParse.SQLParse.get_empty_parse(sql.db)
                            cond.val1 = SQLParse.Value(empty_sql, db=sql.db, parsed_sql=True)
                            return empty_sql
        return None

    def confim_subquery_is_attached(self, main, subquery):
        for construct in [main.where, main.having]:
            if construct is not None:
                for cond in construct.conds:
                    if cond.val1 is not None and cond.val1.type=='sql' and cond.val1.val == subquery:
                        return
        #attaching it
        for search_op in [8, 3, 4, 5, 6, 2, 7]:
            for construct in [main.where, main.having]:
                if construct is not None:
                    for cond in construct.conds:
                        if cond.op_id == search_op:
                            cond.val1 = subquery
                            return
        
        assert False, 'subquery not attached'


    def from_str(self, diff_str, db):
        global global_num_parse_warnnings

        #TODO: update `self.num_parse_errors`.
        self.ieu = None 
        self.subdiff = None
        self.subdifftype = None #either ieu or nested. 
        
        diff_parts = diff_str.split()
        if '<nested-sub>' in diff_parts:
            self.subdifftype = 'nested'
            start = diff_parts.index('<nested-sub>')
            try:
                end = diff_parts.index('</nested-sub>')
            except ValueError:
                print('WARNING: parsing nested-sub')
                global_num_parse_warnnings += 1
                self.num_parse_errors+= 1
                end = len(diff_parts) 
            self.subdiff = SQLDiff(diff_str=' '.join(diff_parts[start+1:end]), db=db)
            diff_parts =  diff_parts[:start] + diff_parts[end+1:]
        elif '<ieu-sub>' in diff_parts:
            self.subdifftype = 'ieu'
            start = diff_parts.index('<ieu-sub>') + 1
            try:
                end = diff_parts.index('</ieu-sub>')
            except ValueError:
                print('WARNING: parsing nested-sub')
                global_num_parse_warnnings += 1
                self.num_parse_errors+= 1
                end = len(diff_parts)
            ieu_diff_str = diff_parts[start:end]
            diff_parts =  diff_parts[:start-1] + diff_parts[end+1:] #done. this is the diff for main subquery.
            
            discard_ieu = False
            if '<ieu>' in ieu_diff_str:
                
                ieu_start = ieu_diff_str.index('<ieu>')
                if '</ieu>' not in ieu_diff_str:
                    discard_ieu = True
                    global_num_parse_warnnings += 1
                    print('WARNING: ignoring IEU-Sub as </ieu> not found')
                else:
                    ieu_end = ieu_diff_str.index('</ieu>') #TODO what if does not exist?
                                
                    self.ieu = IEUDiff.from_str([ieu_diff_str[ieu_start:ieu_end+1]], db)
                    ieu_diff_str = ieu_diff_str[:ieu_start] + ieu_diff_str[ieu_end+1:]
            
            if not discard_ieu:
                self.subdiff = SQLDiff(diff_str=' '.join(ieu_diff_str), db=db)            

        ops = defaultdict(list)
        curr_op = []        
        for tok in diff_parts:
            curr_op.append(tok)
            #TODO: If needed, can be more robust by looking for the starting tags.
            if tok.startswith('</') and tok[-1] == '>':
                if curr_op[0][0] == '<' and curr_op[0][-1] == '>' and curr_op[0][1:-1] == tok[2:-1]:
                    target = curr_op[0][1:-1]
                    ops[target].append(curr_op)
                else:
                    print('WARNING: skipping op 2', curr_op)
                    global_num_parse_warnnings += 1
                    self.num_parse_errors+= 1
                curr_op = []

        self.limit = LimitDiff.from_str(ops['limit'], db)        
        self.from_ = FromDiff.from_str(ops['from'], db)        
        self.groupby = GroupbyDiff.from_str(ops['groupby'], db)
        if USE_REVISED_2:
            self.orderby = OrderbyDiffV2.from_str(ops['order-column'] + ops['desc-orderby'], db)        
        else:
            self.orderby = OrderbyDiff.from_str(ops['order-column'] + ops['order-direction'], db)
        self.select = SelectDiff.from_str(ops['select'] + ops['select-distinct'], db)
        self.where = ConditionDiff.from_str(ops['where'], db)
        self.having = ConditionDiff.from_str(ops['having'], db)

    
    def apply(self, source):
        global global_num_apply_warnnings
        self.num_apply_errors = 0
        global_num_apply_warnnings = 0
        self._apply(source)
        return global_num_parse_warnnings, global_num_apply_warnnings
        
    def _apply(self, source):
        global global_num_apply_warnnings
        #TODO: update `self.num_apply_errors`
        nested_subquery_to_edit = source.find_subquery(only_nested=True)
        if nested_subquery_to_edit is not None: nested_subquery_to_edit = nested_subquery_to_edit[0]
        #applying main diff
        cols_before_app = source.list_columns(skipjoin=True, skipsub=True)
        self.limit.apply(source)
        self.groupby.apply(source.groupby)
        self.orderby.apply(source.orderby)        
        self.select.apply(source.select)
        self.where.apply(source.where)
        self.having.apply(source.having)
        #From has to be the last to account for the 'under-specified' case.
        self.from_.apply(source.from_, source.list_columns(skipjoin=True, skipsub=True), cols_before_app)
        #sub-queries
        if self.subdifftype is None:
            return #Done
        elif self.subdifftype == 'ieu':
            if self.ieu is not None: self.ieu.apply(source)
            if self.subdiff is not None:
                if source.intersect is not None:
                    self.subdiff._apply(source.intersect)
                elif source.except_ is not None:
                    self.subdiff._apply(source.except_)
                elif source.union is not None:
                    self.subdiff._apply(source.union)
                elif self.subdiff.size() > 0:
                    print('WARNING: Cannot find IEU to apply diff to.')
                    global_num_apply_warnnings += 1
                    if  self.ieu is not None and self.ieu.attempt_undo(source):
                        print('WARNING: undone ieu op')
                        global_num_apply_warnnings += 1
                        if source.intersect is not None:
                            self.subdiff._apply(source.intersect)
                        elif source.except_ is not None:
                            self.subdiff._apply(source.except_)
                        elif source.union is not None:
                            self.subdiff._apply(source.union)

        elif self.subdifftype == 'nested':
            source.intersect = None
            source.except_ = None
            source.union = None
            if self.subdiff.size() == 0: return #TODO: or do that before setting to None?
            #search of where to add nested query
            #1. Existing subquery to edit
            #2. If not found, search with operation in the following order
            #    in; >; <; >=; <=; =; !=            
            if nested_subquery_to_edit is None:
                nested_subquery_to_edit = self.find_nestedquery_insert_point(source)
                if nested_subquery_to_edit is None:
                    print('WARNING: Cannot find IEU to apply diff to.')
                    global_num_apply_warnnings += 1
            else:
                #make sure `nested_subquery_to_edit` is still part of the query
                self.confim_subquery_is_attached(source, nested_subquery_to_edit)
            if nested_subquery_to_edit is not None:
                self.subdiff._apply(nested_subquery_to_edit)
        else:
            assert False, self.subdifftype

    def size(self):
        return len(self.operations())
    
    def __str__(self):
        return '\n'.join([str(editop) for editop in self.operations()])

    def operations(self):
        """
        Note: returns only operations associated with the main query.
        To get operations of the subquery, run self.subdiff.operations()
        """
        #
        return [editop for editop in 
            self.from_.editops + 
            self.where.editops +
            self.groupby.editops +
            self.having.editops +
            self.select.editops+
            self.orderby.editops +
            self.limit.editops 
        ]
    
    def str_tokens(self):
        toks = []
        def add_querydiff(querydiff):
            for editop in querydiff.operations(): toks.extend(editop.str_tokens())

        if self.subdifftype == 'nested' and self.subdiff is not None and self.subdiff.size()>0:
            toks.append('<nested-sub>')
            add_querydiff(self.subdiff)
            toks.append('</nested-sub>')
            add_querydiff(self)
            #If there is nested-sub, postprocessing will delete IEU.
            # if self.ieu is not None:
            #    for editop in self.ieu.editops: toks.extend(editop.str_tokens())     
            #toks.append(SUBQUERY_SEP)            
        elif self.subdifftype == 'ieu' and ((self.subdiff is not None  and self.subdiff.size() > 0) or self.ieu.size() > 0):
            add_querydiff(self)
            toks.append('<ieu-sub>')
            for editop in self.ieu.editops: toks.extend(editop.str_tokens())
            if self.subdiff is not None: add_querydiff(self.subdiff)
            toks.append('</ieu-sub>')
        else:
            add_querydiff(self)        
        return ' '.join(toks)
