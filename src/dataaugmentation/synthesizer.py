from numpy.random import choice, rand
import abc
import utils
from sqlutils import SQL2NL

class ConcurrentEdits:
    def __init__(self):
        self.invalid = set()
        self.invalid.add(('AddOrderDesc', 'RemoveOrderDesc'))
        self.invalid.add(('RemoveLimit', 'RemoveOrderDesc'))
        self.invalid.add(('AddLimitAndOrdering', 'RemoveOrderDesc'))

        self.invalid.add(('RemoveLimit', 'AddOrderDesc'))
        self.invalid.add(('AddLimitAndOrdering', 'AddOrderDesc'))

        self.invalid.add(('RemoveSelectDistinct', 'AddSelectDistinct'))

        self.invalid.add(('AddLimitAndOrdering', 'RemoveLimit'))

        self.invalid.add(('RemoveOrderDesc', 'RemoveLimitAndOrdering'))
        self.invalid.add(('AddOrderDesc', 'RemoveLimitAndOrdering'))
        self.invalid.add(('RemoveLimit', 'RemoveLimitAndOrdering'))
        self.invalid.add(('AddLimitAndOrdering', 'RemoveLimitAndOrdering'))

        self.invalid.add(('RemoveLimit', 'AddLimitAndOrdering'))
        self.invalid.add(('RemoveLimitAndOrdering', 'AddLimitAndOrdering'))
        self.invalid.add(('RemoveOrdering', 'AddLimitAndOrdering'))
        self.invalid.add(('AddOrdering', 'AddLimitAndOrdering'))
        self.invalid.add(('ChangeOrderingColumn', 'AddLimitAndOrdering'))

        self.invalid.add(('RemoveOrderDesc', 'RemoveOrdering'))
        self.invalid.add(('AddOrderDesc', 'RemoveOrdering'))
        self.invalid.add(('RemoveLimit', 'RemoveOrdering'))
        self.invalid.add(('AddLimitAndOrdering', 'RemoveOrdering'))
        self.invalid.add(('AddOrdering', 'RemoveOrdering'))
        self.invalid.add(('ChangeOrderingColumn', 'RemoveOrdering'))

        self.invalid.add(('AddOrderDesc', 'AddOrdering'))
        self.invalid.add(('RemoveOrderDesc', 'AddOrdering'))
        self.invalid.add(('RemoveLimit', 'AddOrdering'))
        self.invalid.add(('AddLimitAndOrdering', 'AddOrdering'))
        self.invalid.add(('RemoveOrdering', 'AddOrdering'))
        self.invalid.add(('ChangeOrderingColumn', 'AddOrdering'))

        self.invalid.add(('RemoveLimit', 'ChangeOrderingColumn'))
        self.invalid.add(('AddLimitAndOrdering', 'ChangeOrderingColumn'))
        self.invalid.add(('AddOrdering', 'ChangeOrderingColumn'))
        self.invalid.add(('ChangeOrderingColumn', 'ChangeOrderingColumn'))

        self.invalid.add(('ChangeFromTable', 'RemoveFromTable'))

        self.invalid.add(('RemoveSelectDistinct', 'RemoveSelectColumn'))
        self.invalid.add(('ChangeFromTable', 'RemoveSelectColumn'))
        self.invalid.add(('RemoveSelectColumn', 'RemoveSelectColumn'))
        self.invalid.add(('AddSelectColumn', 'RemoveSelectColumn'))
        self.invalid.add(('ChangeSelectColumn', 'RemoveSelectColumn'))
        self.invalid.add(('ChangeAggrgSelectColumn', 'RemoveSelectColumn'))

        self.invalid.add(('RemoveSelectDistinct', 'AddSelectColumn'))
        self.invalid.add(('ChangeFromTable', 'AddSelectColumn'))
        self.invalid.add(('RemoveSelectColumn', 'AddSelectColumn'))
        self.invalid.add(('ChangeSelectColumn', 'AddSelectColumn'))
        self.invalid.add(('ChangeAggrgSelectColumn', 'AddSelectColumn'))

        self.invalid.add(('RemoveSelectDistinct', 'ChangeSelectColumn'))
        self.invalid.add(('ChangeFromTable', 'ChangeSelectColumn'))
        self.invalid.add(('RemoveSelectColumn', 'ChangeSelectColumn'))
        self.invalid.add(('AddSelectColumn', 'ChangeSelectColumn'))
        self.invalid.add(('ChangeSelectColumn', 'ChangeSelectColumn'))
        self.invalid.add(('ChangeAggrgSelectColumn', 'ChangeSelectColumn'))

        self.invalid.add(('RemoveSelectDistinct', 'ChangeAggrgSelectColumn'))
        self.invalid.add(('ChangeFromTable', 'ChangeAggrgSelectColumn'))
        self.invalid.add(('RemoveSelectColumn', 'ChangeAggrgSelectColumn'))
        self.invalid.add(('AddSelectColumn', 'ChangeAggrgSelectColumn'))
        self.invalid.add(('ChangeSelectColumn', 'ChangeAggrgSelectColumn'))

        self.invalid.add(('RemoveGroupbyColumn', 'ChangeGroupbyColumn'))

        self.invalid.add(('AddCondition', 'RemoveCondition'))               #no change to where and having at the same time.
        self.invalid.add(('ChangeConditionColumn', 'RemoveCondition'))
        self.invalid.add(('ChangeConditionOperation', 'RemoveCondition'))

        self.invalid.add(('ChangeConditionColumn', 'AddCondition'))
        self.invalid.add(('ChangeConditionOperation', 'AddCondition'))

        self.invalid.add(('ChangeConditionOperation', 'ChangeConditionColumn'))
        
    def valid(self, potential_edit, existing_edits):
        if '+' in potential_edit: potential_edit = potential_edit[:potential_edit.index('+')]
        existing_edits_ = set()
        for edit in existing_edits:
            if '+' in edit:
                existing_edits_.add(edit[:edit.index('+')])
            else:
                existing_edits_.add(edit)

        if potential_edit in existing_edits_: return False #no two edits of the same type.

        for existing in existing_edits_:
            
            if (potential_edit,existing) in self.invalid or (existing,potential_edit) in self.invalid: return False
        return True

concurrentEdits = ConcurrentEdits()

class Editor(metaclass=abc.ABCMeta):
    """
    Directly edit seed.pred_sql and feedback and return it afterwards.
    Can call seed [append_feedback_phrase, get_pred_explanation]
    """
    @abc.abstractmethod
    def is_feasible(self, seed): return
    @abc.abstractmethod
    def apply(self, seed): return

class RemoveOrderDesc(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        if utils.order_in_subquery(seed.pred_sql): return False
        return not seed.pred_sql.orderby.empty and seed.pred_sql.orderby.dir_ == 'desc'
    
    def apply(self, seed):
        seed.pred_sql.orderby.dir_ = 'asc'
        if seed.pred_sql.limit == 1:
            seed.append_feedback_phrase("replace smallest with largest")
        else:
            seed.append_feedback_phrase("it should be descending instead of ascending")
        
        seed.applied_edits.add(self.__class__.__name__)
        return True

class AddOrderDesc(Editor): 

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        if utils.order_in_subquery(seed.pred_sql): return False
        return not seed.pred_sql.orderby.empty and seed.pred_sql.orderby.dir_ == 'asc'
    
    def apply(self, seed):
        seed.pred_sql.orderby.dir_ = 'desc'
        if seed.pred_sql.limit == 1:
            seed.append_feedback_phrase("replace largest with smallest")
        else:
            seed.append_feedback_phrase("it should be ascending instead of descending")
        seed.applied_edits.add(self.__class__.__name__)
        return True

class RemoveSelectDistinct(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        if utils.distinct_in_subquery(seed.pred_sql): return False
        return seed.pred_sql.select.distinct
    
    def apply(self, seed):        
        c = choice(3)
        if c == 0:
            seed.append_feedback_phrase("find unique {}".format(utils.select_column_names(seed.pred_sql.select)))
        elif c == 1:
            seed.append_feedback_phrase("find {} without repetition".format(utils.select_column_names(seed.pred_sql.select)))
        else:
            seed.append_feedback_phrase("find without repetition")

        seed.pred_sql.select.distinct = False    
        seed.applied_edits.add(self.__class__.__name__)
        return True

class AddSelectDistinct(Editor):
  
    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        if seed.pred_sql.find_subquery() is not None: return False
        return not seed.pred_sql.select.distinct
    
    def apply(self, seed):
        seed.pred_sql.select.distinct = True
        seed.append_feedback_phrase("remove without repetition")
        seed.applied_edits.add(self.__class__.__name__)
        return True

class RemoveLimit(Editor):
    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        if utils.limit_in_subquery(seed.pred_sql): return False
        if seed.pred_sql.limit is None: return False
        if not seed.pred_sql.orderby.empty and seed.pred_sql.limit ==1:
            return len(seed.pred_sql.orderby.vals) == 1
        return True
    
    def apply(self, seed):
        if not seed.pred_sql.orderby.empty and seed.pred_sql.limit ==1:
            orderdir = 'smallest' if seed.pred_sql.orderby.dir_ == 'asc' else 'largest'            
            seed.append_feedback_phrase("find the {} {}".format(orderdir, utils.order_column_names(seed.pred_sql.orderby)))
        else:
            seed.append_feedback_phrase("only top {} rows are needed".format(seed.pred_sql.limit))
        seed.pred_sql.limit = None
        seed.applied_edits.add(self.__class__.__name__)
        return True

class RemoveLimitAndOrdering(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        if utils.limit_in_subquery(seed.pred_sql): return False
        return seed.pred_sql.limit == 1 and not seed.pred_sql.orderby.empty and len(seed.pred_sql.orderby.vals) ==1
    
    def apply(self, seed):
        orderdir = 'smallest' if seed.pred_sql.orderby.dir_ == 'asc' else 'largest'            
        seed.append_feedback_phrase("find the {} {}".format(orderdir, utils.order_column_names(seed.pred_sql.orderby)))

        removed_cols = set()
        seed.pred_sql.orderby._list_columns(removed_cols)        
        seed.pred_sql.limit = None
        seed.pred_sql.orderby.empty = True
        seed.pred_sql.orderby.dir_ = 'asc'
        seed.pred_sql.orderby.vals = []
        for col in removed_cols: utils.update_tables_removedcol(seed.pred_sql, col)
        seed.applied_edits.add(self.__class__.__name__)
        return True

class AddLimitAndOrdering(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return seed.pred_sql.limit is None and seed.pred_sql.orderby.empty
    
    def apply(self, seed):

        orderdir = choice(['asc', 'desc'])        
        orderdir_ = 'smallest' if orderdir == 'asc' else 'largest'
        col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True)
        if col_id is None: return False
        attempts = 0
        while col_id in seed.banned_cols:
            col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True)
            attempts += 1
            if attempts == 10: return False

        colname = utils.column_name(col_id, seed.db_id)        

        seed.pred_sql.orderby.dir_ = orderdir
        seed.pred_sql.orderby.empty = False
        seed.pred_sql.orderby.vals = [utils.valunit_from_colid(col_id, seed.db_id)]
        seed.pred_sql.limit = 1
        seed.append_feedback_phrase("no need to find the {} {}".format(orderdir_, colname))     
        seed.applied_edits.add(self.__class__.__name__)   
        return True
    
class RemoveOrdering(Editor):   

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return seed.pred_sql.limit is None and not seed.pred_sql.orderby.empty and len(seed.pred_sql.orderby.vals) == 1
    
    def apply(self, seed):
        orderdir = 'ascending' if seed.pred_sql.orderby.dir_ == 'asc' else 'descending'
        col_id = seed.pred_sql.orderby.vals[0].unit1.col_id
        colname = utils.column_name(col_id, seed.db_id)
        seed.append_feedback_phrase("order them {} order of their {}".format(orderdir, colname))
        seed.pred_sql.orderby.empty = True
        seed.pred_sql.orderby.dir_ = 'asc'
        seed.pred_sql.orderby.vals = []
        
        utils.update_tables_removedcol(seed.pred_sql, col_id)
        seed.applied_edits.add(self.__class__.__name__)
        return True

class AddOrdering(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight
 
    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return seed.pred_sql.limit is None and seed.pred_sql.orderby.empty
    
    def apply(self, seed):        
        col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True)        
        if col_id is None: return False
        attempts = 0
        while col_id in seed.banned_cols:
            col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True)        
            attempts +=1
            if attempts == 10: return False


        orderdir = choice(['asc', 'desc'])        
        seed.pred_sql.orderby.dir_ = orderdir
        seed.pred_sql.orderby.empty = False
        seed.pred_sql.orderby.vals = [utils.valunit_from_colid(col_id, seed.db_id)]
        seed.append_feedback_phrase("no need for ordering")
        seed.applied_edits.add(self.__class__.__name__)
        return True

class ChangeOrderingColumn(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return seed.pred_sql.limit is None and not seed.pred_sql.orderby.empty and len(seed.pred_sql.orderby.vals) == 1

    def apply(self, seed):
        old_col_id = seed.pred_sql.orderby.vals[0].unit1.col_id
        new_col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=[old_col_id])
        if new_col_id is None: return False
        used_cols = seed.pred_sql.list_columns()
        attempts = 0
        while new_col_id in seed.banned_cols or new_col_id in used_cols:
            new_col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=[old_col_id])
            attempts += 1
            if attempts == 10: return False

        new_col_name = utils.column_name(new_col_id, seed.db_id)
        old_col_name = utils.column_name(old_col_id, seed.db_id)

        if choice(2) == 0:
            seed.append_feedback_phrase("you should order using {}".format(old_col_name))            
        else:
            seed.append_feedback_phrase("replace {} with {}".format(new_col_name,old_col_name))
        
        
        seed.pred_sql.orderby.vals[0].unit1.col_id = new_col_id
        utils.update_tables_removedcol(seed.pred_sql, old_col_id)
        seed.applied_edits.add(self.__class__.__name__)
        return True

class RemoveFromTable(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        len(utils.get_joinonly_tables(seed.pred_sql)) > 0
    
    def apply(self, seed):
        join_tables = utils.get_joinonly_tables(seed.pred_sql)
        removed_table_id = int(choice(join_tables))
        seed.banned_tabs.add(removed_table_id)
        tablename = utils.table_name(removed_table_id, seed.db_id)
        utils.remove_table(seed.pred_sql, tab_id)
        seed.append_feedback_phrase("you need to use {} table".format(tablename))
        seed.applied_edits.add(self.__class__.__name__)
        return True

class ChangeFromTable(Editor): 

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    """
    For now limit to count(*) from one table.
    Any column in the removed table (where, groupby, orderby, having) should be replace with column from new table. for up to 1 edit.
        for now limiting to only the queries that do not use any columns.
    """
    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return len(seed.pred_sql.from_.tables) == 1 and type(seed.pred_sql.from_.tables[0].table) == int\
            and all([col == 0 for col in seed.pred_sql.list_columns(skipjoin=True, skipsub=True)])
    
    def apply(self, seed):
        old_table = seed.pred_sql.from_.tables[0].table
        new_table = utils.sample_table(seed.db_id, exclude=[old_table])        

        attempts = 0
        while new_table in seed.banned_tabs:
            new_table = utils.sample_table(seed.db_id, exclude=[old_table])
            attempts += 1
            if attempts == 10: return False

        oldtable_name = utils.table_name(old_table, seed.db_id)
        newtable_name = utils.table_name(new_table, seed.db_id)
        if choice(2) == 0:
            seed.append_feedback_phrase("you need to use {} table".format(oldtable_name))
        else:
            seed.append_feedback_phrase("use {} instead of {} table".format(newtable_name,oldtable_name))

        seed.pred_sql.from_.tables[0].table = new_table #no join conds.
        seed.applied_edits.add(self.__class__.__name__)
        return True

class RemoveSelectColumn(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        len(seed.pred_sql.select.list_cols()) > 1
    
    def apply(self, seed):
        removed_col_ix = choice(len(seed.pred_sql.select.list_cols()))
        col_expln = utils.explain_valunit(seed.pred_sql.select.valunits[removed_col_ix],
                                                seed.pred_sql.select.aggs[removed_col_ix])
        removed_col_id = seed.pred_sql.select.valunits[removed_col_ix].unit1.col_id
        seed.pred_sql.select.valunits = utils.remove_index_from_list(seed.pred_sql.select.valunits, removed_col_ix)
        seed.pred_sql.select.aggs = utils.remove_index_from_list(seed.pred_sql.select.aggs, removed_col_ix)        
                
        seed.append_feedback_phrase("also find {}".format(col_expln))
        utils.update_tables_removedcol(seed.pred_sql, removed_col_id)
        seed.applied_edits.add(self.__class__.__name__)
        return True

class AddSelectColumn(Editor):    

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return all(agg == 0 for agg in seed.pred_sql.select.aggs)
    
    def apply(self, seed):        
        added_col_id = utils.sample_column_from_current_tables(seed.pred_sql,  used_tables=True,
                        exclude=seed.pred_sql.list_columns(skipjoin=True))
        if added_col_id is None: return False
        attempts = 0
        while added_col_id in seed.banned_cols:
            added_col_id = utils.sample_column_from_current_tables(seed.pred_sql,  used_tables=True,
                            exclude=seed.pred_sql.list_columns(skipjoin=True))
            attempts += 1
            if attempts == 10: return False

        col_name = utils.column_name(added_col_id, seed.db_id, fullname=choice([False, True]))

        seed.append_feedback_phrase("remove {}".format(col_name))
        seed.pred_sql.select.valunits.append(utils.valunit_from_colid(added_col_id,seed.db_id))
        seed.pred_sql.select.aggs.append(0)
        seed.applied_edits.add(self.__class__.__name__)
        return True

class ChangeSelectColumn(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        cols = seed.pred_sql.select.list_cols()
        return any([c != 0 and not utils.col_used_outside_select(seed.pred_sql, c) for c in cols]) and all(agg == 0 for agg in seed.pred_sql.select.aggs)
    
    def apply(self, seed):
        cols = seed.pred_sql.select.list_cols()
        to_remove_ix = choice(len(cols))
        attempts = 0
        while cols[to_remove_ix] == 0 or utils.col_used_outside_select(seed.pred_sql, cols[to_remove_ix]):
            to_remove_ix = choice(len(cols))
            attempts += 1
            if attempts == 10: return False
        
        to_add = utils.sample_column_from_current_tables(seed.pred_sql,  used_tables=True, exclude=cols)
        if to_add is None: return False
        attempts = 0
        while to_add in seed.banned_cols:
            to_add = utils.sample_column_from_current_tables(seed.pred_sql,  used_tables=True, exclude=cols)
            attempts += 1
            if attempts == 10: return False


        removecolname = utils.column_name(cols[to_remove_ix],  seed.db_id, fullname=False)
        addcolname = utils.column_name(to_add,  seed.db_id, fullname=False)
        
        if choice(2) == 0:
            seed.append_feedback_phrase("replace {} with {}".format(addcolname, removecolname))
        else:
            seed.append_feedback_phrase("you should find {}".format(removecolname))
        
        removed_col_id = seed.pred_sql.select.valunits[to_remove_ix].unit1.col_id
        seed.banned_cols.add(removed_col_id)                
        seed.pred_sql.select.valunits[to_remove_ix].unit1.col_id = to_add
        utils.update_tables_removedcol(seed.pred_sql, removed_col_id)
        seed.applied_edits.add(self.__class__.__name__)
        return True


class ChangeAggrgSelectColumn(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    #either the aggreg only or the aggreg+col
    def is_feasible(self, seed):   
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return any([agg != 0 for agg in seed.pred_sql.select.aggs])

    def apply(self, seed):
        agg_indices = [i for i, agg in enumerate(seed.pred_sql.select.aggs) if agg != 0]
        to_remove_ix = choice(agg_indices)
        old_agg = seed.pred_sql.select.aggs[to_remove_ix]
        new_agg = utils.sample_agg(exclude=[old_agg])
        old_col = seed.pred_sql.select.valunits[to_remove_ix].unit1.col_id        
        replace_col = choice([True, False])
        if replace_col:            
            new_col = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=seed.pred_sql.select.list_cols())
            if new_col is None:
                new_col = old_col
        else:
            new_col = old_col        
        old_agg_str = SQL2NL.AGG_OPS[old_agg]
        new_agg_str = SQL2NL.AGG_OPS[new_agg]
        old_col_str = utils.column_name(old_col, seed.db_id)
        new_col_str = utils.column_name(new_col, seed.db_id)
        seed.append_feedback_phrase("replace {} {} with {} {}".format(new_agg_str, new_col_str, old_agg_str, old_col_str))
        seed.pred_sql.select.aggs[to_remove_ix] = new_agg
        seed.pred_sql.select.valunits[to_remove_ix].unit1.col_id  = new_col
        seed.applied_edits.add(self.__class__.__name__)
        return True

class ChangeGroupbyColumn(Editor):

    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return not seed.pred_sql.groupby.empty and len(seed.pred_sql.groupby.cols) == 1
    
    def apply(self, seed):
        old_col_id = seed.pred_sql.groupby.cols[0].col_id
        new_col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=[old_col_id])
        if new_col_id is None: return False
        attempts = 0
        while new_col_id in seed.banned_cols:
            new_col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=[old_col_id])
            attempts += 1
            if attempts == 10: return False

        old_col_name = utils.column_name(old_col_id, seed.db_id)
        new_col_name = utils.column_name(new_col_id, seed.db_id)

        if choice(2) == 0:
            seed.append_feedback_phrase("find for each {}".format(old_col_name))
        else:
            seed.append_feedback_phrase("replace {} with {}".format(new_col_name, old_col_name))

        seed.pred_sql.groupby.cols[0].col_id = new_col_id
        select_cols = seed.pred_sql.select.list_cols()
        try:
            ix_in_select = select_cols.index(old_col_id)
            seed.pred_sql.select.valunits[ix_in_select].unit1.col_id = new_col_id
        except ValueError:
            pass
        seed.banned_cols.add(old_col_id)
        seed.banned_cols.add(new_col_id)
        utils.update_tables_removedcol(seed.pred_sql, old_col_id)
        seed.applied_edits.add(self.__class__.__name__)
        return True
    
class RemoveGroupbyColumn(Editor):
    def __init__(self, sampling_weight=1):
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        return not seed.pred_sql.groupby.empty and len(seed.pred_sql.groupby.cols) == 1
    
    def apply(self, seed):
        col_id = seed.pred_sql.groupby.cols[0].col_id
        colname = utils.column_name(col_id, seed.db_id)
        seed.append_feedback_phrase("find for each {}".format(colname))
        seed.pred_sql.groupby.empty = True
        seed.pred_sql.groupby.cols = []
        utils.update_tables_removedcol(seed.pred_sql, col_id)
        seed.applied_edits.add(self.__class__.__name__)
        return True

class RemoveCondition(Editor):

    def __init__(self, type_='where',sampling_weight=1):
        self.type_ = type_
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        return len(condclause.conds) > 0
    
    def apply(self, seed):
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        remove_ix = choice(len(condclause.conds))

        attempts = 0
        while utils.nested_in_cond(condclause.conds[remove_ix]):
            remove_ix = choice(len(condclause.conds))
            attempts += 1
            if attempts == 10: return False
        
        cond_expln = utils.explain_condition(condclause.conds[remove_ix])
        removed_col_id = condclause.conds[remove_ix].valunit.unit1.col_id

        seed.append_feedback_phrase("confirm that {}".format(cond_expln))

        condclause.conds = utils.remove_index_from_list(condclause.conds, remove_ix)
        if len(seed.pred_sql.where.andor) > 0:
            condclause.andor = utils.remove_index_from_list(condclause.andor, max(0,remove_ix-1))
        
        if len(condclause.conds) == 0: condclause.empty = True

        utils.update_tables_removedcol(seed.pred_sql, removed_col_id)
        seed.applied_edits.add(self.__class__.__name__+f'+{self.type_}')
        return True

class AddCondition(Editor):
    """
    select: _cols_, from: _table_, groupby: _cols_, having: _op_, _agg_ _col_, _val_

    select: _cols_, from: _table_, _table_ with join cond, groupby: _cols_, having: _op_, _agg_ _col_, _val_

    """
    def __init__(self, type_='where',sampling_weight=1):
        self.type_ = type_
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        if self.type_ == 'where':
            return len(condclause.conds) < 3
        else:
            return len(condclause.conds) == 0 and len(seed.pred_sql.where.conds) == 0 and not seed.pred_sql.groupby.empty
    
    def apply(self, seed):
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        existing_where_cols = set()
        condclause._list_columns(existing_where_cols, skipsub=True)
        col_id = utils.sample_column_from_current_tables(seed.pred_sql,  used_tables=True, exclude=existing_where_cols)
        if col_id is None: return False
        attempts = 0
        while col_id in seed.banned_cols:
            col_id = utils.sample_column_from_current_tables(seed.pred_sql,  used_tables=True, exclude=existing_where_cols)
            attempts += 1
            if attempts == 10: return False

        op_id = int(choice(len(SQL2NL.WHERE_OPS)))
        while op_id in [8, 10,11]: #in, is, exists
            op_id = int(choice(len(SQL2NL.WHERE_OPS)))
        
        agg_id = None
        if self.type_ == 'having':
            agg_id = int(choice(5)) + 1

        condunit = utils.get_condunit(col_id, op_id, seed.db_id, agg_id=agg_id)
        condclause.conds.append(condunit)
        if len(condclause.conds) > 1: condclause.andor.append('and')        
        condclause.empty = False
        cond_expln = utils.explain_condition(condunit)
        seed.append_feedback_phrase("remove {}".format(cond_expln))
        seed.applied_edits.add(self.__class__.__name__+f'+{self.type_}')
        return True


class ChangeConditionColumn(Editor):

    def __init__(self, type_='where',sampling_weight=1):
        self.type_ = type_
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        return len(condclause.conds) > 0 
    
    def apply(self, seed):
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having        

        existing_where_cols = set()
        condclause._list_columns(existing_where_cols)

        cond_ix = choice(len(condclause.conds))
        old_col_id = condclause.conds[cond_ix].valunit.unit1.col_id
        new_col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=list(existing_where_cols))
        if new_col_id is None: return False
        attempts = 0
        while new_col_id in seed.banned_cols:
            new_col_id = utils.sample_column_from_current_tables(seed.pred_sql, used_tables=True, exclude=list(existing_where_cols))
            attempts += 1
            if attempts == 10: return False

        old_col_name = utils.column_name(old_col_id, seed.db_id, fullname=False)
        new_col_name = utils.column_name(new_col_id, seed.db_id, fullname=False)
        
        if utils.col_used_outside_condition(seed.pred_sql, new_col_id, type_=self.type_) or choice(2)== 0:
            cond_expln = utils.explain_condition(condclause.conds[cond_ix])
            seed.append_feedback_phrase("it should be {}".format(cond_expln))
        else:
            seed.append_feedback_phrase("replace {} with {}".format(new_col_name, old_col_name))

        removed_col_id = condclause.conds[cond_ix].valunit.unit1.col_id
        condclause.conds[cond_ix].valunit.unit1.col_id = new_col_id
        utils.update_tables_removedcol(seed.pred_sql, removed_col_id)

        seed.applied_edits.add(self.__class__.__name__+f'+{self.type_}')
        return True
                 
class ChangeConditionOperation(Editor):

    def __init__(self, type_='where',sampling_weight=1):
        self.type_ = type_
        self.sampling_weight = sampling_weight

    def is_feasible(self, seed):
        if not concurrentEdits.valid(self.__class__.__name__, seed.applied_edits): return False
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        return len(condclause.conds) > 0 
    
    def apply(self, seed):
        condclause = seed.pred_sql.where if self.type_ == 'where' else seed.pred_sql.having
        cond_ix = choice(len(condclause.conds))
        old_not = condclause.conds[cond_ix].not_
        old_op_id = condclause.conds[cond_ix].op_id

        new_op_id = int(choice(len(SQL2NL.WHERE_OPS)))
        while new_op_id in [8, 10,11, old_op_id]: #in, is, exists
            new_op_id = int(choice(len(SQL2NL.WHERE_OPS)))
                        
        all_ops = [cond.op_id for cond in condclause.conds] + [cond.op_id for cond in seed.pred_sql.having.conds]
        subquery = seed.pred_sql.find_subquery()

        if old_op_id not in all_ops and subquery is None:
            seed.append_feedback_phrase("replace {} with {}".format(
                SQL2NL.WHERE_OPS[new_op_id], SQL2NL.WHERE_OPS[old_op_id]))
        else:
            cond_expln = utils.explain_condition(condclause.conds[cond_ix])
            seed.append_feedback_phrase("it should be {}".format(cond_expln))            
        
        condclause.conds[cond_ix].op_id = new_op_id
        seed.applied_edits.add(self.__class__.__name__+f'+{self.type_}')
        return True

class Synthesizer:
    def __init__(self, configs):
        self.default_num_clones = configs['default_num_clones']
        self.configs = configs
        num_edits = []
        probs = []
        for k, v in configs['num_edits_probs'].items():
            num_edits.append(int(k))
            probs.append(v)
        self.sample_num_edits = lambda : choice(num_edits, p=probs)

        self.drop_feedback = lambda: rand() < configs['drop_feedback_prob']
        
        sampling_weights = configs['editor_sampling_weights']
        self.editors = []
        self.editor_str = []
        self.editors.append(RemoveOrderDesc(sampling_weight=sampling_weights['RemoveOrderDesc'])); self.editor_str.append('RemoveOrderDesc')
        self.editors.append(RemoveLimit(sampling_weight=sampling_weights['RemoveLimit'])); self.editor_str.append('RemoveLimit')
        self.editors.append(RemoveLimitAndOrdering(sampling_weight=sampling_weights['RemoveLimitAndOrdering'])); self.editor_str.append('RemoveLimitAndOrdering')
        self.editors.append(AddLimitAndOrdering(sampling_weight=sampling_weights['AddLimitAndOrdering'])); self.editor_str.append('AddLimitAndOrdering')
        self.editors.append(AddOrderDesc(sampling_weight=sampling_weights['AddOrderDesc'])); self.editor_str.append('AddOrderDesc')
        self.editors.append(RemoveSelectDistinct(sampling_weight=sampling_weights['RemoveSelectDistinct'])); self.editor_str.append('RemoveSelectDistinct')
        self.editors.append(AddSelectDistinct(sampling_weight=sampling_weights['AddSelectDistinct'])); self.editor_str.append('AddSelectDistinct')
        self.editors.append(RemoveOrdering(sampling_weight=sampling_weights['RemoveOrdering'])); self.editor_str.append('RemoveOrdering')
        self.editors.append(AddOrdering(sampling_weight=sampling_weights['AddOrdering'])); self.editor_str.append('AddOrdering')
        self.editors.append(ChangeOrderingColumn(sampling_weight=sampling_weights['ChangeOrderingColumn'])); self.editor_str.append('ChangeOrderingColumn')
        self.editors.append(RemoveFromTable(sampling_weight=sampling_weights['RemoveFromTable'])); self.editor_str.append('RemoveFromTable')
        self.editors.append(ChangeFromTable(sampling_weight=sampling_weights['ChangeFromTable'])); self.editor_str.append('ChangeFromTable')
        self.editors.append(RemoveSelectColumn(sampling_weight=sampling_weights['RemoveSelectColumn'])); self.editor_str.append('RemoveSelectColumn')
        self.editors.append(AddSelectColumn(sampling_weight=sampling_weights['AddSelectColumn'])); self.editor_str.append('AddSelectColumn')
        self.editors.append(ChangeSelectColumn(sampling_weight=sampling_weights['ChangeSelectColumn'])); self.editor_str.append('ChangeSelectColumn')
        self.editors.append(ChangeAggrgSelectColumn(sampling_weight=sampling_weights['ChangeAggrgSelectColumn'])); self.editor_str.append('ChangeAggrgSelectColumn')
        self.editors.append(ChangeGroupbyColumn(sampling_weight=sampling_weights['ChangeGroupbyColumn'])); self.editor_str.append('ChangeGroupbyColumn')
        self.editors.append(RemoveGroupbyColumn(sampling_weight=sampling_weights['RemoveGroupbyColumn'])); self.editor_str.append('RemoveGroupbyColumn')
        self.editors.append(RemoveCondition(type_="where", sampling_weight=sampling_weights['RemoveCondition+where'])); self.editor_str.append('RemoveCondition+where')
        self.editors.append(AddCondition(type_="where", sampling_weight=sampling_weights['AddCondition+where'])); self.editor_str.append('AddCondition+where')
        self.editors.append(ChangeConditionColumn(type_="where", sampling_weight=sampling_weights['ChangeConditionColumn+where'])); self.editor_str.append('ChangeConditionColumn+where')
        self.editors.append(ChangeConditionOperation(type_="where", sampling_weight=sampling_weights['ChangeConditionOperation+where'])); self.editor_str.append('ChangeConditionOperation+where')
        self.editors.append(RemoveCondition(type_="having", sampling_weight=sampling_weights['RemoveCondition+having'])); self.editor_str.append('RemoveCondition+having')
        self.editors.append(AddCondition(type_="having", sampling_weight=sampling_weights['AddCondition+having'])); self.editor_str.append('AddCondition+having')
        self.editors.append(ChangeConditionColumn(type_="having", sampling_weight=sampling_weights['ChangeConditionColumn+having'])); self.editor_str.append('ChangeConditionColumn+having')
        self.editors.append(ChangeConditionOperation(type_="having", sampling_weight=sampling_weights['ChangeConditionOperation+having'])); self.editor_str.append('ChangeConditionOperation+having')
        
        #TODO: add editors that remove whole subquery or adds a new one.
        
        #stats
        self.attempted_clones = 0
        self.successfully_synthesized = 0
        self.error_empty_feedback = 0
        self.error_unexplained = 0
        self.duplicate_skipped = 0

            
    def _editor_from_str(self, editor):
        return self.editors[self.editor_str.index(editor)]

    def print_stats(self):
        print('='*20, 'Synthesis Stats', '='*20)
        print('Attempted clones', self.attempted_clones)
        print('Successfully synthesized', self.successfully_synthesized)
        print('Error empty feedback', self.error_empty_feedback)
        print('Error unexplainable SQL', self.error_unexplained)
        print('Duplicate Skipped', self.duplicate_skipped)
        

    def expand_seed(self, seed, sql2nl):
        finalized_clones = {} #feedback > clone .. used to avoid duplicate feedback
        for _ in range(self._num_clones(seed)):
            clone = seed.clone()
            self.attempted_clones += 1
            num_edits = self.sample_num_edits()            
            for edit_ix in range(num_edits):
                try:
                    isedited = self._edit(clone)                
                    #simulating incomplete feedback
                    if isedited and num_edits > 1 and self.drop_feedback():
                        clone.pop_feedback_phrase()                    
                except ValueError as e:
                    #print('Synthesis Error: {}'.format(e))
                    pass
                    
                            
            if len(clone.feedback) == 0:
                self.error_empty_feedback += 1
                continue
            elif not sql2nl.is_supported(clone.pred_sql) or str(clone.pred_sql).count('select') > 3:                
                self.error_unexplained += 1
                continue
            else:
                feedback = clone._gen_feedback(randomize=False)
                if feedback in finalized_clones:
                    self.duplicate_skipped += 1
                else:
                    finalized_clones[feedback] = clone
                    self.successfully_synthesized += 1
        return finalized_clones.values()

    def _edit(self, seed):
        isedited = False
        #TODO: refer to steps.
        feasible_edits = self._feasible_edits(seed)
        if all([edit.sampling_weight == 0 for edit in feasible_edits]): return False
        if len(feasible_edits) > 0:
            attemps = 0
            while not isedited and attemps < 10:
                edit = choice(feasible_edits, p=utils.weights_to_probs([edit.sampling_weight for edit in feasible_edits]))
                isedited = edit.apply(seed)  #For now, applying only to main.
                attemps += 1
        return isedited

    def _feasible_edits(self, seed):
        return [editor for editor in self.editors if editor.is_feasible(seed)]

    def _num_clones(self, seed):
        num_clones = self.default_num_clones
        for feasibility in self.configs['num_clones_if_feasible']:
            if all([self._editor_from_str(editor).is_feasible(seed) for editor in feasibility['editors']]):
                num_clones = max(num_clones, feasibility['num_clones'])

        return num_clones
