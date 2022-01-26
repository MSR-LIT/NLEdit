from collections import Counter
import pathlib
from os import path

ADD_SUBQUERY_BOUNDARIES = False
IEU_START = '<ieu-sub>'
IEU_END = '</ieu-sub>'
NESTED_START = '<nested-sub>'
NESTED_END = '</nested-sub>'


#               0         1      2    3    4     6    6      7    8       9     10      11
#WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')          
WHERE_OPS = ['not', 'is between', 'equals', 'greater than', 'less than',\
         'greater than or equals', 'less than or equals', 'not equals', 'one of', 'contains', 'is', 'exists']

AGG_OPS = ['', 'maximum', 'minimum', 'number of', 'summation of', 'average']
AGG_OPS2 = ['', 'maximum', 'minimum', 'number of', 'sum of', 'average'] 

#('none', '-', '+', "*", '/')
UNIT_OPS = ['', 'minus', 'plus', 'multiplied by', 'divided by']

class SQL2NL:

    def __init__(self, schema_utils, patterns_file=None):
        if patterns_file is None:
            patterns_file = path.join(pathlib.Path(__file__).parent.resolve(), 'PATTERN_TO_PROCESSOR.TSV')
        # pattern \tab processor_name
        with open(patterns_file) as inh:
            self.PATTERN_PROCESSOR = {}
            for ln in inh:
                pattern, processor = ln.strip().split('\t')
                #TODO: Clean! hacky for now .. later update the file
                pattern = pattern.replace(' with join cond', '')
                self.PATTERN_PROCESSOR[pattern] = processor
        self.schema_utils = schema_utils
        self.current_db = None
        self.tname = lambda tid: tid if isinstance(tid, str) else self.schema_utils.get_table_name(self.current_db, tid)+' table'

    def cname(self, colid, disambiguate=True):
        #self.
        col_name = self.schema_utils.get_column_name(self.current_db, colid)
        if disambiguate and col_name in self.current_ambiguous_names:
            return "{}'s {}".format(\
            self.schema_utils.get_table_name(self.current_db, self.schema_utils.get_table_of_col(self.current_db, colid)),\
            col_name)        
        return col_name

    def is_supported(self, parsedSQL, debug=False):
        if parsedSQL.intersect and \
            str(parsedSQL.intersect) not in self.PATTERN_PROCESSOR:
            if debug: print(parsedSQL.intersect)
            return False
            
        if parsedSQL.union and \
            str(parsedSQL.union) not in self.PATTERN_PROCESSOR:
            if debug: print(parsedSQL.union)
            return False

        if parsedSQL.except_ and \
            str(parsedSQL.except_) not in self.PATTERN_PROCESSOR:
            if debug: print(parsedSQL.except_)
            return False
        
        if not str(parsedSQL) in self.PATTERN_PROCESSOR:
            if debug: print(parsedSQL)
            return False
        
        return True

    def _get_processor(self, parsedSQL):
        pattern = str(parsedSQL)
        if pattern not in self.PATTERN_PROCESSOR:            
            return None
        return getattr(self, self.PATTERN_PROCESSOR[pattern])

    def _hacky_post_processor(self, step):
        replacements = [\
                    (' rows of the corresponding rows ',' the corresponding rows '), \
                    (' 1 rows ', ' row ')]
        for find, replace in replacements:
            step = step.replace(find, replace)
        
        #find the Reviewer's rID of Reviewer table  -> if one table no need to disambugate
        parts = step.split()
        if parts[-1] == 'table' and parts[-3] == 'of':
            table_name = parts[-2]
            step = step.replace(" {}'s ".format(table_name),' ')

        #contains %xx .. ends with // contains xx% .. starts with
        if 'contains' in parts:
            following = parts[parts.index('contains') + 1]
            if following[0] in ['"',"'"] and following[-1] in ['"', "'"]:
                following_ = following[1:-1]
            else:
                following_ = following
            
            if len(following_) == 0: return step
            if following_[0] == '%' and following_[-1] != '%':
                #ends with
                step = step.replace('contains '+following, 'ends with '+following_[1:])
            elif following_[-1] == '%' and following_[0] != '%':
                #starts with
                step = step.replace('contains '+following, 'starts with '+following_[:-1])
        return step

    def _remove_duplicate_steps(self, steps):        
        # repeated joins
        if len(steps) > len(set(steps)) and \
            steps[0].startswith('For each row in'):
            output_steps = [steps[0]]
            found_index = -1
            for ix, step in enumerate(steps[1:]):
                if step == steps[0]:
                    found_index = ix + 2
                elif found_index == -1:
                    output_steps.append(step)
                else:
                    #fixing set
                    step = step.replace(\
                        'the results of step {}'.format(found_index),'the results of step 1')
                    #any mention of step after found_index .. subtract 1 from step index
                    sstr_ix = 0
                    while True:                        
                        try:
                            sstr_ix = step.index('the results of step ', sstr_ix)+20                            
                            #assuming step is one digit
                            step_num = int(step[sstr_ix])
                            if step_num > found_index:
                                step = step.replace(\
                                    'the results of step {}'.format(step_num),'the results of step {}'.format(step_num-1))
                        except ValueError as e:
                            assert str(e) == 'substring not found'
                            break

                    output_steps.append(step)
            return output_steps
        return steps

    def _remove_duplicate_aggr_steps(self, steps):

        if len(steps) == len(set(steps)): return steps
        output_steps = []
        first_found_index = -1
        second_found_index = -1
        for ix, step in enumerate(steps):
            if step.startswith('find the number of rows of each value of'):
                if first_found_index == -1:
                    output_steps.append(step)
                    first_found_index = ix + 1
                else:
                    second_found_index = ix + 1
            elif second_found_index != -1:
                #replace
                step = step.replace(\
                    'whose corresponding value in step {}'.format(second_found_index),\
                    'whose corresponding value in step {}'.format(first_found_index))
                sstr_ix = 0
                while True:                        
                    try:
                        sstr_ix = step.index('the results of step ', sstr_ix)+19
                        #assuming step is one digit
                        step_num = int(step[sstr_ix])
                        if step_num > second_found_index:
                            step = step.replace(\
                                'the results of step {}'.format(step_num),'the results of step {}'.format(step_num-1))
                    except:
                        break

                output_steps.append(step)
            else:
                output_steps.append(step)
        return output_steps


    def _push_steps_down(self, step, prev_steps):
        sstr_ix = 0
        while True:
            try:
                sstr_ix = step.index('step ', sstr_ix)+5
                if '1' <= step[sstr_ix] <= '9':
                    step_num = int(step[sstr_ix])
                    step = step.replace('step {}'.format(step_num),'step {}'.format(step_num+prev_steps))
            except:
                break
        return step

    def _ambiguous_names(self, parsedSQL):
        tables = parsedSQL.list_tables()
        skip = set()
        col_name_count = Counter()
        for table in tables:
            cols = self.schema_utils.get_columns_of_table(self.current_db, table)
            for col in cols:
                col_name = self.schema_utils.get_column_name(self.current_db, col)
                if col_name not in col_name_count:
                    col_name_count[col_name] = []
                col_name_count[col_name].append(col)

        ambiguous_names = set()
        fk_pk = self.schema_utils.list_pk_fk(self.current_db)
        for name, cols in col_name_count.items():
            for fk, pk in fk_pk:
                if len(cols) == 2 and fk in cols and pk in cols:
                    pass
                elif len(cols) > 1:
                    ambiguous_names.add(name)

        return ambiguous_names

    def get_nl(self, parsedSQL):
        self.current_db = parsedSQL.get_db()
        #intersect, union, except, limit are not part of the pattern        
        processor = self._get_processor(parsedSQL)
        if processor is None:
            assert False, 'processor not found - {}'.format(parsedSQL)

        self.current_ambiguous_names = self._ambiguous_names(parsedSQL)

        nl = processor(parsedSQL)
        
        if parsedSQL.intersect:         
            processor = self._get_processor(parsedSQL.intersect)
            if processor is None: assert False,'unknown intersect pattern!'
                        
            intersect_nl = processor(parsedSQL.intersect)

            if parsedSQL.intersect.limit:
                intersect_nl.append('only keep the first {} rows of the results of step {}'.format(\
                    parsedSQL.intersect.limit, len(intersect_nl)))        

            if ADD_SUBQUERY_BOUNDARIES:
                intersect_nl[0] = f'{IEU_START} {intersect_nl[0]}'                
                nl += [self._push_steps_down(step, len(nl)) for step in intersect_nl] + ['show the rows that are in both the '+ \
                    'results of step {} and the results of step {} {}'.format( \
                        len(nl), len(nl) + len(intersect_nl), IEU_END)]
            else:
                nl += [self._push_steps_down(step, len(nl)) for step in intersect_nl] + ['show the rows that are in both the '+ \
                    'results of step {} and the results of step {}'.format( \
                        len(nl), len(nl) + len(intersect_nl))]

        if parsedSQL.union:
            processor = self._get_processor(parsedSQL.union)
            if processor is None: assert False,'unknown union pattern!'
            union_nl = processor(parsedSQL.union)   

            if parsedSQL.union.limit:
                union_nl.append('only keep the first {} rows of the results of step {}'.format(\
                    parsedSQL.union.limit, len(union_nl)))        

            if ADD_SUBQUERY_BOUNDARIES:
                union_nl[0] = f'{IEU_START} {union_nl[0]}'                
                nl += [self._push_steps_down(step, len(nl)) for step in union_nl] + ['show the rows that are in any of the '+ \
                    'results of step {} or the results of step {} {}'.format( \
                        len(nl), len(nl) + len(union_nl), IEU_END)]
            else:
                nl += [self._push_steps_down(step, len(nl)) for step in union_nl] + ['show the rows that are in any of the '+ \
                    'results of step {} or the results of step {}'.format( \
                        len(nl), len(nl) + len(union_nl))]

        if parsedSQL.except_:
            processor = self._get_processor(parsedSQL.except_)
            if processor is None: assert False,'unknown except pattern!'
            except_nl = processor(parsedSQL.except_)

            if parsedSQL.except_.limit:
                except_nl.append('only keep the first {} rows of the results of step {}'.format(\
                    parsedSQL.except_.limit, len(except_nl)))        

            if ADD_SUBQUERY_BOUNDARIES:
                except_nl[0] = f'{IEU_START} {except_nl[0]}'                
                nl += [self._push_steps_down(step, len(nl)) for step in except_nl] + ['show the rows that are in the results '+ \
                    'of step {} but not in the results of step {} {}'.format( \
                        len(nl), len(nl) + len(except_nl), IEU_END)]
            else:
                nl += [self._push_steps_down(step, len(nl)) for step in except_nl] + ['show the rows that are in the results '+ \
                    'of step {} but not in the results of step {}'.format( \
                        len(nl), len(nl) + len(except_nl))]

        if parsedSQL.limit:
            last_step_parts = nl[-1].split()
            if parsedSQL.limit == 1:
                if last_step_parts[-4:-1] == ['ordered', 'ascending', 'by']:        
                    nl[-1] = ' '.join(last_step_parts[:-4]+['with smallest value of', last_step_parts[-1]])
                elif last_step_parts[-4:-1] == ['ordered', 'descending', 'by']:
                    nl[-1] = ' '.join(last_step_parts[:-4]+['with largest value of', last_step_parts[-1]])
                elif last_step_parts[-8:-1] == ['ordered', 'ascending', 'by', 'the', 'results', 'of', 'step']:
                    nl[-1] = ' '.join(last_step_parts[:-8]+['with smallest value in the results of step', last_step_parts[-1]])
                elif last_step_parts[-8:-1] == ['ordered', 'descending', 'by', 'the', 'results', 'of', 'step']:
                    nl[-1] = ' '.join(last_step_parts[:-8]+['with largest value in the results of step', last_step_parts[-1]])
                else:
                    nl.append('only show the first {} rows of the results'.format(\
                        parsedSQL.limit))                    
            else:
                nl.append('only show the first {} rows of the results'.format(\
                    parsedSQL.limit))        
        
        return self._remove_duplicate_aggr_steps(self._remove_duplicate_steps([self._hacky_post_processor(step) for step in nl]))
            
    ## PROCESSOR HELPERS  ##
    def _add_nested_boundaries(self,steps):
        steps[0] = f'{NESTED_START} {steps[0]}'
        steps[-1] += ' '+NESTED_END

    def _order_dir(self,parsedSQL):
        if parsedSQL.orderby.dir_ == 'desc':
            return 'descending'
        else:
            assert parsedSQL.orderby.dir_ == 'asc'
            return 'ascending'

    def _distinct_select(self, parsedSQL):
        if parsedSQL.select.distinct:
            return ' without repetition'
        else:
            return ''       
    
    def _cond_op(self,condUnit):
        output = WHERE_OPS[condUnit.op_id]
        if condUnit.not_:
            output = 'not '+output
        return output
    
    def _aggr(self, groupby, agg_colunit, table):
        #find the _agg_ of _col_ of each _col_ in _table_ 
        distinct = agg_colunit.distinct
        return 'find the {}{} {} of each value of {} in {}'.format(\
            AGG_OPS[agg_colunit.agg_id],' different' if distinct else '', self.cname(agg_colunit.col_id), \
            ', '.join([self.cname(c) for c in groupby.getcols()]), self.tname(table))
    
    def _cond(self, cond):
        return '{} {} {}'.format(self.cname(cond.valunit.unit1.col_id), self._cond_op(cond), cond.val1.val)

    def _colagg(self, aggop, distinct, col):
        return 'the {}{} {}'.format(AGG_OPS[aggop], ' different' if distinct else '', self.cname(col))

    def _join(self, table1, table2):
        """
        table1 is the Pk table and table2 is the Fk table
        """
        return 'For each row in {}, find the corresponding rows in {}'.format(self.tname(table1), self.tname(table2))

    def _join_two_tables(self, parsedSQL):
        db = parsedSQL.get_db()        
        
        conds = parsedSQL.from_.list_join_conds()
        if len(conds) == 1:
            c1, c2 = conds[0]
            pk, fk = self.schema_utils.get_pk_fk(db, c1, c2)
            return [self._join(self.schema_utils.get_table_of_col(db, pk), \
                self.schema_utils.get_table_of_col(db, fk))]
        else:
            #wrong tables .. unrelated
            table1, table2 = parsedSQL.from_.list_tables()
            return [self._join(table1, table2)]

    def _join_3tables(self, common_table, table2, table3):
        return 'For each row in {}, find corresponding rows in {} and in {}'.format(\
            self.tname(common_table), self.tname(table2), self.tname(table3))

    def _join_three_tables(self, parsedSQL):
        db = parsedSQL.get_db()
        conds = parsedSQL.from_.list_join_conds()
        if len(conds) == 2:
            cond1, cond2 = conds
            c1, c2 = cond1
            c3, c4 = cond2
            t1, t2, t3, t4 = [self.schema_utils.get_table_of_col(db, c) for c in [c1,c2,c3,c4]]
            if t3 == t1:
                common = t3
                table2 = t2
                table3 = t4
            elif t3 == t2:
                common = t3
                table2 = t1
                table3 = t4
            elif t4 == t1:
                common = t4
                table2 = t2
                table3 = t3
            elif t4 == t2:
                common = t4
                table2 = t1
                table3 = t3
            else:
                assert False
        else:
            #The case when the query has a wrong table
            common, table2, table3 = parsedSQL.from_.list_tables()

        return [self._join_3tables(common, table2, table3)]

    ## PATTERN PROCESSORS ##
    def _sel_cols_one_tbl(self,parsedSQL):
        """
            return list of string steps
        """
        #sql: select: _cols_, from: _table_
        #nl: "show _cols_ of _table_"

        cols = parsedSQL.select.list_cols()
        table = parsedSQL.from_.list_tables()[0]
        return ['find{} the {} of {}'.format(self._distinct_select(parsedSQL),\
            ', '.join([self.cname(c) for c in cols]), self.tname(table))]

    def _sel_cols_one_tbl_order(self,parsedSQL):
        #sql: select: _cols_, from: _table_, orderby: _col_
        #nl: "show _cols_ of _table_ in a dir order of _col_"
        cols = [self.cname(c) for c in parsedSQL.select.list_cols()]
        table = self.tname(parsedSQL.from_.list_tables()[0])
        order_col = self.cname(parsedSQL.orderby.get_cols()[0])
        return ['find{} the {} of {} ordered {} by {}'.format(\
            self._distinct_select(parsedSQL),', '.join(cols), table, self._order_dir(parsedSQL), order_col)]

    def _sel_cols_one_tbl_whr(self,parsedSQL):
        #sql: select: _cols_, from: _table_, where: _op_, _col_, _val_
        #nl: show _cols_ of _table_ for which _col_ _op_ _val_

        output = self._sel_cols_one_tbl(parsedSQL)
        condunit = parsedSQL.where.conds[0]

        output[0] += ' for which {} {} {}'.format(self.cname(condunit.valunit.unit1.col_id), \
                            self._cond_op(condunit), str(condunit.val1.val))
        return output

    def _sel_aggr_col(self, parsedSQL):
        #sql: select: _agg_ _col_, from: _table_
        #nl: find the _agg_ [different] col_ in _table_ 

        col = self.cname(parsedSQL.select.list_cols()[0])
        table = self.tname(parsedSQL.from_.list_tables()[0])
        distinct = parsedSQL.select.valunits[0].unit1.distinct
        return ['find{} the {}{} {} in {}'.format(self._distinct_select(parsedSQL),\
                        AGG_OPS[parsedSQL.select.aggs[0]], \
                        ' different' if distinct else '',col, table)]


    def _sel_group_orderaggr(self, parsedSQL):
        #sql: select: _cols_, from: _table_, groupby: _cols_, orderby: _agg_ _col_
        #nl: 1- find the _agg_ of _col_ of each _col_ in _table_ 
        #2- find_cols_ of _table_ and order them _dir_ by the outputs in step 1
        
        table = parsedSQL.from_.list_tables()[0]        
        nl = [self._aggr(parsedSQL.groupby, parsedSQL.orderby.vals[0].unit1, table)]
        
        cols = [self.cname(c) for c in parsedSQL.select.list_cols()]
        nl.append('find{} {} of {} ordered {} by the results of step 1'.format(\
            self._distinct_select(parsedSQL),', '.join(cols), self.tname(table), self._order_dir(parsedSQL)))
        return nl


    def _sel_two_conds(self, parsedSQL):
        #sqlL select: _cols_, from: _table_, where: _op_, _col_, _val_, _op_, _col_, _val_
        #nl: find _cols_ of _table0_ whose _col1_ _op0_ _val0_ and _col2_ _op1_ _val1_

        cols = [self.cname(c) for c in parsedSQL.select.list_cols()]
        table = self.tname(parsedSQL.from_.list_tables()[0])
        andor = parsedSQL.where.andor[0]
        return ['find{} {} of {} whose {} {} {}'.format(self._distinct_select(parsedSQL), ', '.join(cols), table, \
            self._cond(parsedSQL.where.conds[0]), andor, self._cond(parsedSQL.where.conds[1]))]

    def _sel_col_aggcol_groupby(self, parsedSQL):
        #sql: select: _col_, _agg_ _col_, from: _table_, groupby: _cols_
        #note: _col_ is always the same as _cols_ and _cols_ is just one col
        #nl: find each value of _col_ in _table_ along with the _agg_ of _col_ corresponding to each value
        
        table = self.tname(parsedSQL.from_.list_tables()[0])
        return ['find each value of {} in {} along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]), table, AGG_OPS[parsedSQL.select.aggs[1]], \
            self.cname(parsedSQL.select.list_cols()[1]))]

    def _sel_aggcol_col_groupby(self, parsedSQL):
        #sql: select: _agg_ _col_, _col_, from: _table_, groupby: _cols_        
        table = self.tname(parsedSQL.from_.list_tables()[0])
        return ['find each value of {} in {} along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]), table, AGG_OPS[parsedSQL.select.aggs[0]], \
            self.cname(parsedSQL.select.list_cols()[0]))]

    def _sel_aggcol_one_cond(self, parsedSQL):
        #select: _agg_ _col_, from: _table_, where: _op_, _col_, _val_
        #nl: find the _agg_ _col_ of _table_ whose _col_ _op_ _val_
        table = self.tname(parsedSQL.from_.list_tables()[0])
        return ['find{} {} {} of {} whose {}'.format(self._distinct_select(parsedSQL), \
                                            AGG_OPS[parsedSQL.select.aggs[0]],\
         self.cname(parsedSQL.select.list_cols[0]), table, self._cond(parsedSQL.where.conds[0]))]

    def _sel_agg_whr(self, parsedSQL):
        #sql: select: _agg_ _col_, from: _table_, where: _op_, _col_, _val_
        return [self._sel_aggr_col(parsedSQL)[0]+' whose {}'.format(self._cond(parsedSQL.where.conds[0]))]


    def _sel_two_agg(self, parsedSQL):
        #sql: select: _agg_ _col_, _agg_ _col_, from: _table_
        #nl: find the _agg_ [different] col_ and the _agg_ [different] col_ in _table_ 

        col1 = parsedSQL.select.list_cols()[0]
        agg1 = parsedSQL.select.aggs[0]
        distinct1 = parsedSQL.select.valunits[0].unit1.distinct

        col2 = parsedSQL.select.list_cols()[1]
        agg2 = parsedSQL.select.aggs[1]
        distinct2 = parsedSQL.select.valunits[1].unit1.distinct
        table = parsedSQL.from_.list_tables()[0]    
        return ['find{} the {}{} {} and the {}{} {} in {}'.format(self._distinct_select(parsedSQL),\
                        AGG_OPS[agg1], \
                        ' different' if distinct1 else '',self.cname(col1), AGG_OPS[agg2], \
                        ' different' if distinct2 else '',self.cname(col2), self.tname(table))]

    def _sel_two_agg_whr(self, parsedSQL):
        #sql: select: _agg_ _col_, _agg_ _col_, from: _table_, where: _op_, _col_, _val_
        return [self._sel_two_agg(parsedSQL)[0]+' whose {}'.format(self._cond(parsedSQL.where.conds[0]))]

    def _sel_group_having_agg(self, parsedSQL):
        #sql: select: _cols_, from: _table_, groupby: _cols_, having: _op_, _agg_ _col_, _val_
        #nl: 1- calc agg _col and group by, 2- find those matching the having cond
        table = parsedSQL.from_.list_tables()[0]
        selcols = ', '.join([self.cname(c) for c in parsedSQL.select.list_cols()])
        val = parsedSQL.having.conds[0].val1.val
        op = self._cond_op(parsedSQL.having.conds[0])
        nl = [self._aggr(parsedSQL.groupby, parsedSQL.having.conds[0].valunit.unit1, table)]
        nl.append('find{} {} in {} whose corresponding value in step 1 is {} {}'.format(\
            self._distinct_select(parsedSQL), selcols, self.tname(table), op, val))
        return nl
    
    def  _sel_three_conds(self, parsedSQL):
        #sqlL select: _cols_, from: _table_, where: _op_, _col_, _val_, _op_, _col_, _val_, _op_, _col_, _val_
        #nl: find _cols_ of _table0_ whose _col1_ _op0_ _val0_ and _col2_ _op1_ _val1_

        cols = [self.cname(c) for c in parsedSQL.select.list_cols()]
        table = self.tname(parsedSQL.from_.list_tables()[0])
        andor = parsedSQL.where.andor
        return ['find{} {} of {} whose {} {} {} {} {}'.format(self._distinct_select(parsedSQL), ', '.join(cols), table, \
            self._cond(parsedSQL.where.conds[0]), andor[0], self._cond(parsedSQL.where.conds[1]), andor[1], self._cond(parsedSQL.where.conds[2]))]
    
    def _sel_whr_nested(self, parsedSQL):
        #sql: select: _cols_, from: _table_, where: _op_, _col_, select: _cols_, from: _table_
        #nl: 1. explain nested, 2. explain main and link to nested

        nl = self._sel_cols_one_tbl(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_cols_one_tbl(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl

    def _sel_aggcol_whr_nested(self, parsedSQL):
        #sql: select: _agg_ _col_, from: _table_, where: _op_, _col_, select: _cols_, from: _table_

        nl = self._sel_cols_one_tbl(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_aggr_col(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl

    def _sel_whr_nested_agg(self, parsedSQL):
        #sql: select: _cols_, from: _table_, where: _op_, _col_, select: _agg_ _col_, from: _table_
        nl = self._sel_aggr_col(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_cols_one_tbl(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl       

    def _sel_cols_whr_nested_agg_whr(self, parsedSQL):
        
        #sql: select: _cols_, from: _table_, where: _op_, _col_, select: _agg_ _col_, from: _table_, where: _op_, _col_, _val_
        
        nl = self._sel_agg_whr(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES:  self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_cols_one_tbl(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl

    def _sel_col_whr_nested_whr(self, parsedSQL):
        
        #sql: select: _cols_, from: _table_, where: _op_, _col_, select: _cols_, from: _table_, where: _op_, _col_, _val_
        nl = self._sel_cols_one_tbl_whr(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_cols_one_tbl(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl
    
    def _sel_cols_whr_orderby(self, parsedSQL):
        #sql: select: _cols_, from: _table_, where: _op_, _col_, _val_, orderby: _col_
        nl = self._sel_cols_one_tbl_whr(parsedSQL)
        order_col = parsedSQL.orderby.get_cols()[0]
        nl[0] += ' ordered {} by {}'.format(self._order_dir(parsedSQL), self.cname(order_col))
        return nl

    def _sel_aggcol_two_conds(self, parsedSQL):
        #sql: select: _agg_ _col_, from: _table_, where: _op_, _col_, _val_, _op_, _col_, _val_
        andor = parsedSQL.where.andor[0]
        return [self._sel_aggr_col(parsedSQL)[0]+' whose {} {} {}'.format(\
            self._cond(parsedSQL.where.conds[0]), andor, self._cond(parsedSQL.where.conds[1]))]

    def _sel_whr_two_vals(self, parsedSQL):
        #sql: select: _cols_, from: _table_, where: _op_, _col_, _val_, _val_       
        #nl: show _cols_ of _table_ for which _col_ equals or between _val_ and _val_ 

        output = self._sel_cols_one_tbl(parsedSQL)
        condunit = parsedSQL.where.conds[0]

        output[0] += ' for which {} equals or between {} and {}'.format(self.cname(condunit.valunit.unit1.col_id), \
                            condunit.val1.val, condunit.val2.val)
        return output

    def _sel_three_aggcol(self, parsedSQL):
        #sql: select: _agg_ _col_, _agg_ _col_, _agg_ _col_, from: _table_
        #nl: find the _agg_ [different] col_, the _agg_ [different] col_ and the _agg_ [different] col_  in _table_ 

        table = parsedSQL.from_.list_tables()[0]
        cols = parsedSQL.select.list_cols()
        
        distinct1 = parsedSQL.select.valunits[0].unit1.distinct
        distinct2 = parsedSQL.select.valunits[1].unit1.distinct
        distinct3 = parsedSQL.select.valunits[2].unit1.distinct
        aggs = parsedSQL.select.aggs

        return ['find {}, {} and {} in {}'.format(self._colagg(aggs[0], distinct1, cols[0]), \
                    self._colagg(aggs[1], distinct1, cols[1]), self._colagg(aggs[2], distinct1, cols[2]), self.tname(table))]

    def _sel_aggcol_col_whr_onecond_groupby(self, parsedSQL):
        #sql: select: _agg_ _col_, _col_, from: _table_, where: _op_, _col_, _val_, groupby: _cols_

        table = parsedSQL.from_.list_tables()[0]
        nl = ['find rows in {} whose {}'.format(self.tname(table), self._cond(parsedSQL.where.conds[0]))]
        nl.append(\
            'find each value of {} the results of step 1 along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]), AGG_OPS[parsedSQL.select.aggs[0]], \
            self.cname(parsedSQL.select.list_cols()[0])))
        return nl

    def _sel_col_aggcol_whr_onecond_groupby(self, parsedSQL):

        #sql: select: _col_, _agg_ _col_, from: _table_, where: _op_, _col_, _val_, groupby: _cols_
        table = parsedSQL.from_.list_tables()[0]
        nl = ['find rows in {} whose {}'.format(self.tname(table), self._cond(parsedSQL.where.conds[0]))]
        nl.append(\
            'find each value of {} in the results of step 1 along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]),  AGG_OPS[parsedSQL.select.aggs[0]], \
            self.cname(parsedSQL.select.list_cols()[1])))
        return nl

    def _sel_col_aggcol_group_orderby_agg(self, parsedSQL):
        #sql: select: _col_, _agg_ _col_, from: _table_, groupby: _cols_, orderby: _agg_ _col_      
        #sel col is the same as groupby col
        #nl: 1- for each value of group_col in table, calculate order agg
        #    2- find each value of group col in table along with the {} {} and in a dir order of step 1 result
        table = parsedSQL.from_.list_tables()[0]
        groupcol = parsedSQL.groupby.getcols()[0]
        orderunit = parsedSQL.orderby.vals[0].unit1
        nl = ['for each value of {} in {}, calculate {}{} {}'.format(self.cname(groupcol), self.tname(table), \
                AGG_OPS[orderunit.agg_id], \
                        ' different' if orderunit.distinct else '', self.cname(orderunit.col_id))]

        sel_aggunit = parsedSQL.select.valunits[1].unit1

        nl.append('show each value of {} in {} along with the corresponding {}{} {} ordered {} by the results of step 1'.format(\
            self.cname(groupcol), self.tname(table), AGG_OPS[parsedSQL.select.aggs[1]], ' different' if sel_aggunit.distinct else '', \
            self.cname(sel_aggunit.col_id), self._order_dir(parsedSQL)))

        return nl

    def _sel_cols_whr_cond_group_orderby_agg(self, parsedSQL):      
        
        #sql: select: _cols_, from: _table_, where: _op_, _col_, _val_, groupby: _cols_, orderby: _agg_ _col_
        #sel_col in same as groupby col
        #nl: 1- find the rows in table whose cond
        #    2- find each value of selcol in step 1 result and order them dir by agg ordercol that correspond of each value
        
        selcols = ', '.join([self.cname(c) for c in parsedSQL.select.list_cols()])
        table = parsedSQL.from_.list_tables()[0]
        orderunit = parsedSQL.orderby.vals[0].unit1
        nl = ['find the rows in {} whose {}'.format(self.tname(table), self._cond(parsedSQL.where.conds[0]))]
        nl.append('find each value of {} in the results of step 1 ordered {} by {}{} {} that correspond of each value'.format(\
            selcols, self._order_dir(parsedSQL),AGG_OPS[orderunit.agg_id], \
             ' different' if orderunit.distinct else '', self.cname(orderunit.col_id)))
        return nl


    def _sel_agg_whr_nested_whr(self, parsedSQL):    
        #select: _agg_ _col_, from: _table_, where: _op_, _col_, select: _cols_, from: _table_, where: _op_, _col_, _val_
        nl = self._sel_cols_one_tbl_whr(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_aggr_col(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl

    def _sel_cols_whr_nested_whr_nested_agg(self, parsedSQL):        
        #select: _cols_, from: _table_, where: _op_, _col_, select: _cols_, from: _table_, where: _op_, _col_, select: _agg_ _col_, from: _table_
        nl = self._sel_whr_nested_agg(parsedSQL.where.conds[0].val1.val)
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_cols_one_tbl(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl


    def _sel_cols_whr_nested_aggcol_nested_nested(self, parsedSQL):
        
        #select: _cols_, from: _table_, where: _op_, _col_, select: _agg_ _col_, from: _table_, where: _op_, _col_, select: _cols_, from: _table_, where: _op_, _col_, _val_, _op_, _col_, select: _cols_, from: _table_, where: _op_, _col_, _val_
        #step 4                                              step 3                                                 step 2                                                                 step 1                                                               
        nl = self._sel_cols_one_tbl_whr(parsedSQL.where.conds[0].val1.val.where.conds[0].val1.val.where.conds[0].val1.val)
        
        nl.append('{} whose {} {} the results of step 1'.format(self._sel_cols_one_tbl(parsedSQL.where.conds[0].val1.val.where.conds[0].val1.val)[0],\
            self.cname(parsedSQL.where.conds[0].val1.val.where.conds[0].val1.val.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0].val1.val.where.conds[0].val1.val.where.conds[0])))

        nl.append('{} whose {} {} the results of step 2'.format(self._sel_aggr_col(parsedSQL.where.conds[0].val1.val)[0],\
            self.cname(parsedSQL.where.conds[0].val1.val.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0].val1.val.where.conds[0])))

        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        
        nl.append('{} whose {} {} the results of step 3'.format(self._sel_cols_one_tbl(parsedSQL)[0],\
            self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))        
        return nl

    ##### joins .. j2 indicates joining two tables
    def _sel_cols_j2(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond
        nl = self._join_two_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        nl.append('find{} {} of the results of step 1'.format(self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols])))
        return nl

    def _sel_cols_j2_cond(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_
        nl = self._join_two_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        nl.append('find{} {} of the results of step 1 whose {}'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols]), self._cond(parsedSQL.where.conds[0])))
        return nl

    def _sel_cols_j2_two_cond(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_
        nl = self._join_two_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        andor = parsedSQL.where.andor[0]
        nl.append('find{} {} of the results of step 1 whose {} {} {}'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols]), \
            self._cond(parsedSQL.where.conds[0]), andor, self._cond(parsedSQL.where.conds[1])))        
        return nl

    def _sel_cols_j2_three_cond(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_, _op_, _col_, _val_
        nl = self._join_two_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        andor1 = parsedSQL.where.andor[0]
        andor2 = parsedSQL.where.andor[1]
        nl.append('find{} {} of the results of step 1 whose {} {} {} {} {}'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols]), \
            self._cond(parsedSQL.where.conds[0]), andor1, self._cond(parsedSQL.where.conds[1]),\
            andor2, self._cond(parsedSQL.where.conds[2])))        
        return nl

    def _sel_cols_j2_whr_four_conds(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_, _op_, _col_, _val_, _op_, _col_, _val_
        nl = self._join_two_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        andor1 = parsedSQL.where.andor[0]
        andor2 = parsedSQL.where.andor[1]
        andor3 = parsedSQL.where.andor[2]
        nl.append('find{} {} of the results of step 1 whose {} {} {} {} {} {} {}'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols]), \
            self._cond(parsedSQL.where.conds[0]), andor1, self._cond(parsedSQL.where.conds[1]),\
            andor2, self._cond(parsedSQL.where.conds[2]), andor3, self._cond(parsedSQL.where.conds[3])))        
        return nl

 
    def _sel_cols_j2_order(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, orderby: _col_ 
        nl = self._sel_cols_j2(parsedSQL)
        nl[-1] += ' ordered {} by {}'.format(self._order_dir(parsedSQL), self.cname(parsedSQL.orderby.get_cols()[0]))
        return nl

    def _sel_aggcol_j2_cond(self, parsedSQL):       
        #select: _agg_ _col_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_
        nl = self._join_two_tables(parsedSQL)
        col = parsedSQL.select.list_cols()[0]        
        distinct = parsedSQL.select.valunits[0].unit1.distinct
        
        nl.append('find{} the {}{} {} in the results of step 1 whose {}'.format(self._distinct_select(parsedSQL),\
                        AGG_OPS[parsedSQL.select.aggs[0]], \
                        ' different' if distinct else '', self.cname(col), self._cond(parsedSQL.where.conds[0])))
        return nl

    def _sel_aggcol_j2_two_cond(self, parsedSQL):
        #select: _agg_ _col_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_
        nl = self._sel_aggcol_j2_cond(parsedSQL)
        andor = parsedSQL.where.andor[0]
        nl[-1] += ' {} {}'.format(andor, self._cond(parsedSQL.where.conds[1]))
        return nl

    def _sel_aggcol_j2_three_cond(self, parsedSQL):
        #select: _agg_ _col_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_, _op_, _col_, _val_
        nl = self._sel_aggcol_j2_cond(parsedSQL)
        andor1 = parsedSQL.where.andor[0]
        andor2 = parsedSQL.where.andor[1]
        nl[-1] += ' {} {} {} {}'.format(andor1, self._cond(parsedSQL.where.conds[1]), andor2, self._cond(parsedSQL.where.conds[2]))
        return nl
        
    def _sel_aggcol_col_j2_groupby(self, parsedSQL):
        #select: _agg_ _col_, _col_, from: _table_, _table_ with join cond, groupby: _cols_
        nl = self._join_two_tables(parsedSQL)
        nl.append('find each value of {} in the results of step 1 along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]),  AGG_OPS[parsedSQL.select.aggs[0]], \
            self.cname(parsedSQL.select.list_cols()[0])))        
        return nl
        

    def _sel_col_aggcol_j2_grouby(self, parsedSQL):
        #select: _col_, _agg_ _col_, from: _table_, _table_ with join cond, groupby: _cols_
        nl = self._join_two_tables(parsedSQL)
        nl.append('find each value of {} in the results of step 1 along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]),  AGG_OPS[parsedSQL.select.aggs[1]], \
            self.cname(parsedSQL.select.list_cols()[1])))        
        return nl

    def _sel_cols_j2_groupby_order_agg(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, groupby: _cols_, orderby: _agg_ _col_    
        nl = self._join_two_tables(parsedSQL)
        nl.append(self._aggr(parsedSQL.groupby, parsedSQL.orderby.vals[0].unit1, 'the results of step 1'))        
        cols = parsedSQL.select.list_cols()
        nl.append('find{} {} of {} ordered {} by the results of step 2'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in cols]), 'step 1 results', self._order_dir(parsedSQL)))
        return nl


    def _sel_col_col_aggcol_j2_groupby(self, parsedSQL):
        #select: _col_, _col_, _agg_ _col_, from: _table_, _table_ with join cond, groupby: _cols_
        nl = self._join_two_tables(parsedSQL)
        cols = parsedSQL.select.list_cols()
        nl.append('for each value of {} in the results of step 1, find the {} {} along with {} and {}'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]), AGG_OPS[parsedSQL.select.aggs[2]], \
            self.cname(cols[2]), self.cname(cols[0]), self.cname(cols[1])))
        return nl


    def _sel_cols_j2_cond_orderby(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, orderby: _col_              
        nl = self._sel_aggcol_j2_cond(parsedSQL)
        order_col = parsedSQL.orderby.get_cols()[0]
        nl[-1] += ' ordered {} by {}'.format(self._order_dir(parsedSQL), self.cname(order_col))
        return nl


    def _sel_col_aggcol_j2_groupby_order_agg(self, parsedSQL):
        #select: _col_, _agg_ _col_, from: _table_, _table_ with join cond, groupby: _cols_, orderby: _agg_ _col_
        
        nl = self._join_two_tables(parsedSQL)
        groupcol = parsedSQL.groupby.getcols()[0]
        orderunit = parsedSQL.orderby.vals[0].unit1
        nl.append('for each value of {} in the results of step 1, calculate {}{} {}'.format(self.cname(groupcol), \
                AGG_OPS[orderunit.agg_id], ' different' if orderunit.distinct else '', self.cname(orderunit.col_id)))
        sel_aggunit = parsedSQL.select.valunits[1].unit1
        """
        nl.append('show each value of {} in step 1 results along with the {}{} {} ordered {} by the results of step 2'.format(\
            self.cname(groupcol), AGG_OPS[sel_aggunit.agg_id], ' different' if sel_aggunit.distinct else '', \
            self.cname(sel_aggunit.col_id), self._order_dir(parsedSQL)))
        """
        nl.append('show each value of {} in the results of step 1 along with the {}{} {} ordered {} by the results of step 2'.format(\
            self.cname(groupcol), AGG_OPS[parsedSQL.select.aggs[1]], ' different' if sel_aggunit.distinct else '', \
            self.cname(sel_aggunit.col_id), self._order_dir(parsedSQL)))
        return nl

    def _sel_cols_j2_whr_nested_agg(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, select: _agg_ _col_, from: _table_
        
        nl = self._sel_aggr_col(parsedSQL.where.conds[0].val1.val)        
        if ADD_SUBQUERY_BOUNDARIES: self._add_nested_boundaries(nl)
        
        nl.append(self._join_two_tables(parsedSQL)[0])
        cols = parsedSQL.select.list_cols()
        nl.append('find{} {} in the results of step 2 whose {} {} the results of step 1'.format(self._distinct_select(parsedSQL), \
            ', '.join([self.cname(c) for c in cols]), self.cname(parsedSQL.where.conds[0].valunit.unit1.col_id), \
            self._cond_op(parsedSQL.where.conds[0])))
        return nl       

    def _sel_cols_j2_groupby_having_agg(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, groupby: _cols_, having: _op_, _agg_ _col_, _val_
        nl = self._join_two_tables(parsedSQL)        
        selcols = ', '.join([self.cname(c) for c in parsedSQL.select.list_cols()])
        val = parsedSQL.having.conds[0].val1.val
        op = self._cond_op(parsedSQL.having.conds[0])
        nl.append(self._aggr(parsedSQL.groupby, parsedSQL.having.conds[0].valunit.unit1, 'the results of step 1'))
        nl.append('find{} {} in the results of step 1 whose corresponding value in step 2 is {} {}'.format(\
            self._distinct_select(parsedSQL), selcols, op, val))
        return nl

    def _sel_cols_j2_cond_groupby_order_agg(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, groupby: _cols_, orderby: _agg_ _col_

        nl = self._join_two_tables(parsedSQL)
        nl.append('only keep the results of step 1 whose {}'.format(self._cond(parsedSQL.where.conds[0])))        
        nl.append(self._aggr(parsedSQL.groupby, parsedSQL.orderby.vals[0].unit1, 'the results of step 2'))        
        cols = parsedSQL.select.list_cols()
        nl.append('find{} {} of the results of step 2 ordered {} by the results of step 3'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in cols]), self._order_dir(parsedSQL)))
        return nl


    def _sel_cols_j2_whr_cond_groupby_having(self, parsedSQL):
        #select: _cols_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, groupby: _cols_, having: _op_, _agg_ _col_, _val_
        
        nl = self._join_two_tables(parsedSQL)
        nl.append('find rows in the results of step 1 whose {}'.format(self._cond(parsedSQL.where.conds[0])))
        selcols = ', '.join([self.cname(c) for c in parsedSQL.select.list_cols()])
        val = parsedSQL.having.conds[0].val1.val
        op = self._cond_op(parsedSQL.having.conds[0])
        nl.append(self._aggr(parsedSQL.groupby, parsedSQL.having.conds[0].valunit.unit1, 'step 1 rsults'))
        nl.append('find{} {} in the results of step 1 whose corresponding value in step 2 is {} {}'.format(\
            self._distinct_select(parsedSQL), selcols, op, val))
        return nl

    def _sel_aggcol_col_j2_whr_cond_groupby(self, parsedSQL):        
        #select: _agg_ _col_, _col_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, groupby: _cols_
        nl = self._join_two_tables(parsedSQL)
        table = parsedSQL.from_.list_tables()[0]
        nl.append('find rows in the results of step 1 whose {}'.format(self._cond(parsedSQL.where.conds[0])))
        nl.append(\
            'find each value of {} the results of step 1 along with the {} {} of the corresponding rows to each value'.format(\
            self.cname(parsedSQL.groupby.getcols()[0]), AGG_OPS[parsedSQL.select.aggs[0]], self.cname(parsedSQL.select.list_cols()[0])))
        return nl

    def _sel_aggcol_col_j2_whr_cond_groupby_orderby_agg(self, parsedSQL):
        #select: _agg_ _col_, _col_, from: _table_, _table_ with join cond, where: _op_, _col_, _val_, groupby: _cols_, orderby: _agg_ _col_ ?
        nl = self._sel_aggcol_col_j2_whr_cond_groupby(parsedSQL)
        nl.append('Order the results of step {} {} by {} {}'.format(len(nl),\
            self._order_dir(parsedSQL), AGG_OPS[parsedSQL.select.aggs[0]], self.cname(parsedSQL.select.list_cols()[0])))
        return nl

    def _sel_cols_j3(self, parsedSQL):
        #select: _cols_, from: _table_, _table_, _table_ with join cond
        nl = self._join_three_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        nl.append('find{} {} of the results of step {}'.format(self._distinct_select(parsedSQL),\
            ', '.join([self.cname(c) for c in selcols]), len(nl)))
        return nl

    def _sel_cols_j3_cond(self, parsedSQL):
        #select: _cols_, from: _table_, _table_, _table_ with join cond, where: _op_, _col_, _val_
        nl = self._join_three_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        nl.append('find{} {} of the results of step 1 whose {}'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols]), self._cond(parsedSQL.where.conds[0])))
        return nl

    def _sel_cols_j3_two_cond(self, parsedSQL):
        #select: _cols_, from: _table_, _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_
        nl = self._join_three_tables(parsedSQL)
        selcols = parsedSQL.select.list_cols()
        andor = parsedSQL.where.andor[0]
        nl.append('find{} {} of the results of step 1 whose {} {} {}'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in selcols]), \
            self._cond(parsedSQL.where.conds[0]), andor, self._cond(parsedSQL.where.conds[1])))
        return nl

    def _sel_aggcol_j3_cond(self, parsedSQL):
        #select: _agg_ _col_, from: _table_, _table_, _table_ with join cond, where: _op_, _col_, _val_
        nl = self._join_three_tables(parsedSQL)
        col = parsedSQL.select.list_cols()[0]        
        distinct = parsedSQL.select.valunits[0].unit1.distinct        
        nl.append('find{} the {}{} {} in the results of step 1 whose {}'.format(self._distinct_select(parsedSQL),\
                        AGG_OPS[parsedSQL.select.aggs[0]], \
                        ' different' if distinct else '',self.cname(col), self._cond(parsedSQL.where.conds[0])))
        return nl

    def _sel_cols_j3_order(self, parsedSQL):        
        #select: _cols_, from: _table_, _table_, _table_ with join cond, orderby: _col_
        nl = self._sel_cols_j3(parsedSQL)
        nl[-1] += ' ordered {} by {}'.format(self._order_dir(parsedSQL), self.cname(parsedSQL.orderby.get_cols()[0]))
        return nl

    def _sel_aggcol_j3_two_cond(self, parsedSQL):
        #select: _agg_ _col_, from: _table_, _table_, _table_ with join cond, where: _op_, _col_, _val_, _op_, _col_, _val_ 10

        nl = self._sel_aggcol_j3_cond(parsedSQL)
        andor = parsedSQL.where.andor[0]
        nl[-1] += ' {} {}'.format(andor, self._cond(parsedSQL.where.conds[1]))
        return nl

    def _sel_cols_j3_groupby_order_agg(self, parsedSQL):
        #select: _cols_, from: _table_, _table_, _table_ with join cond, groupby: _cols_, orderby: _agg_ _col_        
        nl = self._join_three_tables(parsedSQL)
        nl.append(self._aggr(parsedSQL.groupby, parsedSQL.orderby.vals[0].unit1, 'the results of step 1'))        
        cols = parsedSQL.select.list_cols()
        nl.append('find{} {} of {} ordered {} by the results of step 2'.format(\
            self._distinct_select(parsedSQL),', '.join([self.cname(c) for c in cols]), 'the results of step 1', self._order_dir(parsedSQL)))
        return nl
