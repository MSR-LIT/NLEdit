import json
import sqlite3
import os

class Schema_Utils:

    def __init__(self, tables_json_path, db_dir):
        self.db_dir = db_dir
        with open(tables_json_path) as inh:
            dbs = json.load(inh)

        self.schemata = {}
        for db in dbs:
            db_name = db['db_id']
            #will use original names and revise if needed
            self.schemata[db_name] = {}
            self.schemata[db_name]['table_names'] = db['table_names_original']
            self.schemata[db_name]['column_names'] = [col[1] for col in db['column_names_original']]
            self.schemata[db_name]['colid2tableid'] = \
                                {i: col[0] for i, col in enumerate(db['column_names_original'])}
            self.schemata[db_name]['column_types'] = db['column_types']
            self.schemata[db_name]['foreign_keys'] = db['foreign_keys']
            self.schemata[db_name]['primary_keys'] = db['primary_keys']
            
            self.schemata[db_name]['natural_table_names'] = db['table_names']
            self.schemata[db_name]['natural_column_names'] = [col[1] for col in db['column_names']]

            #original table name to table id
            self.schemata[db_name]['table_name2id'] = \
                        {name.lower():i for i, name in enumerate(db['table_names_original'])}

            #table_id,col_name -> col_id
            self.schemata[db_name]['column_name2id'] = \
                        {(col[0],col[1].lower()):i for i, col in enumerate(db['column_names_original'])}

    def is_pk(self, db, col_id):
        return col_id in self.schemata[db]['primary_keys']
        
    def get_table_name(self, db, table_id):            
        return self.schemata[db]['table_names'][table_id]

    def get_natural_table_name(self, db, table_id):
        return self.schemata[db]['natural_table_names'][table_id]

    def get_table_pk(self, db, table_id):
        for pk in  self.schemata[db]['primary_keys']:
            if table_id == self.get_table_of_col(db, pk):
                return pk
        return None

    def get_table_id_by_name(self, db, table_name):        
        return self.schemata[db]['table_name2id'][table_name.lower()]

    def get_column_name(self, db, col_id):
        if col_id == 0: return 'rows'
        return self.schemata[db]['column_names'][col_id]

    def get_natural_column_name(self, db, col_id):
        if col_id == 0: return 'rows'
        return self.schemata[db]['natural_column_names'][col_id]
        
    def get_column_fullname(self, db, col_id):
        if col_id == 0: return 'rows'
        return '{}.{}'.format(self.get_table_name(db, self.get_table_of_col(db,col_id)), 
                              self.get_column_name(db,col_id))

    def get_natural_column_fullname(self, db, col_id):
        if col_id == 0: return 'rows'
        return '{}\'s{}'.format(self.get_natural_table_name(db, self.get_table_of_col(db,col_id)), 
                              self.get_natural_column_name(db,col_id))

    def get_column_id_by_name(self, db, table_id, column_name):
        if column_name in ['all', '*']: return 0 
        return self.schemata[db]['column_name2id'][(table_id,column_name)]
        
    def is_foreign_key(self, db, col1, col2):
        for fk in self.schemata[db]['foreign_keys']:
            if [col1, col2] == fk or [col2, col1] == fk: return True
        return False

    def get_pk_fk(self, db, col1, col2):
        for fk in self.schemata[db]['foreign_keys']:
            if [col1, col2] == fk:
                return col2, col1
            if [col2, col1] == fk:
                return col1, col2

        return col1, col2

    def list_pk_fk(self, db):
        return self.schemata[db]['foreign_keys']

    def get_table_of_col(self, db, col_id):
        assert db in self.schemata
        return self.schemata[db]['colid2tableid'][col_id]

    def get_col_type(self, db, col_id):
        return self.schemata[db]['column_types'][col_id]

    def get_tables(self, db):
        return list(range(len(self.schemata[db]['table_names'])))

    def get_columns(self, db):
        return list(range(len(self.schemata[db]['column_names'])))[1:]

    def list_schema_items_names(self, db):
        return self.schemata[db]['table_names'] + self.schemata[db]['column_names'][1:]

    def get_columns_of_table(self, db, table_id):
       return [col for col, tbl in self.schemata[db]['colid2tableid'].items() if tbl == table_id and col != 0]

    def get_values(self, db_path, table_name, columns, num_rows):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = "select {} from {} ORDER BY random() limit {}".format(\
            ','.join(['`{}`'.format(col) for col in columns]),table_name,num_rows)
        cursor.execute(query)
        rs = cursor.fetchall()

        output = []
        for _ in range(len(columns)): output.append([])
        for row in rs:
            for i,v in enumerate(row):
                output[i].append(v)
        return output

    def get_col_val(self, db, col_id, num_vals):
        db_path = db_path = os.path.join(self.db_dir, db, db + ".sqlite")
        column_name = self.get_column_name(db, col_id)

        table_name = self.get_table_name(db, self.get_table_of_col(db, col_id))
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('select `{}` from {} limit {}'.format(column_name, table_name, num_vals))
        rs = cursor.fetchall()
        return [r[0] for r in rs]

