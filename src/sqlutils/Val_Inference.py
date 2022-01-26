from .Schema_Utils import Schema_Utils
from .SQLParse import SQLParse, Value

class Val_Inference:

	def __init__(self, table_file, db_dir):
		self.schema = Schema_Utils(table_file, db_dir)

	def _set_val(self, col, val):
		match = True
		if isinstance(val, float) and val.is_integer():
			val = int(val)
		if isinstance(val, str):
			if val[0] in ['"',"'"] and val[-1] in ['"', "'"]:
				val = val[1:-1]
			if val[0] == '%' and val[-1] =='%':
				val = val[1:-1]
		col_id, obj = col
		if isinstance(obj, SQLParse):
			assert col_id == 'limit'
			if not isinstance(val, int):
				match = False
				val = 1
			obj.limit = val

		else:
			assert isinstance(obj,Value)
			obj.val = val
		return match

	def _map_vals_to_cols(self, cols, vals, data_type, db):
		match = True
		if len(cols) != len(vals): match = False
		shortest = min(len(cols), len(vals))
		for i in range(shortest):
			if not self._set_val(cols[i], vals[i]):
				match = False
		
		#set default values for unassigned columns
		
		default = 0 if data_type == 'numeric' else 'a'
		for col in cols[shortest:]:
			col_id, obj = col
			if col_id == 'limit' or col_id == 0:
				default = 1
			else:
				default = self.schema.get_col_val(db, col_id, 1)
				if len(default) == 0 or (isinstance(default[0],str) and len(default[0]) == 0):
					default = 0 if data_type == 'numeric' else 'A'
				else:
					default = default[0]
			self._set_val(col, default)

		return match

	def infer(self, parsedSQL, vals):
		"""
		parsedSQL: parse of the predicted value-less sql
		vals: list of gold vals
		
		return False if 100% sure that values do not
		fit.
		"""
		cols = parsedSQL.list_cols_vals()[0]
		
		numeric_vals = []
		text_vals = []
		for val in vals:
			if isinstance(val, str):
				text_vals.append(val)
			else:
				numeric_vals.append(val)
		numeric_cols = []
		text_cols = []
		for col in cols:
			col_id, _ = col			
			if col_id == 'limit' or col_id == 0 or \
				self.schema.get_col_type(parsedSQL.get_db(), col_id) == 'number':
				numeric_cols.append(col)
			else:
				text_cols.append(col)

		if len(numeric_cols) == 0:
			text_vals += numeric_vals

		if len(text_cols) == 0:
			numeric_vals += text_vals

		numeric_result = self._map_vals_to_cols(numeric_cols, numeric_vals, 'numeric', parsedSQL.get_db())
		text_result = self._map_vals_to_cols(text_cols, text_vals, 'text', parsedSQL.get_db())

		return numeric_result and text_result