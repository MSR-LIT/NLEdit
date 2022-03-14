import sys
sys.path.append('../')
import json
import _jsonnet
import attr
from copy import deepcopy
import random
import numpy
from random import shuffle
from synthesizer import Synthesizer
import utils

import sqlutils
from sqlutils.SQL2NL import SQL2NL
from sqlutils.SQLParse import SQLParse
from sqlutils.process_sql import get_schema, Schema, get_sql
from sqlutils.Val_Inference import Val_Inference
from sqlutils.Schema_Utils import Schema_Utils

SPIDER_DB_DIR = '/home/ahmed/fuse_connections/nledit/spider/database'
TABLES_FILE = '../../data/spider-tables.json'
EXPLANATION_PATTERNS = '../sqlutils/PATTERN_TO_PROCESSOR.TSV'
schema_utils = Schema_Utils(TABLES_FILE, SPIDER_DB_DIR)
utils.schema_utils = schema_utils
sqlutils.SQLParse.schema = schema_utils
sqlutils.SQLDiffBertwithSub.schema = schema_utils
sql2nl = SQL2NL(schema_utils, patterns_file=EXPLANATION_PATTERNS)

@attr.s
class Seed:
    db_id = attr.ib()
    question = attr.ib()
    gold_sql_str = attr.ib()
    gold_sql = attr.ib()
    #used for synthesize
    banned_tabs = attr.ib(factory=set)
    banned_cols = attr.ib(factory=set)
    applied_edits = attr.ib(factory=set)    
    feedback = attr.ib(factory=list) #list of phrases. 
    
    pred_expln = attr.ib(default=None)
    pred_sql = attr.ib()
    @pred_sql.default
    def _init_pred(self): return deepcopy(self.gold_sql)

    def _gen_feedback(self, randomize=True):
        order = list(range(len(self.feedback)))
        if randomize: shuffle(order)
        return ' , '.join([self.feedback[i] for i in order]).lower()

    def _explain_pred_sql(self):
        self.pred_expln = []
        for i, step in enumerate(sql2nl.get_nl(self.pred_sql)):
            self.pred_expln.append(f'Step {i+1}: {step}')
    
    def clone(self): return deepcopy(self)

    def append_feedback_phrase(self, phrase): self.feedback.append(phrase)
    def pop_feedback_phrase(self):
        if len(self.feedback) > 0: self.feedback = self.feedback[:-1]

    def get_pred_explanation(self, force_recompute=False):
        """
            return None if pred_sql is not explainable.
        """
        if not sql2nl.is_supported(self.pred_sql): return None
        if force_recompute or self.pred_expln is None: self._explain_pred_sql()
        return self.pred_expln
    
    def to_dict(self):        
        self._explain_pred_sql()
        return {
            'db_id': self.db_id,
            'question': self.question,
            'gold_sql_str': self.gold_sql_str,
            'gold_parse': self.gold_sql.to_spider_parse(),
            'predicted_parse': self.pred_sql.to_spider_parse(),
            'predicted_parse_explanation': self.pred_expln,
            'feedback': self._gen_feedback(),
            'diff': self.pred_sql.diff(self.gold_sql).str_tokens(),
            'applied_edits': list(self.applied_edits),
        }
    
    def is_valid(self):
        if not sql2nl.is_supported(self.pred_sql):
            print('Predicted SQL is not explainable.', self.pred_sql)
            return False
        if len(self.feedback) == 0: return False
        return True

def write_synthesized(synthesized, writeto):
    with open(writeto, 'w') as f: json.dump([s.to_dict() for s in synthesized if s.is_valid()], f, indent=4)

def load_seed(spider_train, spider_train_other, splash_dev):
    # seed = spider training - examples with dbs under splash dev        
    with open(splash_dev) as f:  skipdbs = set([ex['db_id'] for ex in json.load(f)])
    seeds = []
    for spider_file in [spider_train, spider_train_other]:
        with open(spider_file) as f: spider = json.load(f)
        for ex in spider:
            if ex['db_id'] in skipdbs: continue
            seeds.append(Seed(
                db_id = ex['db_id'],
                question = ex['question'],
                gold_sql_str = ex['query'],
                gold_sql = SQLParse(ex['sql'], db=ex['db_id'])
            ))
    return seeds

def synthesize(synthesizer, seeds, configs):    
    synthesized = []
    num_expanded = 0
    for seed in seeds:
        for ex in synthesizer.expand_seed(seed, sql2nl):
            synthesized.append(ex)
        num_expanded += 1        
    return synthesized

if __name__ == '__main__':
    """
        python main.py configs.jsonnet
    """    
    configs = json.loads(_jsonnet.evaluate_file(sys.argv[1]))
    
    random.seed(configs['seed'])
    numpy.random.seed(configs['seed'])

    seeds = load_seed(configs['spider_train'],
                        configs['spider_train_other'], 
                        configs['splash_dev'])
    
    synthesizer = Synthesizer(configs)
    synthesized = synthesize(synthesizer, seeds, configs)
    write_synthesized(synthesized, configs['output_file'])
    synthesizer.print_stats()
