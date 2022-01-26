import nltk
import sqlparse

"""
copied from editsql repo.
"""
def sql_tokenize(string):
    """ Tokenizes a SQL statement into tokens.
    Inputs:
       string: string to tokenize.
    Outputs:
       a list of tokens.
    """
    tokens = []
    statements = sqlparse.parse(string)

    # SQLparse gives you a list of statements.
    for statement in statements:
        # Flatten the tokens in each statement and add to the tokens list.
        flat_tokens = sqlparse.sql.TokenList(statement.tokens).flatten()
        for token in flat_tokens:
            strip_token = str(token).strip()
            if len(strip_token) > 0:
                tokens.append(strip_token)

    newtokens = []
    keep = True
    for i, token in enumerate(tokens):
        if token == ".":
            newtoken = newtokens[-1] + "." + tokens[i + 1]
            newtokens = newtokens[:-1] + [newtoken]
            keep = False
        elif keep:
            newtokens.append(token)
        else:
            keep = True

    return newtokens

def normalize(sql_toks):
    def is_number(val):
        try:
            float(val)
            return True
        except:
            return False

    sql_toks = [tok.lower() for tok in sql_toks]
    
    output_sql = []
    for ix, tok in enumerate(sql_toks):
        #if ix > 0 and sql_toks[ix-1] == 'limit':
        #    output_sql.append('limit_value')        
        if tok[0] in ['"',"'",'``','`'] or is_number(tok):
           output_sql.append('value')         
        elif tok == ';':
            pass
        else:
            output_sql.append(tok)

        """
        elif tok == '>=':
            output_sql += ['>','=']
        elif tok == '<=':
            output_sql += ['<','=']
        elif '.' in tok and len(tok.split('.')) == 2:
            parts = tok.split('.')
            output_sql += [parts[0],'.',parts[1]]
        """
        
    return output_sql


def drop_join_conds(sql_toks):
    output_sql = []
    skip = 0
    for ti, tok in enumerate(sql_toks):
        if skip > 0:
            skip -= 1
        elif tok == 'on':
            if ti + 4 < len(sql_toks) and sql_toks[ti + 4] == 'and':
                skip = 7
            else:
                skip = 3
        else:
            output_sql.append(tok)
    return output_sql
        