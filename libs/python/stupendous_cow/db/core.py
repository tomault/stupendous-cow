import datetime

class ResultSet:
    def __init__(self, cursor, result_factory):
        self._cursor = cursor
        self._create_result = result_factory

    def init(self, stmt, columns, variables):
        try:
            self._cursor.execute(stmt, variables)
            self._columns = columns
            return self
        except:
            print stmt, variables
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def __iter__(self):
        return self

    def next(self):
        rec = self._cursor.fetchone()
        if not rec:
            self.close()
            raise StopIteration()
        args = dict(zip(self._columns, rec))
        return self._create_result(**args)

    def close(self):
        if self._cursor:
            self._cursor.close()
            self._cursor = None

class OneColumnResultSet(ResultSet):
    def init(self, stmt, variables):
        return ResultSet.init(self, stmt, (), variables)

    def next(self):
        rec = self._cursor.fetchone()
        if not rec:
            self.close()
            raise StopIteration()
        return self._create_result(rec[0])

_UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0)

def prepare_variable(value):
    if isinstance(value, datetime.datetime):
        return (value - _UNIX_EPOCH).total_seconds()
    return value

def format_for_sql(value):
    v = prepare_variable(value)
    if isinstance(v, str) or isinstance(v, unicode):
        return "'%s'" % value
    else:
        return str(v)

_LEGAL_CONSTRAINT_TYPES = (int, float, datetime.datetime, str, unicode)

def has_legal_constraint_type(value):
    return any(isinstance(value, t) for t in _LEGAL_CONSTRAINT_TYPES)

def construct_constraint(column, constraint):
    def verify_constraint_type(value):
        if not has_legal_constraint_type(value):
            msg = 'Value of type %s cannot be used as a constraint for ' + \
                  'column %s' % (value.__class__.__name__, column)
            raise ValueError(msg)
        return True
    
    if hasattr(constraint, 'to_sql'):
        return constraint.to_sql(column)
    elif isinstance(constraint, tuple) or isinstance(constraint, list):
        all(verify_constraint_type(x) for x in constraint)
        formatted = ', '.join(format_for_sql(x for x in constraint))
        sql = '%s IN (%s)' % (column, formatted)
        return (sql, ())
    elif constraint == None:
        return ('%s IS NULL' % column, ())
    else:
        verify_constraint_type(constraint)
        return ('%s = ?' % column, (prepare_variable(constraint), ))
        
def construct_constraints(criteria):
    if not criteria:
        return ('', ())
    
    sql = [ ]
    variables = [ ]
    for (column, constraint) in criteria.iteritems():
        (criteria_sql, criteria_variables) = \
            construct_constraint(column, constraint)
        sql.append(criteria_sql)
        variables.extend(criteria_variables)
    if len(sql) > 1:
        return (' AND '.join('(%s)' % s for s in sql), variables)
    return (sql[0], variables)

def construct_where_clause(criteria):
    if criteria:
        (constraints, variables) = construct_constraints(criteria)
        return (' WHERE ' + constraints, variables)
    return ('', ())

def construct_select_statement(table, columns, criteria):
    cols = ', '.join(columns)
    (where_clause, variables) = construct_where_clause(criteria)
    stmt = 'SELECT %s FROM %s%s' % (cols, table, where_clause)
    return (stmt, variables)

def construct_count_statement(table, criteria):
    (where_clause, variables) = construct_where_clause(criteria)
    stmt = 'SELECT count(*) FROM %s%s' % (table, where_clause)
    return (stmt, variables)

def construct_insert_statement(table, values):
    cols = ', '.join(values)
    values = ', '.join(format_for_sql(values[k]) for k in values)
    return ('INSERT INTO %s(%s) VALUES (%s)' % (table, cols, values), ())

def construct_update_statement(table, columns, criteria):
    def construct_column_update(column, value):
        return '%s=%s' % (column, format_for_sql(value))
    
    cols = ', '.join(construct_column_update(**x) for x in columns.iteritems())
    (where_clause, variables) = construct_where_clause(criteria)
    stmt = 'UPDATE %s SET %s%s' % (table, cols, where_clause)
    return (stmt, variables)

def construct_delete_statement(table, criteria):
    (where_clause, variables) = construct_delete_statement(criteria)
    stmt = 'DELETE FROM %s%s' % (table, where_clause)
    return (stmt, variables)

def execute_count(db, table, criteria):
    (stmt, variables) = construct_count_statement(table, criteria)
    cursor = db.cursor()
    try:
        cursor.execute(stmt, variables)
        return cursor.fetchone()[0]
    finally:
        cursor.close()

def execute_select(db, table, columns, criteria,
                    create_result = lambda **x: x):
    (stmt, variables) = construct_select_statement(table, columns, criteria)
    return ResultSet(db.cursor(), create_result).init(stmt, columns, variables)

def execute_dml(db, stmt, variables):
    cursor = db.cursor()
    try:
        cursor.execute(stmt, variables)
    except:
        print stmt
        raise
    finally:
        cursor.close()

def execute_insert(db, table, values):
    (stmt, variables) = construct_insert_statement(table, values)
    execute_dml(db, stmt, variables)

def execute_update(db, table, columns, criteria):
    (stmt, variables) = construct_update_statement(table, columns, criteria)
    execute_dml(db, stmt, variables)

def execute_delete(db, table, criteria):
    (stmt, variables) = construct_delete_statement(table, criteria)
    execute_dml(db, stmt, variables)

def next_item_id(db, table_name):
    cursor = db.cursor()
    try:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("SELECT id FROM id_sequence WHERE table_name = ?",
                       (table_name, ))
        try:
            next_id = next(cursor)[0]
            cursor.execute(
                "UPDATE id_sequence SET id = id + 1 WHERE table_name = ?;",
                (table_name,))
        except StopIteration:
            next_id = 1
            cursor.execute("INSERT INTO id_sequence VALUES(?, ?)",
                           (table_name, next_id + 1))
        db.commit()
        return next_id
    finally:
        cursor.close()

