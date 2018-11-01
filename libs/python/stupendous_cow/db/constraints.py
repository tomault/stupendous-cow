from stupendous_cow.db.core import prepare_variable

class ColumnConstraint:
    def __init__(self, value):
        self.value = value

    def to_sql(self, column):
        sql = '%s %s ?' % (column, self._operator)
        variables = (prepare_variable(value), )
        return (sql, variables)

class GreaterThan(ColumnConstraint):
    _operator = '>'

class GreaterEqual(ColumnConstraint):
    _operator = '>='

class LessThan(ColumnConstraint):
    _operator = '<'

class LessEqual(ColumnConstraint):
    _operator = '<='

class InRange:
    def __init__(self, low, high, low_exclusive = False, high_exclusive = True):
        self.low = low
        self.high = high
        if low_exclusive:
            self._low_constraint = GreaterThan(low)
        else:
            self._low_constraint = GreaterEqual(low)

        if high_exclusive:
            self._high_constraint = LessThan(high)
        else:
            self._high_constraint = LessEqual(high)

    def to_sql(self, column):
        (low_sql, low_variables) = self._low_constraint.to_sql(column)
        (high_sql, high_variables) = self._high_constraint.to_sql(column)
        sql = '(%s) AND (%s)' % (low_sql, high_sql)
        variables = low_variables + high_variables
        return (sql, variables)

class _IsNull:
    def to_sql(self, column):
        return ('%s IS NULL' % column, ())

IsNull = _IsNull()

class _NotNull:
    def to_sql(self, column):
        return ('%s IS NOT NULL' % column, ())

NotNull = _NotNull()
