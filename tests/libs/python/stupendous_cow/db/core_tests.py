"""Unit tests for stupendous_cow.db.core"""
from stupendous_cow.db.core import *
from stupendous_cow.testing import DatabaseTestCase
import datetime
import os
import os.path
import sqlite3
import unittest

class ResultSetTests(DatabaseTestCase):
    def test_retrieve_all_items(self):
        def the_test(cursor):
            rs = ResultSet(cursor, ResultSetTests.Item)
            self.assertIs(rs, rs.init('SELECT * FROM items',
                                      ('id', 'name', 'code'), ()))
            result = [ x for x in rs ]
            self.assertEqual(self.all_items, result)
            self.assertIsNone(rs._cursor)

        self._execute_with_cursor(the_test)

    def test_retrieve_with_variables(self):
        def the_test(cursor):
            rs = ResultSet(cursor, ResultSetTests.Item)
            rs.init('SELECT * FROM items WHERE id = ?', ('id', 'name', 'code'),
                    (2, ))
            result = [ x for x in rs ]
            self.assertEqual([ self.all_items[1] ], result)
            self.assertIsNone(rs._cursor)

        self._execute_with_cursor(the_test)

    def test_close_midway(self):
        def the_test(cursor):
            rs = ResultSet(cursor, ResultSetTests.Item)
            rs.init('SELECT * FROM items', ('id', 'name', 'code'), ())
            self.assertEqual(self.all_items[0], next(rs))
            self.assertEqual(self.all_items[1], next(rs))

            rs.close()
            self.assertIsNone(rs._cursor)
            with self.assertRaises(AttributeError):
                next(rs)

        self._execute_with_cursor(the_test)

    def test_init_with_bad_statement(self):
        def the_test(cursor):
            rs = ResultSet(cursor, ResultSetTests.Item)
            with self.assertRaises(sqlite3.OperationalError):
                rs.init('SELECT * FROM no_table', ('id', 'name', 'code'), ())
            self.assertIsNone(rs._cursor)

        self._execute_with_cursor(the_test)
        
    class Item:
        def __init__(self, id, name, code):
            self.id = id
            self.name = name
            self.code = code

        def __str__(self):
            return '(%s, %s, %s)' % (self.id, self.name, self.code)

        def __repr__(self):
            return 'Item(%s, %s, %s)' % (repr(self.id), repr(self.name),
                                         repr(self.code))

        def __eq__(self, other):
            return (self.id == other.id) and (self.name == other.name) and \
                   (self.code == other.code)

        def __ne__(self, other):
            return (self.id != other.id) or (self.name != other.name) or \
                   (self.code != other.code)
            
    @classmethod
    def setUpDatabase(cls, cursor):
        cursor.execute("""
            CREATE TABLE items(
                id NUMBER PRIMARY KEY,
                name VARCHAR(64),
                code CHAR(3)
            )""")
        cursor.execute("""INSERT INTO items VALUES(1, 'Cow', 'MOO')""")
        cursor.execute("""INSERT INTO items VALUES(2, 'Penguin', 'PEG')""")
        cursor.execute("""INSERT INTO items VALUES(3, 'Bear', 'KUM')""")
        cls.db.commit()

        cls.all_items = [ ResultSetTests.Item(1, 'Cow', 'MOO'),
                          ResultSetTests.Item(2, 'Penguin', 'PEG'),
                          ResultSetTests.Item(3, 'Bear', 'KUM') ]
    

class OneColumnResultSetTests(DatabaseTestCase):
    def test_retrieve_all_items(self):
        def the_test(cursor):
            truth = [ 'Item#%d' % i.id for i in self.all_items ]
            rs = OneColumnResultSet(cursor, lambda x: 'Item#%d' % x)
            self.assertIs(rs, rs.init('SELECT id FROM items', ()))

            result = [ x for x in rs ]
            self.assertEqual(truth, result)
            self.assertIsNone(rs._cursor)

        self._execute_with_cursor(the_test)


    def test_retrieve_one_item(self):
        def the_test(cursor):
            rs = OneColumnResultSet(cursor, lambda x: 'Item#%d' % x)
            rs.init("SELECT id FROM items WHERE code = ?", ('KUM', ))

            self.assertEqual('Item#3', next(rs))
            with self.assertRaises(StopIteration):
                next(rs)
            self.assertIsNone(rs._cursor)

        self._execute_with_cursor(the_test)
        
    @classmethod
    def setUpDatabase(cls, cursor):
        cursor.execute("""
            CREATE TABLE items(
                id NUMBER PRIMARY KEY,
                name VARCHAR(64),
                code CHAR(3)
            )""")
        cursor.execute("""INSERT INTO items VALUES(1, 'Cow', 'MOO')""")
        cursor.execute("""INSERT INTO items VALUES(2, 'Penguin', 'PEG')""")
        cursor.execute("""INSERT INTO items VALUES(3, 'Bear', 'KUM')""")
        cls.db.commit()

        cls.all_items = [ ResultSetTests.Item(1, 'Cow', 'MOO'),
                          ResultSetTests.Item(2, 'Penguin', 'PEG'),
                          ResultSetTests.Item(3, 'Bear', 'KUM') ]


class StatementConstructionTests(unittest.TestCase):
    def test_prepare_variable(self):
        self.assertEqual(1, prepare_variable(1))
        self.assertEqual('Moo', prepare_variable('Moo'))

        dt = datetime.datetime(2018, 10, 1, 12, 34, 56)
        truth = (dt - datetime.datetime.utcfromtimestamp(0)).total_seconds()
        self.assertEqual(truth, prepare_variable(dt))

    def test_format_for_sql(self):
        self.assertEqual('1', format_for_sql(1))
        self.assertEqual("'Cow'", format_for_sql('Cow'))
        self.assertEqual('NULL', format_for_sql(None))

        dt = datetime.datetime(2018, 10, 1, 12, 34, 56)
        truth = \
            str((dt - datetime.datetime.utcfromtimestamp(0)).total_seconds())
        self.assertEqual(truth, format_for_sql(dt))

    def test_has_legal_constraint_type(self):
        self.assertTrue(has_legal_constraint_type(3))
        self.assertTrue(has_legal_constraint_type(3.0))

        dt = datetime.datetime(2018, 10, 1, 12, 34, 56)
        self.assertTrue(has_legal_constraint_type(dt))
        self.assertTrue(has_legal_constraint_type('Moo'))
        self.assertTrue(has_legal_constraint_type(u'Moo'))
        self.assertTrue(has_legal_constraint_type(None))

        self.assertFalse(has_legal_constraint_type([ ]))

    def test_construct_equality_constraint(self):
        dt = datetime.datetime(2018, 10, 1, 12, 34, 56)
        since_epoch = \
            (dt - datetime.datetime.utcfromtimestamp(0)).total_seconds()

        (stmt, variables) = construct_constraint('last_updated_at', dt)
        self.assertEqual('last_updated_at = ?', stmt)
        self.assertEqual((since_epoch, ), variables)

    def test_construct_set_constraint(self):
        codes = ('x', 'y', 'z')

        (stmt, variables) = construct_constraint('code', codes)
        self.assertEqual("code IN ('x', 'y', 'z')", stmt)
        self.assertEqual((), variables)
    
    def test_construct_null_constraint(self):
        (stmt, variables) = construct_constraint('last_indexed_at', None)
        self.assertEqual('last_indexed_at IS NULL', stmt)
        self.assertEqual((), variables)

    def test_construct_custom_constraint(self):
        custom = StatementConstructionTests.CustomConstraint(10)
        (stmt, variables) = construct_constraint('moo_count', custom)

        self.assertEqual('moo_count > ?', stmt)
        self.assertEqual((10, ), variables)

    def test_construct_equality_constraint_with_bad_type(self):
        with self.assertRaises(ValueError):
            construct_constraint('code', { 'a' : 1 })

    def test_construct_set_constraint_with_bad_type(self):
        with self.assertRaises(ValueError):
            construct_constraint('code', [ 'a', { 'b' : 2 }, 'c' ])

    def test_construct_empty_constraint(self):
        (stmt, variables) = construct_constraints({ })
        self.assertEqual('', stmt)
        self.assertEqual((), variables)

    def test_construct_single_clause_constraint(self):
        (stmt, variables) = construct_constraints({ 'code' : 5 })
        self.assertEqual('code = ?', stmt)
        self.assertEqual((5, ), variables)

    def test_construct_multiple_clause_constraint(self):
        criteria = { 'code' : (1, 2, 3),
                     'name' : 'Mike',
                     'custom' : \
                         StatementConstructionTests.CustomConstraint(10) }
        true_statement = '(code IN (1, 2, 3)) AND (name = ?) AND ' + \
                         '(custom > ?)'
        true_variables = ('Mike', 10)
        
        (stmt, variables) = construct_constraints(criteria)
        self.assertEqual(true_statement, stmt)
        self.assertEqual(true_variables, variables)

    def test_construct_where_clause(self):
        (stmt, variables) = construct_where_clause({ 'id' : 5 })
        self.assertEqual(' WHERE id = ?', stmt)
        self.assertEqual((5, ), variables)

        (stmt, variables) = construct_where_clause({ })
        self.assertEqual('', stmt)
        self.assertEqual((), variables)

    def test_construct_select_statement(self):
        (stmt, variables) = construct_select_statement('items', ('id', 'name'),
                                                       { 'name' : 'Kuma-chan' })
        self.assertEqual(stmt, 'SELECT id, name FROM items WHERE name = ?')
        self.assertEqual(('Kuma-chan', ), variables)

        (stmt, variables) = \
            construct_select_statement('items', ('id', ), { })
        self.assertEqual(stmt, 'SELECT id FROM items', stmt)
        self.assertEqual((), variables)

    def test_construct_count_statement(self):
        criteria = { 'cost' : StatementConstructionTests.CustomConstraint(100),
                     'type' : ('A', 'B', 'C') }
        (stmt, variables) = construct_count_statement('items', criteria)

        true_statement = "SELECT count(*) FROM items WHERE (cost > ?) AND " + \
                         "(type IN ('A', 'B', 'C'))"
        self.assertEqual(true_statement, stmt)
        self.assertEqual((100, ), variables)

        (stmt, variables) = construct_count_statement('items', { })
        self.assertEqual('SELECT count(*) FROM items', stmt)
        self.assertEqual((), variables)

    def test_construct_insert_statement(self):
        values = { 'id' : 5, 'name' : 'Tom' }
        (stmt, variables) = construct_insert_statement('items', values)
        self.assertEqual("INSERT INTO items(id, name) VALUES (5, 'Tom')", stmt)
        self.assertEqual((), variables)

    def test_construct_update_statement(self):
        values = { 'name' : 'Tom', 'department' : 'Cows' }
        criteria = { 'id' : 10 }
        (stmt, variables) = construct_update_statement('items', values,
                                                       criteria)
        true_statement = "UPDATE items SET department = 'Cows', " + \
                         "name = 'Tom' WHERE id = ?"
        self.assertEqual(true_statement, stmt)
        self.assertEqual((10, ), variables)

    def test_construct_delete_statement(self):
        criteria = { 'last_indexed_at' : None }
        (stmt, variables) = construct_delete_statement('items', criteria)

        self.assertEqual('DELETE FROM items WHERE last_indexed_at IS NULL',
                         stmt)
        self.assertEqual((), variables)

    class CustomConstraint:
        def __init__(self, t):
            self.t = t

        def to_sql(self, column):
            return ('%s > ?' % column, (self.t, ))

class StatementExecutionTests(DatabaseTestCase):
    def setUp(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("INSERT INTO items VALUES(1, 'Tom', 'MOO')")
            cursor.execute("INSERT INTO items VALUES(2, 'Cheryl', 'HRS')")
            cursor.execute("INSERT INTO items VALUES(3, 'John', 'MOO')")
            cursor.execute("INSERT INTO items VALUES(4, 'Susan', 'MOO')")
            cursor.execute("INSERT INTO items VALUES(5, 'Alan', 'HRS')")
            self.db.commit()
        finally:
            cursor.close()

        self.all_items = [ StatementExecutionTests.Item(1, 'Tom', 'MOO'),
                           StatementExecutionTests.Item(2, 'Cheryl', 'HRS'),
                           StatementExecutionTests.Item(3, 'John', 'MOO'),
                           StatementExecutionTests.Item(4, 'Susan', 'MOO'),
                           StatementExecutionTests.Item(5, 'Alan', 'HRS') ]
    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute('DELETE FROM items')
            self.db.commit()
        finally:
            cursor.close()
        
    def test_execute_count(self):
        def count_dept(d):
            return len([i for i in self.all_items if i.dept == d])
        
        self.assertEqual(count_dept('MOO'),
                         execute_count(self.db, 'items',
                                       { 'department' : 'MOO' }))
        self.assertEqual(count_dept('HRS'),
                         execute_count(self.db, 'items',
                                       { 'department' : 'HRS' }))
        self.assertEqual(0, execute_count(self.db, 'items',
                                          { 'name' : 'Max' }))
        self.assertEqual(len(self.all_items),
                         execute_count(self.db, 'items', { }))

    def test_execute_select(self):
        criteria = { 'department' : 'HRS' }
        result = [ x for x in execute_select(self.db, 'items',
                                             self.table_columns, criteria,
                                             StatementExecutionTests.Item) ]
        result.sort(key = lambda i: i.id)
        truth = [ i for i in self.all_items if i.dept == 'HRS' ]
        self.assertEqual(result, truth)

    def test_execute_insert(self):
        new_person = StatementExecutionTests.Item(6, 'Doug', 'ZZZ')
        values = { 'id' : new_person.id, 'name' : new_person.name,
                   'department' : new_person.dept }
        execute_insert(self.db, 'items', values)
        self.db.commit()

        truth = self.all_items + [ new_person, ]
        self.assertEqual(truth, self._fetch_all())

    def test_execute_update(self):
        def change_department(item):
            if item.dept == 'MOO':
                return StatementExecutionTests.Item(item.id, item.name, 'QQQ')
            return item
        
        values = { 'department' : 'QQQ' }
        criteria = { 'department' : 'MOO' }
        execute_update(self.db, 'items', values, criteria)

        truth = [ change_department(i) for i in self.all_items ]
        self.assertEqual(truth, self._fetch_all())

    def test_execute_delete(self):
        criteria = { 'name' : 'Tom' }
        execute_delete(self.db, 'items', criteria)

        truth = [ i for i in self.all_items if i.name != 'Tom' ]
        self.assertEqual(truth, self._fetch_all())

    def _fetch_all(self):
        stmt = 'SELECT %s FROM items' % ', '.join(self.table_columns)
        with ResultSet(self.db.cursor(), StatementExecutionTests.Item) as rs:
            rs.init(stmt, self.table_columns, ())
            return sorted([ x for x in rs ], key = lambda i: i.id)

    def _fetch_with_id(self, item_id):
        column_names = ', '.join(self.table_columns)
        stmt = 'SELECT %s FROM items WHERE id = ?' % column_names

        with ResultSet(self.db.cursor(), StatementExecutionTests.Item) as rs:
            rs.init(stmt, self.table_columns, (item_id, ))
            return next(rs)

    class Item:
        def __init__(self, id, name, department):
            self.id = id
            self.name = name
            self.dept = department

        def __str__(self):
            return '(%s, %s, %s)' % (self.id, self.name, self.dept)

        def __repr__(self):
            return 'Item(%s, %s, %s)' % (repr(self.id), repr(self.name),
                                         repr(self.dept))

        def __eq__(self, other):
            return (self.id == other.id) and (self.name == other.name) and \
                   (self.dept == other.dept)

        def __ne__(self, other):
            return (self.id != other.id) or (self.name != other.name) or \
                   (self.dept != other.dept)

    @classmethod
    def setUpDatabase(cls, cursor):
        cursor.execute("""
            CREATE TABLE items(
                id NUMBER PRIMARY KEY,
                name VARCHAR(64),
                department CHAR(3)
            )""")
        cls.db.commit()

        cls.table_columns = ('id', 'name', 'department')


class NextItemIdTests(DatabaseTestCase):
    def test_next_id(self):
        self.assertEqual(1, next_item_id(self.db, 'cows'))
        self.assertEqual(2, next_item_id(self.db, 'cows'))
        self.assertEqual(1, next_item_id(self.db, 'penguins'))

    @classmethod
    def setUpDatabase(cls, cursor):
        cursor.execute("""
            CREATE TABLE id_sequence(
                table_name VARCHAR(256) PRIMARY KEY,
                id NUMBER
            )""")
    
if __name__ == '__main__':
    unittest.main()

