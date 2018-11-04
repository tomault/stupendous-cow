"""Unit tests for stupendous_cow.db.table"""

from stupendous_cow.db.tables import *
from stupendous_cow.db.core import ResultSet, OneColumnResultSet, execute_count
from stupendous_cow.testing import DatabaseTestCase
import unittest

class Department:
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'Department(%s, %s)' % (repr(self.id), repr(self.name))

    def __eq__(self, other):
        return (self.id == other.id) and (self.name == other.name)

    def __ne__(self, other):
        return (self.id != other.id) or (self.name != other.name)

class DepartmentTable(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'Department', db, 'departments',
                           ('id', 'name'), Department, getattr)

    def _count_references_to(self, item_id):
        if item_id == 3:
            return 0
        return 1

class Employee:
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

class EmployeeTable(Table):
    departments = (Department(1, 'MOO'), Department(2, 'HRS'),
                   Department(3, 'ZZZ'))
    
    def __init__(self, db):
        Table.__init__(self, 'Employee', db, 'emp',
                       ('id', 'name', 'dept_id'), self._create_employee,
                       self._get_column_value)

    def _create_employee(self, id, name, dept_id):
        department = [ x for x in self.departments if x.id == dept_id ][0]
        return Employee(id, name, department)

    def _get_column_value(self, employee, column):
        if column == 'dept_id':
            return employee.dept.id if employee.dept else None
        return getattr(employee, column)

    def _normalize_criteria(self, criteria):
        def extract_ids(name, items, item_type):
            if isinstance(items, item_type):
                return items.id
            elif isinstance(items, tuple) or isinstance(items, list):
                return [ i.id for i in items ]
            else:
                msg = 'Invalid Employee constraint "%s" -- must be a ' + \
                      'single %s, a tuple of %s or a list of %s'
                msg = msg % (constraint_name, item_type, item_type, item_type)
                raise ValueError(msg)
               
        def normalize_constraint(name, value):
            if name == 'department':
                if 'dept_id' in criteria:
                    msg = 'Invalid Employee constraints -- cannot specify ' + \
                          '"department" and "department_id" constraints ' + \
                          'simultaneously'
                    raise ValueError(msg)
                return ('dept_id',
                        extract_ids('department', value, Department))
            return (name, value)

        return dict(normalize_constraint(n, v) \
                        for (n, v) in criteria.iteritems())

    def _set_defaults_for_write(self, values):
        if not values['dept_id']:
            values['dept_id'] = 1
            
class TableTests(DatabaseTestCase):
    def setUp(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("INSERT INTO emp VALUES(1, 'Tom', 1)")
            cursor.execute("INSERT INTO emp VALUES(2, 'Cheryl', 2)")
            cursor.execute("INSERT INTO emp VALUES(3, 'John', 1)")
            cursor.execute("INSERT INTO emp VALUES(4, 'Susan', 1)")
            cursor.execute("INSERT INTO emp VALUES(5, 'Alan', 2)")

            cursor.execute("INSERT INTO id_sequence VALUES('emp', 6)")
            self.db.commit()
        finally:
            cursor.close()

        depts = EmployeeTable.departments
        self.all_employees = [ Employee(1, 'Tom', depts[0]),
                               Employee(2, 'Cheryl', depts[1]),
                               Employee(3, 'John', depts[0]),
                               Employee(4, 'Susan', depts[0]),
                               Employee(5, 'Alan', depts[1]) ]

        self.table = EmployeeTable(self.db)

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM emp")
            cursor.execute("DELETE FROM id_sequence")
            self.db.commit()
        finally:
            cursor.close()

    def test_properties(self):
        self.assertEqual(sorted([x.id for x in self.all_employees ]),
                         sorted(self.table.ids))
        self.assertEqual(self.all_employees,
                         sorted(self.table.all, key = lambda x: x.id))

    def test_count(self):
        cow_department = EmployeeTable.departments[0]
        
        self.assertEqual(1, self.table.count(name = 'Susan'))
        self.assertEqual(3, self.table.count(department = cow_department))
        self.assertEqual(2, self.table.count(department = cow_department,
                                             name = ('Tom', 'Cheryl', 'John')))

    def test_retrieve(self):
        depts = EmployeeTable.departments
        emps = self.all_employees

        with self.table.retrieve(name = 'Alan') as results:
            self.assertEqual([ emps[4] ], [ x for x in results ])

        with self.table.retrieve(department = depts[1]) as results:
            employees = sorted([ x for x in results ], key = lambda y: y.id)
        self.assertEqual([ emps[1], emps[4] ], employees)

        with self.table.retrieve(department = (depts[0], depts[2]),
                                 name = ('Cheryl', 'John', 'Susan')) as results:
            employees = sorted([ x for x in results ], key = lambda y: y.id)
        self.assertEqual([ emps[2], emps[3] ], employees)

    def test_with_id(self):
        for emp in self.all_employees:
            self.assertEqual(emp, self.table.with_id(emp.id))

        self.assertIsNone(self.table.with_id(0))

    def test_add(self):
        depts = EmployeeTable.departments

        new_employee = Employee(None, 'Margaret', depts[1])
        created = self.table.add(new_employee)

        self.assertEqual(6, created.id)
        self.assertEqual(new_employee.name, created.name)
        self.assertEqual(new_employee.dept, created.dept)
        self.assertEqual(self.all_employees + [ created ],
                         self._retrieve_all())

    def test_add_existing_employee(self):
        with self.assertRaises(ValueError):
            self.table.add(self.all_employees[0])

    def test_add_with_default_department(self):
        new_employee = Employee(None, 'Marcus', None)
        created = self.table.add(new_employee)

        self.assertEqual(6, created.id)
        self.assertEqual(new_employee.name, created.name)
        self.assertEqual(EmployeeTable.departments[0], created.dept)
        self.assertEqual(self.all_employees + [ created ], self._retrieve_all())

    def test_update(self):
        emp = self.all_employees[0]
        emp.name = 'Thomas'
        emp.dept = EmployeeTable.departments[1]

        self.table.update(emp)

        self.assertEqual(self.all_employees, self._retrieve_all())

    def test_update_with_no_id(self):
        emp = Employee(None, 'Thomas', EmployeeTable.departments[1])
        with self.assertRaises(ValueError):
            self.table.update(emp)

    def test_update_nonexistent_employee(self):
        emp = Employee(6, 'David', EmployeeTable.departments[0])
        with self.assertRaises(ValueError):
            self.table.update(emp)

    def test_update_with_default_department(self):
        emp = self.all_employees[1]
        emp.name = 'Cherlyn'
        emp.dept = None

        self.table.update(emp)

        emp.dept = EmployeeTable.departments[0]
        self.assertEqual(self.all_employees, self._retrieve_all())

    def test_delete(self):
        new_employees = [ e for e in self.all_employees if e.id != 3 ]

        self.table.delete(3)
        self.assertEqual(new_employees, self._retrieve_all())

    def test_delete_nonexistent_id(self):
        self.table.delete(6)
        self.assertEqual(self.all_employees, self._retrieve_all())

    def _retrieve_all(self):
        def create_employee(id, name, dept_id):
            dept = [ d for d in EmployeeTable.departments if d.id == dept_id][0]
            return Employee(id, name, dept)
        
        with ResultSet(self.db.cursor(), create_employee) as rs:
            rs.init('SELECT id, name, dept_id FROM emp',
                    ('id', 'name', 'dept_id'), ())
            return sorted([ e for e in rs ], key = lambda x: x.id)

    @classmethod
    def setUpDatabase(cls, cursor):
        cursor.execute("""
            CREATE TABLE id_sequence(
                table_name VARCHAR(256) PRIMARY KEY,
                id NUMBER
            )""")
        cursor.execute("""
            CREATE TABLE emp(
                id NUMBER PRIMARY KEY,
                name VARCHAR(256),
                dept_id NUMBER
            )""")

        cls.table_columns = ('id', 'name', 'dept_id')

class EnumTableTests(DatabaseTestCase):
    def setUp(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("INSERT INTO departments VALUES(1, 'MOO')")
            cursor.execute("INSERT INTO departments VALUES(2, 'HRS')")
            cursor.execute("INSERT INTO departments VALUES(3, 'ZZZ')")

            cursor.execute("INSERT INTO id_sequence VALUES('departments', 4)")
            self.db.commit()
        finally:
            cursor.close()

        self.table = DepartmentTable(self.db)
        self.all_depts = [ Department(1, 'MOO'), Department(2, 'HRS'),
                           Department(3, 'ZZZ') ]

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute('DELETE FROM departments')
            cursor.execute('DELETE FROM id_sequence')
        finally:
            cursor.close()

    def test_all_property(self):
        self.assertEqual(self.all_depts,
                         sorted(self.table.all, key = lambda x: x.id))

    def test_with_id(self):
        for dept in self.all_depts:
            self.assertEqual(dept, self.table.with_id(dept.id))
        self.assertIsNone(self.table.with_id(0))

    def test_with_name(self):
        for dept in self.all_depts:
            self.assertEqual(dept, self.table.with_name(dept.name))
        self.assertIsNone(self.table.with_name('Urgh'))

    def test_count_references_to(self):
        depts = self.all_depts
        self.assertEqual(1, self.table.count_references_to(depts[0]))
        self.assertEqual(1, self.table.count_references_to(depts[1]))
        self.assertEqual(0, self.table.count_references_to(depts[2]))

    def test_add(self):
        new_dept = Department(None, 'USH')
        created = self.table.add(new_dept)

        self.assertEqual(4, created.id)
        self.assertEqual(new_dept.name, created.name)

        self.assertEqual(self.all_depts + [ created ], self._retrieve_all())

        self.assertEqual(self.all_depts + [ created ], self.table.all)
        self.assertEqual(created, self.table.with_id(created.id))
        self.assertEqual(created, self.table.with_name(created.name))

    def test_add_dept_with_id(self):
        with self.assertRaises(ValueError):
            self.table.add(self.all_depts[0])

    def test_add_dept_with_existing_name(self):
        with self.assertRaises(ValueError):
            self.table.add(Department(None, self.all_depts[0].name))

    def test_update(self):
        dept = self.all_depts[0]
        old_name = dept.name
        dept.name = 'COW'

        self.table.update(dept)
        self.assertEqual(self.all_depts, self._retrieve_all())
        self.assertEqual(self.all_depts, self.table.all)
        self.assertEqual(dept, self.table.with_id(dept.id))
        self.assertEqual(dept, self.table.with_name(dept.name))
        self.assertIsNone(self.table.with_name(old_name))

    def test_update_item_with_no_id(self):
        with self.assertRaises(ValueError):
            self.table.update(Department(None, 'QQQ'))

    def test_update_nonexistent_item(self):
        with self.assertRaises(ValueError):
            self.table.update(Department(4, 'QQQ'))

    def test_delete(self):
        to_delete = self.all_depts[2]
        remaining = [ x for x in self.all_depts if x.id != to_delete.id ]

        self.table.delete(to_delete)
        self.assertEqual(remaining, self._retrieve_all())
        self.assertEqual(remaining, self.table.all)

        for dept in remaining:
            self.assertEqual(dept, self.table.with_id(dept.id))
            self.assertEqual(dept, self.table.with_name(dept.name))
        self.assertEqual(None, self.table.with_id(to_delete.id))
        self.assertEqual(None, self.table.with_name(to_delete.name))

    def test_delete_item_with_no_id(self):
        with self.assertRaises(ValueError):
            self.table.delete(Department(None, 'ZZZ'))
        self.assertEqual(self.all_depts, self._retrieve_all())

    def test_delete_nonexistent_item(self):
        with self.assertRaises(ValueError):
            self.table.delete(Department(4, 'QQQ'))
        self.assertEqual(self.all_depts, self._retrieve_all())

    def test_delete_item_with_inconsistent_name(self):
        with self.assertRaises(ValueError):
            self.table.delete(Department(3, 'QQQ'))
        self.assertEqual(self.all_depts, self._retrieve_all())

    def test_delete_item_with_references_to_it(self):
        with self.assertRaises(ValueError):
            self.table.delete(self.all_depts[0])
        self.assertEqual(self.all_depts, self._retrieve_all())
        
    def _retrieve_all(self):
        with ResultSet(self.db.cursor(), Department) as rs:
            rs.init('SELECT id, name FROM departments', ('id', 'name'), ())
            return sorted([ x for x in rs ], key = lambda y: y.id)

    @classmethod
    def setUpDatabase(cls, cursor):
        cursor.execute("""
            CREATE TABLE id_sequence (
                table_name VARCHAR(256) PRIMARY KEY,
                id NUMBER
            )""")
        cursor.execute("""
            CREATE TABLE departments (
                id NUMBER PRIMARY KEY,
                name VARCHAR(256)
            )""")

        cls.table_columns = ('id', 'name')
        
if __name__ == '__main__':
    unittest.main()
