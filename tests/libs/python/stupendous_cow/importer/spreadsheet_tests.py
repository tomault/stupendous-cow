from stupendous_cow.importer.spreadsheets import *
import inspect
import os.path
import unittest

resource_dir = None

class WorkbookTests(unittest.TestCase):
    def test_load_rworkbook(self):
        workbook = Workbook(os.path.join(resource_dir, 'test_ss.ods'))
        self.assertEqual(2, workbook.num_sheets)
        self.assertEqual(('alpha', 'beta'), tuple(workbook.sheet_names))

    def test_worksheet_properties(self):
        workbook = Workbook(os.path.join(resource_dir, 'test_ss.ods'))
        sheet = workbook['alpha']
        self.assertEqual('alpha', sheet.name)
        self.assertEqual(4, sheet.num_columns)
        self.assertEqual(('A', 'B', 'C', 'D'), tuple(sheet.columns))

        sheet = workbook['beta']
        self.assertEqual('beta', sheet.name)
        self.assertEqual(3, sheet.num_columns)
        self.assertEqual(('TYPE', 'TITLE', 'RATING'), tuple(sheet.columns))

    def test_read_row_by_position(self):
        workbook = Workbook(os.path.join(resource_dir, 'test_ss.ods'))
        sheet = workbook['alpha']
        row = next(iter(sheet))

        self.assertEqual(4, len(row))
        self.assertEqual(('A', 'B', 'C', 'D'), tuple(row.columns))

        data = [ row[i] for i in xrange(0, len(row)) ]
        self.assertEqual([ 10, 'abc', 1.5, 'cows' ], data)

    def test_read_row_by_name(self):
        workbook = Workbook(os.path.join(resource_dir, 'test_ss.ods'))
        sheet = workbook['alpha']
        row = next(iter(sheet))

        self.assertEqual(4, len(row))
        self.assertEqual(('A', 'B', 'C', 'D'), tuple(row.columns))

        data = [ row[x] for x in row.columns ]
        self.assertEqual([ 10, 'abc', 1.5, 'cows' ], data)

    def test_read_rows(self):
        workbook = Workbook(os.path.join(resource_dir, 'test_ss.ods'))
        sheet = workbook['alpha']

        data = [ [ row[x] for x in row.columns ] for row in sheet ]
        self.assertEqual(3, len(data))
        self.assertEqual([ 10, 'abc', 1.5, 'cows' ], data[0])
        self.assertEqual([ 20, 'def', 2.25, 'penguins' ], data[1])
        self.assertEqual([ 30, 'ghi', 3.5, 'love' ], data[2])
        
        
def set_resource_dir():
    global resource_dir
    (path, _) = os.path.split(os.path.abspath(inspect.stack()[0][1]))
    path = os.path.join(path, '../../../../resources/importer')
    path = os.path.realpath(os.path.normpath(path))
    resource_dir = path
    
if __name__ == '__main__':
    set_resource_dir()
    unittest.main()

