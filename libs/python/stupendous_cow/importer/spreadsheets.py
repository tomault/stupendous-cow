"""Classes and functions for reading spreadsheets.  Currently a wrapper over
pyexcel."""
import pyexcel

class SpreadsheetPath:
    def __init__(self, sheet, column):
        self.sheet = sheet
        self.column = column

    def __str__(self):
        return '@%s[%s]' % (self.sheet, self.column)

    def __repr__(self):
        return 'SpreadsheetPath(%s, %s)' % (repr(self.sheet), repr(self.column))


class Row:
    def __init__(self, column_names, column_map, data):
        self._column_names = column_names
        self._column_map = column_map
        self._length = len(column_names)
        self._data = data

    @property
    def columns(self):
        return self._column_names

    def __len__(self):
        return self._length

    def __getitem__(self, index):
        if isinstance(index, str) or isinstance(index, unicode):
            return self._value_at(self._name_to_column(index))
        elif isinstance(index, int) or isinstance(index, long):
            return self._get_by_position(index)
        elif isinstance(index, slice):
            raise TypeError('Row slices not supported yet')
        else:
            raise TypeError('Row index must be an int, long, str or unicode')

    def _get_by_position(self, pos):
        i = self._normalize_index(pos)
        if (i < 0) or (i >= self._length):
            msg = 'Index %d is out of range in a row of length %d'
            raise IndexError(msg % (i, self._length))
        return self._value_at(i)

    def _normalize_index(self, i):
        if i < 0:
            return self._length + i
        return i

    def _value_at(self, index):
        if index > len(self._data):
            return None
        return self._data[index]
    
    def _name_to_column(self, name):
        try:
            return self._column_map[name]
        except KeyError:
            raise IndexError('No such column "%s"' % name)

class Worksheet:
    def __init__(self, sheet):
        self._sheet = sheet
        rows = iter(sheet)
        try:
            self._column_names = next(rows)
        except StopIteration:
            self._column_names = [ ]
        self._column_map = \
            dict((x, n) for (n, x) in enumerate(self._column_names))
        if len(self._column_names) < self._sheet.number_of_columns():
            missing = self._sheet.number_of_columns()
            self._column_names += [ '' ] * missing

    @property
    def name(self):
        return self._sheet.name

    @property
    def num_columns(self):
        return len(self._column_names)
    
    @property
    def columns(self):
        return self._column_names

    def __iter__(self):
        rows = iter(self._sheet)

        next(rows)  # Skip header row
        while True:
            yield Row(self._column_names, self._column_map, next(rows))
        
class Workbook:
    def __init__(self, filename):
        self._filename = filename
        self._book = pyexcel.get_book(file_name = filename)

    @property
    def num_sheets(self):
        return self._book.number_of_sheets()
    
    @property
    def sheet_names(self):
        return self._book.sheet_names()

    def __getitem__(self, index):
        return Worksheet(self._book[index])
