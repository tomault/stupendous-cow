"""Classes and functions to help out with unit testing"""

import sqlite3
import unittest

class DatabaseTestCase(unittest.TestCase):
    def _execute_with_cursor(self, test_case):
        cursor = self.db.cursor()
        try:
            test_case(cursor)
        finally:
            cursor.close()

    def _verify_item_list(self, name, truth, items, compute_diffs):
        i = 0
        for (true_item, item) in zip(truth, items):
            diffs = compute_diffs(true_item, item)
            if diffs:
                args = (name, i, ', '.join(diffs))
                self.fail('The %s at index %d differ: %s' % args)

        if len(items) > len(truth):
            args = (name, ', '.join(repr(i) for i in items[len(truth):]))
            self.fail('There are extra %s: %s' % args)

        if len(items) < len(truth):
            args = (name, ', '.join(repr(i) for i in truth[len(items):]))
            self.fail('There are extra %s: %s' % args)

    def _compute_item_diffs(self, truth, item, fields):
        diffs = [ ]
        for field in fields:
            left = getattr(truth, field)
            right = getattr(item, field)
            if left != right:
                diffs.append('%s (%s != %s)' % (field, repr(left), repr(right)))
        return diffs

    def _verify_enum_constant_list(self, name, truth, items):
        def compute_ec_diffs(left, right):
            return self._compute_item_diffs(left, right, ('id', 'name'))
        return self._verify_item_list(name, truth, items, compute_ec_diffs)

    @classmethod
    def setUpClass(cls):
        cls.db = sqlite3.connect(':memory:')
        cursor = cls.db.cursor()
        try:
            cls.setUpDatabase(cursor)
        finally:
            cursor.close()

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    @classmethod
    def setUpDatabase(cls, cursor):
        raise RuntimeError("DatabaseTestCase.setUpDatabase() not implemented")

