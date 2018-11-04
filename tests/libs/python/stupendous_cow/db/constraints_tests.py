"""Unit tests for stupendous_cow.db.constraints"""

from stupendous_cow.db.constraints import *
import datetime
import unittest

class ConstraintsTests(unittest.TestCase):
    def setUp(self):
        self.dt = datetime.datetime(2018, 10, 2, 12, 34, 56)
        self.epoch_time = datetime.datetime.utcfromtimestamp(0)
        self.since_epoch = (self.dt - self.epoch_time).total_seconds()

    def test_less_than_constraint(self):
        self.assertEqual(('foo < ?', (self.since_epoch, )),
                         LessThan(self.dt).to_sql('foo'))

    def test_less_equal_constraint(self):
        self.assertEqual(('foo <= ?', (self.since_epoch, )),
                         LessEqual(self.dt).to_sql('foo'))

    def test_greater_than_constraint(self):
        self.assertEqual(('foo > ?', (self.since_epoch, )),
                         GreaterThan(self.dt).to_sql('foo'))

    def test_greater_equal_constraint(self):
        self.assertEqual(('foo >= ?', (self.since_epoch, )),
                         GreaterEqual(self.dt).to_sql('foo'))

    def test_in_range_constraint(self):
        dt_later = self.dt + datetime.timedelta(seconds = 1000)
        seconds_later = (dt_later - self.epoch_time).total_seconds()

        truth = ('(foo >= ?) AND (foo < ?)', (self.since_epoch, seconds_later))
        constraint = InRange(self.dt, dt_later)
        self.assertEqual(truth, constraint.to_sql('foo'))

        truth = ('(foo >= ?) AND (foo <= ?)', (self.since_epoch, seconds_later))
        constraint = InRange(self.dt, dt_later, high_exclusive = False)
        self.assertEqual(truth, constraint.to_sql('foo'))
        
        truth = ('(foo > ?) AND (foo < ?)', (self.since_epoch, seconds_later))
        constraint = InRange(self.dt, dt_later, low_exclusive = True)
        self.assertEqual(truth, constraint.to_sql('foo'))

        truth = ('(foo > ?) AND (foo <= ?)', (self.since_epoch, seconds_later))
        constraint = InRange(self.dt, dt_later, low_exclusive = True,
                             high_exclusive = False)
        self.assertEqual(truth, constraint.to_sql('foo'))

    def test_is_null_constraint(self):
        self.assertEqual(('foo IS NULL', ()), IsNull.to_sql('foo'))

    def test_is_not_null_constraint(self):
        self.assertEqual(('foo IS NOT NULL', ()), NotNull.to_sql('foo'))

if __name__ == '__main__':
    unittest.main()

