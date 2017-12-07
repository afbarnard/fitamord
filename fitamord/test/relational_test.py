"""Tests `relational.py`"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import unittest

from .. import records
from .. import relational


# Tables of test data

# Name  Exclusive
# *     0
# ints  1
# strs  2
# flts  3
# blns  4

# IDs
_ids_all = set(range(10))
_ids_exclusive = set(range(5))
_ids_common = _ids_all - _ids_exclusive
_ids_ints = sorted({1} | _ids_common)
_ids_strs = sorted({2} | _ids_common)
_ids_flts = sorted({3} | _ids_common)
_ids_blns = sorted({4} | _ids_common)

# Data
_records = (
    # for _ in range(10): print("({!r:>1}, 'int', {!r:>3}),".format(random.choice(_ids_ints), random.randrange(1000)))
    (9, 'int', 885),
    (1, 'int', 280),
    (6, 'int', 351),
    (8, 'int', 856),
    (8, 'int', 344),
    (1, 'int', 842),
    (5, 'int', 532),
    (9, 'int', 894),
    (8, 'int',  92),
    (8, 'int',   5),

    # for _ in range(10): print("({!r:>1}, 'str', {!r}),".format(random.choice(_ids_strs), random.choice(string.ascii_lowercase)))
    (8, 'str', 'q'),
    (2, 'str', 'u'),
    (9, 'str', 'i'),
    (9, 'str', 'k'),
    (6, 'str', 't'),
    (2, 'str', 'u'),
    (9, 'str', 'a'),
    (2, 'str', 'f'),
    (6, 'str', 'z'),
    (7, 'str', 'r'),

    # for _ in range(10): print("({!r:>1}, 'flt', {:>5.3f}),".format(random.choice(_ids_flts), random.random()))
    (5, 'flt', 0.052),
    (5, 'flt', 0.716),
    (9, 'flt', 0.887),
    (7, 'flt', 0.452),
    (3, 'flt', 0.681),
    (8, 'flt', 0.545),
    (3, 'flt', 0.496),
    (6, 'flt', 0.388),
    (8, 'flt', 0.989),
    (6, 'flt', 0.013),

    # for _ in range(10): print("({!r:>1}, 'bln', {!r:<5}),".format(random.choice(_ids_blns), random.choice((False, True))))
    (4, 'bln', False),
    (8, 'bln', True ),
    (7, 'bln', True ),
    (4, 'bln', False),
    (9, 'bln', False),
    (7, 'bln', False),
    (5, 'bln', False),
    (7, 'bln', True ),
    (9, 'bln', False),
    (4, 'bln', False),
)

# Tables
_tab_ints = records.RecordStream(
    name='ints',
    header=records.Header(
        ('id', int),
        ('val', int),
    ),
    records=[r for r in _records if r[1] == 'int'],
)

_tab_strs = records.RecordStream(
    name='strs',
    header=records.Header(
        ('id', int),
        ('val', str),
    ),
    records=[r for r in _records if r[1] == 'str'],
)

_tab_flts = records.RecordStream(
    name='flts',
    header=records.Header(
        ('id', int),
        ('val', float),
    ),
    records=[r for r in _records if r[1] == 'flt'],
)

_tab_blns = records.RecordStream(
    name='blns',
    header=records.Header(
        ('id', int),
        ('val', bool),
    ),
    records=[r for r in _records if r[1] == 'bln'],
)

# Collected records
_records_collected_by_id = {
    k: [(t + 's',
         [r for r in _records if r[0] == k and r[1] == t])
        for t in ('int', 'str', 'flt', 'bln')]
    for k in range(10)
}


class RelationsTest(unittest.TestCase):

    def test_names_relations(self):
        # Tests chain of `add` calls
        rels = relational.Relations(
            _tab_ints, _tab_strs, _tab_flts, _tab_blns)
        nms_rels = [
            ('ints', _tab_ints),
            ('strs', _tab_strs),
            ('flts', _tab_flts),
            ('blns', _tab_blns),
        ]
        self.assertEqual(nms_rels, list(rels.names_relations()))


class CollectedRecordsTest(unittest.TestCase):

    _tables = relational.Relations(
        _tab_ints, _tab_strs, _tab_flts, _tab_blns)

    def setUp(self):
        self._recs_empty = relational.CollectedRecords(0, self._tables)
        self._recs_full = relational.CollectedRecords(9, self._tables)
        self.collect_records(self._recs_full, self._tables, 9)

    @staticmethod
    def collect_records(clctd_recs, tables, id_):
        for table in tables:
            clctd_recs.add(
                table.name,
                [r for r in table.records() if r[0] == id_])

    def test_names(self):
        names = [t.name for t in self._tables]
        self.assertEqual(names, list(self._recs_empty.names()))
        self.assertEqual(names, list(self._recs_full.names()))

    def test_names_records(self):
        self.assertEqual(
            _records_collected_by_id[self._recs_empty.groupby_key],
            list(self._recs_empty.names_records()))
        self.assertEqual(
            _records_collected_by_id[self._recs_full.groupby_key],
            list(self._recs_full.names_records()))

    def test_all_records_iterable(self):
        for clctn in (self._recs_empty, self._recs_full):
            for recs in clctn.records():
                for rec in recs: # Error here if `recs` not iterable
                    pass


class MergeCollectTest(unittest.TestCase):

    def test_empty_constructor(self):
        mc = relational.MergeCollect()
        self.assertEqual(0, len(list(mc)))

    def test_empty_relations(self):
        mc = relational.MergeCollect(
            *(((t.name, t.header, []), 0)
              for t in (_tab_ints, _tab_strs, _tab_flts, _tab_blns)))
        self.assertEqual(0, len(list(mc)))

    def test_merge_collect(self):
        # Sort the records by ID # TODO replace RecordStream with Table that implements `order_by`
        mc = relational.MergeCollect(
            *(((t.name, t.header, sorted(t, key=lambda r: r[0])), 0)
              for t in (_tab_ints, _tab_strs, _tab_flts, _tab_blns)))
        for idx, reccoll in enumerate(mc):
            # Check record collection
            self.assertEqual(
                _records_collected_by_id[reccoll.groupby_key],
                list(reccoll.names_records()))
            # Check sorted order
            self.assertEqual(idx + 1, reccoll.groupby_key)
