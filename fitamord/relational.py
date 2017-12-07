"""Tools for relational data"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import itertools as itools
import operator

from .records import RecordStream
from .collections import NamedItems


class Relations(NamedItems):
    """A collection of relations"""

    def __init__(self, *relations):
        super().__init__()
        for relation in relations:
            self.add(relation)

    def add(self, relation):
        if not isinstance(relation, RecordStream):
            raise ValueError('Not a relation: {!r}'.format(relation))
        super().add(relation.name, relation)

    relations = NamedItems.items

    names_relations = NamedItems.names_items

    relation_at = NamedItems.item_at

    relation_of = NamedItems.item_of


class CollectedRecords(NamedItems):

    def __init__(self, groupby_key, relations):
        super().__init__()
        self._groupby_key = groupby_key
        self._relations = relations
        for name in relations.names():
            self.add(name, [])

    @property
    def groupby_key(self):
        return self._groupby_key

    @property
    def relations(self):
        return self._relations

    records = NamedItems.items

    names_records = NamedItems.names_items

    records_at = NamedItems.item_at

    records_of = NamedItems.item_of


class MergeCollect(Relations):
    """Collects all the records from multiple relations together by key.

    Yields a group of records for each such key.  To efficiently collect
    records by key, the relations are first sorted by key and then
    "merged" by key.

    This is similar to doing a natural (equi-) join by key except that
    the records are returned as a collection rather than as the
    concatenated tuples of their Cartesian product (which is wastefully
    large and not as useful compared to having the records themselves).

    """


    def __init__(self, *relations, key=0):
        """Creates an object that merge-collects the given relations.

        A relation is either a record stream or a (record stream, key)
        pair.  A record stream is either a RecordStream object or a
        (name, header, records) tuple.  A key is either a name or index
        and indicates the column the relation is sorted on.  If the key
        is missing or None, it is defaulted to 0, indicating the first
        column.

        """
        super().__init__()
        self._groupbys = []
        arg_key = key
        for relation in relations:
            # Interpret, validate, and add relation
            relation, key = self._interpret_as_ordered_relation(
                relation, key=arg_key)
            self.add(relation)
            # Sort the relation by key
            relation = relation.order_by(key)
            # Get the column index
            col_idx = relation.header.index_of(key)
            # Construct the group by for the key
            self._groupbys.append(
                itools.groupby(relation.records(),
                               operator.itemgetter(col_idx)))

    @staticmethod
    def _interpret_as_ordered_relation(obj, key=0):
        relation = None
        if isinstance(obj, RecordStream):
            relation = obj
        elif isinstance(obj, tuple) and len(obj) == 2:
            rel_spec, key = obj
            if isinstance(rel_spec, RecordStream):
                relation = rel_spec
            elif isinstance(rel_spec, tuple) and len(rel_spec) == 3:
                name, header, records = rel_spec
                relation = RecordStream(
                    name=name,
                    header=header,
                    records=records,
                    )
            else:
                raise ValueError(
                    'Not a relation specification: {!r}'
                    .format(rel_spec))
        else:
            raise ValueError(
                'Not a relation specification: {!r}'.format(obj))
        # Check that the relation has a name
        if not relation.has_name():
            raise ValueError(
                'Relation does not have a name: {!r}'.format(relation))
        # Check that the relation has a header
        if not relation.has_header():
            raise ValueError(
                'Relation does not have a header: {!r}'
                .format(relation))
        # Check that the key refers to a column in the relation
        header = relation.header
        if key not in header:
            raise ValueError(
                '{}: Column not found: {!r}'.format(relation.name, key))
        return relation, key

    def merge_collect(self):
        """Silently skips any records whose key is None"""
        # Get the initial key-group pairs
        keys_groups = [next(gb, None) for gb in self._groupbys]
        # Increment until all the keys are not None (discard groups with
        # keys that are None)
        for idx in range(len(keys_groups)):
            while (keys_groups[idx] is not None
                   and keys_groups[idx][0] is None):
                keys_groups[idx] = next(self._groupbys[idx], None)
        # Get the initial keys (which should not be None unless the
        # corresponding group-by iterator is exhausted)
        keys = [(kg[0] if kg is not None else None)
                for kg in keys_groups]
        # Loop while any of the groupby iterators have items
        while any(keys_groups):
            # Find the indices of the minimum keys
            min_key, min_idxs = indices_of_minimums(keys)
            # Build the collection of records
            records = CollectedRecords(min_key, self)
            for min_idx in min_idxs:
                records.add(
                    self.name_at(min_idx),
                    list(keys_groups[min_idx][1]))
            yield records
            # Increment
            for min_idx in min_idxs:
                # Get the next group
                keys_groups[min_idx] = next(
                    self._groupbys[min_idx], None)
                # Increment until the key is not None (discard groups
                # with keys that are None)
                while (keys_groups[min_idx] is not None
                       and keys_groups[min_idx][0] is None):
                    keys_groups[min_idx] = next(
                        self._groupbys[min_idx], None)
                # Get the key for the next group
                keys[min_idx] = (keys_groups[min_idx][0]
                                 if keys_groups[min_idx] is not None
                                 else None)

    __iter__ = merge_collect


def indices_of_minimums(values): # TODO move somewhere appropriate
    min_value = None
    min_idxs = []
    for idx, value in enumerate(values):
        if value is None:
            continue
        if min_value is None or value < min_value:
            min_value = value
            min_idxs = [idx]
        elif value == min_value:
            min_idxs.append(idx)
    return min_value, min_idxs
