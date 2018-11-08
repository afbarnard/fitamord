"""Functionality for generating feature vectors from relational data"""

# Copyright (c) 2018 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


# Features are functions that convert a set of input fields to a value.
# They have input and output types.  They have a conversion type.  For
# basic cases, the function can be determined from the input, output,
# and conversion types.
#
# A feature can apply to multiple fields, and a field can be relevant to
# multiple features.  While it would be possible to apply each feature
# to each example, features are likely to be sparse in their
# applicability, so one could look them up by a key related to their
# applicability.  For example, such a key could be (table_name,
# field_name).


from enum import Enum
import io

from barnapy import files
from barnapy import logging
import esal

from . import general
from . import records


class RandomVariableType(Enum): # TODO fix this at some point: need proper RandomVariable object with a domain of values and how those values can be interpreted, their algebraic structure (order, interval, field)
    """Types of random variables"""
    none = 0 # None or unknown, the null type
    binary = 1
    categorical = 2
    ordinal = 3
    count = 4
    interval = 5
    # differentiate finite and infinite discrete? different distributions apply
    continuous = 6

    def is_continuous(self):
        return self == RandomVariableType.continuous

    def is_discrete(self):
        return (self != RandomVariableType.none
                and self != RandomVariableType.continuous)


class SetEncoding(Enum):
    """Encodings of sets"""
    none = 0 # None or unknown, the null encoding
    values = 1 # Each value in the set is itself
    indices = 2 # Values are encoded by their 1-based index in the list of sorted values
    indicators = 3 # Values are encoded as binary indicators


def str_w_empty_none(obj):
    if obj is None:
        return ''
    return str(obj)


def make_identifier(*objs):
    return '-'.join(
        str_w_empty_none(o).strip().replace(' ', '_') for o in objs)


class Feature: # TODO rework to separate out random variable aspects from association to relation

    # Features do not have IDs because they need to be numbered as a
    # contiguous set of 1-based indices.  Maintaining this is
    # incompatible with allowing users to set their own IDs.

    _data_to_rv_types = {
        bool: RandomVariableType.binary,
        float: RandomVariableType.continuous,
        int: RandomVariableType.continuous,
        str: RandomVariableType.categorical,
        object: RandomVariableType.categorical,
        }

    _names_to_data_types = {
        'bool': bool,
        'int': int,
        'float': float,
        'object': object,
        'str': str,
        }

    field_names = (
        'name', 'table_name', 'field_name', 'value', 'data_type', 'rv_type')

    def __init__( # TODO validate arguments
            self,
            name=None,
            table_name=None,
            field_name=None,
            value=None,
            data_type=None,
            rv_type=None,
            function=None,
            ):
        self._name = name
        self._table_name = table_name
        self._field_name = field_name
        self._value = value
        self._data_type = (
            data_type
            if isinstance(data_type, type)
            else self._names_to_data_types.get(data_type, object))
        self._rv_type = None
        if isinstance(rv_type, RandomVariableType):
            self._rv_type = rv_type
        elif isinstance(rv_type, str):
            self._rv_type = RandomVariableType[rv_type]
        self._gen_name = make_identifier(
            *(name for name in (table_name, field_name, value)
              if name is not None))
        self._function = function
        if function is None:
            if (self._rv_type == RandomVariableType.binary
                    and value is not None):
                self._function = make_value_exists(
                    table_name, field_name, value)
            elif (self._rv_type == RandomVariableType.count
                  and value is not None):
                self._function = make_count_values(
                    table_name, field_name, value)
            else:
                self._function = make_get_value(table_name, field_name)
        if not callable(self._function):
            raise ValueError(
                'Bad feature function: {!r}'.format(self._function))
        self._key = None

    @property
    def key(self):
        if self._key is None:
            self._key = (
                (self._table_name, self._field_name, self._value)
                if self._value
                else (self._table_name, self._field_name))
        return self._key

    @property
    def name(self):
        return (self._name
                if self._name is not None
                else self._gen_name)

    @property
    def table_name(self):
        return self._table_name

    @property
    def field_name(self):
        return self._field_name

    @property
    def value(self):
        return self._value

    @property
    def data_type(self):
        return (self._data_type
                if self._data_type is not None
                else object)

    @property
    def data_type_name(self):
        return self.data_type.__name__

    @property
    def rv_type(self):
        if self._rv_type is not None:
            return self._rv_type
        elif (self._data_type is not None
              and self._data_type in self._data_to_rv_types):
            return self._data_to_rv_types[self._data_type]
        return RandomVariableType.categorical

    def apply(self, thing):
        value = self._function(thing)
        if value is not None and self.data_type not in (None, object):
            value = self.data_type(value)
        return value

    def as_record(self):
        return tuple(getattr(self, name) for name in self.field_names)

    def as_strings(self):
        return map(str_w_empty_none, (
            self.name,
            self.table_name,
            self.field_name,
            self.value,
            self.data_type_name,
            self.rv_type.name,
            ))

    def __repr__(self):
        key_value_strs = ('{}={!r}'.format(name, getattr(self, name))
                           for name in self.field_names)
        return '{}({})'.format(
            general.fq_typename(self), ', '.join(key_value_strs))


_spcl_attrs_tbl_nm = '_special_attrs'


# Loading, saving, detecting features


def load(table): # TODO? add support for reading from file?
    table = table.order_by('id').project(*Feature.field_names)
    return [Feature(*record) for record in table]


def save(features, file):
    file = files.new(file)
    with file.open('wt') as csv_file:
        # Write header
        print('id', *Feature.field_names, sep='|', file=csv_file)
        # Write features
        for id, feat in enumerate(features, start=1):
            print(id, *feat.as_strings(), sep='|', file=csv_file)


def detect(
        fact_tables,
        event_tables,
        positive_label,
        fact_key_field=0,
        event_type_field=1,
        numeric_features=False,
        features_are_counts=True,
        ):
    rv_type = (RandomVariableType.count
               if features_are_counts
               else RandomVariableType.binary)
    assert positive_label is not None
    fact_features = [
        Feature(
            table_name=_spcl_attrs_tbl_nm,
            field_name='id',
            data_type=int,
            rv_type=RandomVariableType.continuous,
            ),
        Feature(
            table_name=_spcl_attrs_tbl_nm,
            field_name='label',
            value=positive_label,
            data_type=(int if numeric_features else bool),
            rv_type=RandomVariableType.binary,
            ),
        ]
    event_features = []
    names2tables = {}
    for table in fact_tables:
        fact_features.extend(make_fact_features(table, fact_key_field))
        names2tables[table.name] = table
    for table in event_tables:
        event_features.extend(make_event_features(
            table, event_type_field, rv_type, numeric_features))
        names2tables[table.name] = table
    # Encode categorical features as numeric if requested
    if numeric_features:
        fact_features = encode_categorical_features(
            fact_features, names2tables,
            numeric_features=numeric_features)
    return fact_features + event_features


def make_fact_features(table, key_field=0):
    key_idx = table.header.index_of(key_field)
    features = []
    for idx, field in enumerate(table.header.fields()):
        # Skip the key column
        if idx == key_idx:
            continue
        features.append(Feature(
            table_name=table.name,
            field_name=field.name,
            data_type=field.pytype,
            ))
    return features


def make_event_features(
        table,
        event_type_field=0,
        rv_type=RandomVariableType.binary,
        numeric_features=False,
        ):
    features = []
    event_types = set(
        tup[0] for tup in table.project(event_type_field) if tup)
    event_types.discard(None)
    for ev_type in sorted(event_types):
        features.append(Feature(
            name=make_identifier(table.name, ev_type),
            table_name=table.name,
            field_name=table.header[event_type_field].name,
            value=ev_type,
            rv_type=rv_type,
            data_type=(int
                       if rv_type == RandomVariableType.count
                           or numeric_features
                       else bool),
            ))
    return features


def encode_categorical_features(
        features,
        names2tables,
        rv_type=RandomVariableType.binary,
        numeric_features=False,
        ):
    new_feats = []
    for feature in features:
        # Copy non-categorical features to output
        if feature.rv_type != RandomVariableType.categorical:
            new_feats.append(feature)
            continue
        # Encode categorical features with binary indicators
        # Get unique values
        table = names2tables[feature.table_name]
        values = set(
            tup[0] for tup in table.project(feature.field_name) if tup)
        values.discard(None) # Do not encode None
        # Make a feature for each value
        for value in sorted(values):
            new_feats.append(Feature(
                table_name=feature.table_name,
                field_name=feature.field_name,
                value=value,
                rv_type=rv_type, # TODO why is this not always binary?
                data_type=(int
                           if rv_type == RandomVariableType.count
                               or numeric_features
                           else bool),
                ))
    return new_feats


# Feature functions


def make_get_value(table_name, field_name):
    if not isinstance(table_name, str):
        raise ValueError('Bad table name: {!r}'.format(table_name))
    if not isinstance(field_name, str):
        raise ValueError('Bad field name: {!r}'.format(field_name))
    def get_value(event_sequence):
        return event_sequence.fact((table_name, field_name))
    return get_value


def make_count_values(table_name, field_name, value):
    if not isinstance(table_name, str):
        raise ValueError('Bad table name: {!r}'.format(table_name))
    if not isinstance(field_name, str):
        raise ValueError('Bad field name: {!r}'.format(field_name))
    def count_values(event_sequence):
        return event_sequence.n_events_of_type(
            (table_name, field_name, value))
    return count_values


def make_value_exists(table_name, field_name, value):
    if not isinstance(table_name, str):
        raise ValueError('Bad table name: {!r}'.format(table_name))
    if not isinstance(field_name, str):
        raise ValueError('Bad field name: {!r}'.format(field_name))
    def value_exists(event_sequence):
        return (
            event_sequence.fact((table_name, field_name)) == value or
            event_sequence.has_type((table_name, field_name, value)))
    return value_exists


# Feature vectors


def lookup_feature(features_key2idx, *keys):
    for key in keys:
        feat_idx = features_key2idx.get(key)
        if feat_idx is not None:
            return feat_idx
    return None


def apply_feature(feature_vector, feature_id, feature, event_sequence):
    feat_val = feature.apply(event_sequence)
    # Warn about bad feature values
    if feat_val is None:
        logger = logging.getLogger(__name__)
        strio = io.StringIO()
        event_sequence.pprint(margin=2, file=strio)
        logger.warning('Value of feature {} is `None`: {!r}\n{}',
                       feature_id, feature, strio.getvalue())
    # Only record the value if it is nonzero
    elif feat_val:
        feature_vector[feature_id] = feat_val


def generate_feature_vectors(
        id, facts, events, examples, features, features_key2idx):
    # Create an event sequence to efficiently answer feature queries
    event_sequence = esal.EventSequence(
        (esal.Event(e[0], e[1], e[2]) for e in events), facts, id)
    # Build a feature vector for each example definition
    for example_def in examples:
        # Limit the event records to the window specified in the example
        # definition
        id, ex_beg, ex_end, label = example_def
        es = event_sequence.subsequence(ex_beg, ex_end)
        # Set the special attributes as facts
        es[_spcl_attrs_tbl_nm, 'id'] = id
        es[_spcl_attrs_tbl_nm, 'label'] = label
        # Create the feature vector.  Be efficient by applying only the
        # relevant feature functions.
        feature_vector = {}
        # Apply features to facts
        for key, val in es.facts():
            # Lookup the feature either by (table, field, value) or by
            # (table, field).  (`key` is (table, field).)
            feat_idx = lookup_feature(
                features_key2idx, (*key, val), key)
            if feat_idx is not None:
                apply_feature(feature_vector,
                              # External feature ID is 1-based index
                              feat_idx + 1, features[feat_idx], es)
        # Apply features to events
        for ev_type in es.types():
            # Lookup the feature by (table, field, value), which is the
            # event type, or by (table, field)
            feat_idx = lookup_feature(
                features_key2idx, ev_type, ev_type[:2])
            if feat_idx is not None:
                apply_feature(feature_vector,
                              # External feature ID is 1-based index
                              feat_idx + 1, features[feat_idx], es)
        yield feature_vector
