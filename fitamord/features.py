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

from barnapy import files
from barnapy import logging

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

    def apply(self, record_collection):
        value = self._function(record_collection)
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


class CollectedRecordsView:
    """The object on which feature functions are defined"""

    _field_idx_cache = {}

    def __init__(
            self,
            record_collection,
            event_table_names,
            example_table_names,
            event_time_idx,
            ):
        self._record_collection = record_collection
        self._event_table_names = set(event_table_names)
        self._example_table_names = example_table_names
        self._event_time_idx = event_time_idx
        # Fake the special attributes until I come up with a better way # FIXME
        if _spcl_attrs_tbl_nm not in self._record_collection.relations:
            self._record_collection.relations.add(records.RecordStream(
                records=(),
                name=_spcl_attrs_tbl_nm,
                header=records.Header(('id', int), ('label', str)),
                ))
        self._record_collection.add(_spcl_attrs_tbl_nm, None)

    def set_example_definition(self, example_definition): # TODO validate example def
        _, start, stop, label = example_definition
        self._start_time = start
        self._stop_time = stop
        self._label = label
        self._record_collection[_spcl_attrs_tbl_nm] = [(self.id, label)]

    @property
    def id(self):
        return self._record_collection.groupby_key

    @property
    def label(self):
        return self._label

    def __getitem__(self, idx):
        return self._record_collection.__getitem__(idx)

    def field_idx_of(self, table_name, field_name):
        key = (table_name, field_name)
        if key not in self._field_idx_cache:
            self._field_idx_cache[key] = (
                self._record_collection.relations[table_name]
                .header.index_of(field_name))
        return self._field_idx_cache[key]

    def values(self, table_name, field_name):
        field_idx = self.field_idx_of(table_name, field_name)
        for record in self[table_name]:
            yield record[field_idx]

    def select_records_by_value(self, table_name, field_name, value):
        is_event_table = table_name in self._event_table_names
        field_idx = self.field_idx_of(table_name, field_name)
        for record in self[table_name]:
            if record[field_idx] == value:
                if (not is_event_table
                    or self._start_time
                       <= record[self._event_time_idx]
                       <= self._stop_time):
                    yield record

    def count_values(self, table_name, field_name, value):
        count = 0
        for record in self.select_records_by_value(
                table_name, field_name, value):
            count += 1
        return count

    def value_exists(self, table_name, field_name, value):
        for record in self.select_records_by_value(
                table_name, field_name, value):
            return True
        return False


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
            fact_features, names2tables, rv_type, numeric_features)
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
                rv_type=rv_type,
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
    def get_value(record_collection):
        values = tuple(record_collection.values(table_name, field_name))
        if values:
            return values[0]
        else:
            return None
    return get_value


def make_count_values(table_name, field_name, value):
    if not isinstance(table_name, str):
        raise ValueError('Bad table name: {!r}'.format(table_name))
    if not isinstance(field_name, str):
        raise ValueError('Bad field name: {!r}'.format(field_name))
    def count_values(record_collection):
        return record_collection.count_values(
            table_name, field_name, value)
    return count_values


def make_value_exists(table_name, field_name, value):
    if not isinstance(table_name, str):
        raise ValueError('Bad table name: {!r}'.format(table_name))
    if not isinstance(field_name, str):
        raise ValueError('Bad field name: {!r}'.format(field_name))
    def value_exists(record_collection):
        return record_collection.value_exists(
            table_name, field_name, value)
    return value_exists


# Feature vectors


def generate_feature_vectors(
        features,
        record_collection,
        treatments2table_names,
        event_time_idx,
        ):
    # Check that this record collection has examples
    if not (set(treatments2table_names['examples'])
            & set(record_collection.names())):
        logger = logging.getLogger(__name__)
        logger.warning(
            'Skipping {}: '
            'No example definitions in record collection: {!r}',
            record_collection.groupby_key,
            record_collection)
        return
    # Create view of collected records that is suitable for applying
    # feature functions
    record_collection_view = CollectedRecordsView(
        record_collection,
        treatments2table_names['events'],
        treatments2table_names['examples'],
        event_time_idx,
        )
    # Generate a feature vector for each of the examples in the record
    # collection
    for example_table_name in treatments2table_names['examples']:
        if example_table_name not in record_collection:
            continue
        for example_def in record_collection[example_table_name]:
            # Limit the event records to the window specified in the
            # example definition
            record_collection_view.set_example_definition(example_def)
            # Apply all the feature functions to the limited records
            feature_vector = {}
            for idx, feature in enumerate(features, start=1):
                value = feature.apply(record_collection_view)
                if value is None:
                    logger = logging.getLogger(__name__)
                    logger.info(
                        'Feature value is None.  '
                        'Feature {}: {!r};  Example: {!r};  Data: {!r}',
                        idx, feature, example_def, record_collection)
                if value:
                    feature_vector[idx] = value
            yield feature_vector
