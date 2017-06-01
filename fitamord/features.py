"""Functionality for generating feature vectors from relational data"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
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


class RandomVariableType(Enum): # TODO fix this at some point: need proper RandomVariable object with a domain of values and how those values can be interpreted, their algebraic structure (order, interval, field)
    """Types of random variables"""
    none = 0 # None or unknown, the null type
    binary = 1
    categorical = 2
    ordinal = 3
    interval = 4
    # differentiate finite and infinite discrete? different distributions apply
    continuous = 5

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
        'table', 'field', 'value', 'name', 'data_type', 'rv_type')

    def __init__(
            self,
            table, # name
            field, # name
            value=None, # values
            #id=None,
            name=None,
            data_type=None,
            rv_type=None,
            #encoding=None,
            ):
        self._table = table
        self._field = field
        self._value = value
        #self._id = id
        self._name = name
        self._data_type = (
            data_type
            if isinstance(data_type, type)
            else self._names_to_data_types.get(data_type, object))
        self._rv_type = (
            rv_type
            if isinstance(rv_type, RandomVariableType)
            else RandomVariableType[rv_type])
        #self._encoding = encoding
        self._gen_name = make_identifier(
            *(name for name in (table, field, value)
              if name is not None))

    #@property
    #def id(self):
    #    return self._id
    #
    #@id.setter
    #def id(self, new_id):
    #    if self._id is None:
    #        self._id = new_id
    #    return self._id

    @property
    def key(self):
        if self._key is None:
            self._key = (
                (self._table, self._field, self._value)
                if self._value
                else (self._table, self._field))
        return self._key

    @property
    def name(self):
        return (self._name
                if self._name is not None
                else self._gen_name)

    @property
    def table(self):
        return self._table

    @property
    def field(self):
        return self._field

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

    #@property
    #def encoding(self):
    #    return (self._encoding
    #            if self._encoding is not None
    #            else SetEncoding.values)

    # values (?)
    # function / apply

    @property
    def function(self):
        return self._function

    def _make_function(self):
        # binary rv with value -> value == value
        # continuous rv -> value
        def feat_func():
            pass
        return feat_func

    def value_of(self, value):
        return None

    def as_record(self):
        return (
            #self.id,
            self.name,
            self.table,
            self.field,
            self.value,
            self.data_type,
            self.rv_type,
            #self.encoding,
            )

    def as_strings(self):
        return map(str_w_empty_none, (
            #self.id,
            self.name,
            self.table,
            self.field,
            self.value,
            self.data_type_name,
            self.rv_type.name,
            #self.encoding.name,
            ))


def load(table): # TODO? add support for reading from file?
    table = table.order_by('id').project(*Feature.field_names)
    return [Feature(*record) for record in table]


def save(features, file):
    file = files.new(file)
    with file.open('wt') as csv_file:
        # Write header
        print('id', 'name', 'table', 'field', 'value', 'data_type',
              'rv_type', sep='|', file=csv_file)
        # Write features
        for id, feat in enumerate(features, start=1):
            print(id, *feat.as_strings(), sep='|', file=csv_file)


def detect(fact_tables, event_tables,
           fact_key_field=0, event_type_field=1,
           numeric_features=False):
    fact_features = [] # TODO Add ID fact feature
    event_features = []
    names2tables = {}
    for table in fact_tables:
        fact_features.extend(make_fact_features(
            table, fact_key_field, numeric_features))
        names2tables[table.name] = table
    for table in event_tables:
        event_features.extend(make_event_features(
            table, event_type_field, numeric_features))
        names2tables[table.name] = table
    # Encode categorical features as numeric if requested
    if numeric_features:
        fact_features = encode_categorical_features(
            fact_features, names2tables, numeric_features)
    # Sort features by name, facts ahead of events
    #fact_features.sort(key=lambda f: f.name)
    #event_features.sort(key=lambda f: f.name)
    return fact_features + event_features


def make_fact_features(table, fact_key_field=0, numeric_features=False):
    key_idx = table.header.index_of(fact_key_field)
    features = []
    for idx, field in enumerate(table.header.fields()):
        # Skip the key column
        if idx == key_idx:
            continue
        features.append(Feature(
            table=table.name,
            field=field.name,
            data_type=field.type,
            ))
    return features


def make_event_features(table, event_type_field=0,
                        numeric_features=False):
    features = []
    event_types = set(
        tup[0] for tup in table.project(event_type_field) if tup)
    event_types.discard(None)
    for ev_type in sorted(event_types):
        features.append(Feature(
            name=make_identifier(table.name, ev_type),
            table=table.name,
            field=table.header[event_type_field].name,
            value=ev_type,
            data_type=(int if numeric_features else bool),
            rv_type=RandomVariableType.binary,
            ))
    return features


def encode_categorical_features(features, names2tables,
                                numeric_features=False):
    new_feats = []
    for feature in features:
        # Copy non-categorical features to output
        if feature.rv_type != RandomVariableType.categorical:
            new_feats.append(feature)
            continue
        # Encode categorical features with binary indicators
        # Get unique values
        table = names2tables[feature.table]
        values = set(
            tup[0] for tup in table.project(feature.field) if tup)
        values.discard(None) # Do not encode None
        # Make a feature for each value
        for value in sorted(values):
            new_feats.append(Feature(
                table=feature.table,
                field=feature.field,
                value=value,
                data_type=(int if numeric_features else bool),
                rv_type=RandomVariableType.binary,
                ))
    return new_feats
