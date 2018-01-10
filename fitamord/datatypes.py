"""Data types for tables"""

# Copyright (c) 2018 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import datetime

import barnapy.parse


# TODO implement properly customizable data type class that acts like a type (supports `isinstance` etc.)


class DataTypeError(Exception):
    pass


def parse_datetime(text, format='%Y-%m-%dT%H:%M:%S'):
    dt = None
    err = None
    try:
        dt = datetime.datetime.strptime(text, format)
    except ValueError as e:
        err = e
    return dt, err


def is_datetime(text, format='%Y-%m-%dT%H:%M:%S'):
    _, err = parse_datetime(text, format)
    return err is None


def parse_date(text, format='%Y-%m-%d'):
    dt, err = parse_datetime(text, format)
    if err is None:
        dt = dt.date()
    return dt, err


def parse_time(text, format='%H:%M:%S'):
    dt, err = parse_datetime(text, format)
    if err is None:
        dt = dt.time()
    return dt, err


class TextValueType:
    """Data types whose values can be represented as textual atoms."""

    def __init__(self, name, type_, args, is_repr, parse, format):
        self._name = name
        self._type = type_
        self._args = args
        self._is_repr = is_repr
        self._parse = parse
        self._format = format

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    def isinstance(self, obj):
        return isinstance(obj, self.type)

    def isrepr(self, text):
        return self._is_repr(self._args, text)

    def parse(self, text):
        if not isinstance(text, str):
            return None, ValueError(
                'Cannot parse: Not a string: {!r}'.format(text))
        return self._parse(self._args, text)

    def format(self, obj):
        return self._format(self._args, obj)

    def __str__(self):
        return self.type.__name__

    def __repr__(self):
        if self._args:
            return '{}({}, {!r})'.format(
                self.name, self.type.__name__, self._args)
        else:
            return '{}({})'.format(self.name, self.type.__name__)

    def derive(self, args):
        return TextValueType(
            self.name,
            self.type,
            args,
            self._is_repr,
            self._parse,
            self._format,
        )


def _add_dummy_arg1(func):
    return lambda a, b: func(b)


Atom = TextValueType(
    'Atom',
    object,
    (),
    _add_dummy_arg1(barnapy.parse.is_atom),
    _add_dummy_arg1(barnapy.parse.atom_err),
    repr,
)

Bool = TextValueType(
    'Bool',
    bool,
    (),
    _add_dummy_arg1(barnapy.parse.is_bool),
    _add_dummy_arg1(barnapy.parse.bool_err),
    repr,
)

Int = TextValueType(
    'Int',
    int,
    (),
    _add_dummy_arg1(barnapy.parse.is_int),
    _add_dummy_arg1(barnapy.parse.int_err),
    repr,
)

Float = TextValueType(
    'Float',
    float,
    (),
    _add_dummy_arg1(barnapy.parse.is_float),
    _add_dummy_arg1(barnapy.parse.float_err),
    repr,
)

String = TextValueType(
    'String',
    str,
    (),
    lambda args, text: True,
    lambda args, text: (text, None),
    str,
)

Date = TextValueType(
    'Date',
    datetime.date,
    ('%Y-%m-%d',),
    lambda args, text: is_datetime(text, args[0]),
    lambda args, text: parse_date(text, args[0]),
    lambda args, date: date.strftime(args[0]),
)

Time = TextValueType(
    'Time',
    datetime.time,
    ('%H:%M:%S',),
    lambda args, text: is_datetime(text, args[0]),
    lambda args, text: parse_time(text, args[0]),
    lambda args, time: time.strftime(args[0]),
)

DateTime = TextValueType(
    'DateTime',
    datetime.datetime,
    ('%Y-%m-%dT%H:%M:%S',),
    lambda args, text: is_datetime(text, args[0]),
    lambda args, text: parse_datetime(text, args[0]),
    lambda args, datm: datm.strftime(args[0]),
)


"""
Recognized lowercase names and aliases mapped to the types they
identify.
"""
names2types = {
    'atom': Atom,
    'auto': Atom,
    'object': Atom,
    'bool': Bool,
    'boolean': Bool,
    'int': Int,
    'integer': Int,
    'float': Float,
    'double': Float,
    'real': Float,
    'str': String,
    'string': String,
    'char': String,
    'varchar': String,
    'date': Date,
    'time': Time,
    'datetime': DateTime,
    'timestamp': DateTime,
}


def parse(text):
    """
    Parse the given text and construct a data type.

    Return a (data type, error) pair per Go style.
    """
    val, err = barnapy.parse.predicate_err(text)
    if err is not None:
        return None, err
    name, args = val
    name_lower = name.lower()
    if name_lower not in names2types:
        return None, DataTypeError(
            'Unrecognized data type name: {!r} (not in {{{}}})'
            .format(name, ', '.join(names2types.keys())))
    datatype = names2types[name_lower]
    if args:
        datatype = datatype.derive(args)
    return (datatype, None)
