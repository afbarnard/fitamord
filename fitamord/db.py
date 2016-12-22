"""Databases and tables"""

# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import re
from enum import Enum

from . import records


class DbError(Exception):
    pass

class DbSyntaxError(DbError):
    pass


class DbObjectType(Enum):
    """Types of database objects"""
    none = 0 # None or unknown, the null type
    view = 1
    table = 2
    index = 3
    trigger = 4
    procedure = 5

    @classmethod
    def convert(cls, obj):
        """Converts the given object to a DbObjectType if possible"""
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, str) and obj in cls.__members__:
            return cls[obj]
        else:
            raise DbError('Unrecognized DB object type: {!r}'
                          .format(obj))


class Database:
    """A collection of tables"""

    def namespaces(self):
        """Return the namespaces in this DB as an iterable"""
        return ()

    def tables(self, namespace=None):
        """Return the names of the tables in this DB as an iterable"""
        objects = self.objects(DbObjectType.table, namespace)
        for obj in objects:
            yield obj[0]

    def schema(self, name):
        return None

    def exists(self, name):
        """Whether an object with the given name exists in this DB"""
        # FIXME interpret hierarchical dotted name
        sql_name = interpret_sql_name(name)
        namespace, obj_name = sql_name.rest_last()
        objects = self.objects(namespace=namespace)
        for obj in objects:
            if obj[0] == obj_name:
                return True
        return False

    def typeof(self, name):
        """Return the type of the named object"""
        # FIXME interpret hierarchical dotted name
        sql_name = interpret_sql_name(name)
        namespace, obj_name = sql_name.rest_last()
        objects = self.objects(namespace=namespace)
        for obj in objects:
            if obj[0] == obj_name:
                return DbObjectType[obj[1]]
        raise DbError('Object not found: {}'.format(name))

    def create_table(self, name, header):
        pass

    def drop_table(self, name):
        pass


class Table(records.RecordStream):

    def __init__(self, db, name, header):
        pass

    @property
    def name(self):
        return None

    @property
    def header(self):
        return None

    def add(self, record):
        pass

    def add_all(self, records):
        pass


# Syntax of a SQL identifier, according to the PostgreSQL documentation:
# https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html
# Basically, alphanumeric plus underscore but not starting with a digit,
# or double quoted anything.  PostgreSQL uses doubling to embed double
# quotes.
_sql_identifier_pattern = re.compile(r'[a-zA-Z_]\w*|(?:"[^"]*")+')

def is_sql_identifier(text):
    return _sql_identifier_pattern.fullmatch(text) is not None

def assert_sql_identifier(text):
    if not is_sql_identifier(text):
        raise DbSyntaxError('Bad SQL identifier: {}'.format(text))

def unquote(text, quote='"'):
    """Returns the text with one layer of quoting removed"""
    # Not possible for a string of 0 or 1 characters to be quoted
    if len(text) >= 2 and text[0] == quote and text[-1] == quote:
        return text[1:-1]
    else:
        return text

def interpret_sql_name(name):
    assert_sql_identifier(name)
    return DottedName(unquote(name))


class CompoundName:

    def __init__(self, name, delimiter):
        self._name = name
        self._delimiter = delimiter
        self._parts = name.split(delimiter)

    @property
    def name(self):
        return self._name

    @property
    def delimiter(self):
        return self._delimiter

    @property
    def parts(self):
        return self._parts

    def __len__(self):
        return len(self._parts)

    def __getindex__(self, index):
        return self._parts[index]

    def head(self, reverse=False):
        if self._parts:
            if reverse:
                return self._parts[-1]
            else:
                return self._parts[1]
        else:
            return None

    def tail(self, reverse=False):
        if len(self._parts) > 1:
            if reverse:
                return delimiter.join(self._parts[0:-1])
            else:
                return delimiter.join(self._parts[1:])
        else:
            return None

    def first_rest(self):
        return self.head(), self.tail()

    def rest_last(self):
        return self.tail(reverse=True), self.head(reverse=True)

    def __repr__(self):
        return '{}(name={!r}, delimiter={!r})'.format(
            self.__class__.__name__, self._name, self._delimiter)


class DottedName(CompoundName):

    def __init__(self, text):
        super().__init__(text, '.')
