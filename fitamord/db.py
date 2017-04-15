"""Databases and tables"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

# TODO treat namespaces as objects (eventually, but not for now);
# analogously to a filesystem, namespaces are directories and tables /
# views / indices / etc. are files -> listing all the objects in a
# namespace would list all child namespaces

import re
from enum import Enum

from . import records


# TODO remove initial "Db" from names (Go style advice)

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
        """Return the schema of the named object"""
        return None

    def interpret_sql_name(self, name):
        """Interpret the given SQL name according to this DB"""
        return interpret_sql_name(name)

    def exists(self, name):
        """Whether an object with the given name exists in this DB"""
        sql_name = self.interpret_sql_name(name)
        namespace, obj_name = sql_name.rest_last()
        objects = self.objects(namespace=namespace)
        for obj in objects:
            if obj[0] == obj_name:
                return True
        return False

    def typeof(self, name):
        """Return the type of the named object"""
        sql_name = self.interpret_sql_name(name)
        namespace, obj_name = sql_name.rest_last()
        objects = self.objects(namespace=namespace)
        for obj in objects:
            if obj[0] == obj_name:
                return DbObjectType[obj[1]]
        raise DbError('Object not found: {}'.format(name))

    def create_table(self, name, header):
        """Create and return a table with the given name and fields.

        The table must not already exist.
        """
        pass

    def make_table(self, name, header):
        """Create or replace or simply get the named table as needed.

        Ensures the named table exists and has the given fields.
        """
        pass

    def table(self, name):
        """Return the table with the given name"""
        return None

    def drop_table(self, name):
        """Delete the named table and its data"""
        pass


class Table(records.RecordStream):

    def __init__(self, db, name, header, error_handler=None):
        super().__init__(
            records=None,
            name=name,
            header=header,
            provenance=db,
            error_handler=error_handler,
            is_reiterable=True,
            )
        self._db = db
        self._n_rows = 0

    @property
    def n_rows(self):
        return self._n_rows

    def __len__(self):
        return self.n_rows

    def __repr__(self):
        return 'Table(db={!r}, name={!r}, header={!r})'.format(
            self._db, self.name, self.header)

    def add(self, record):
        pass

    def add_all(self, records):
        pass

    # truncate, delete, etc.


# Syntax of a SQL identifier, according to the PostgreSQL documentation:
# https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html
# Basically, alphanumeric plus underscore but not starting with a digit,
# or double quoted anything.  PostgreSQL uses doubling to embed double
# quotes.
_sql_identifier_unquoted_regex = r'[a-zA-Z_]\w*'
_sql_identifier_quoted_regex = r'"(?:[^"]+|"")+"'
_sql_identifier_pattern = re.compile(
    r'(?:{unq}|{quo})(?:\.(?:{unq}|{quo}))*'.format(
        unq=_sql_identifier_unquoted_regex,
        quo=_sql_identifier_quoted_regex,
        ))

def is_sql_identifier(text):
    return _sql_identifier_pattern.fullmatch(text) is not None

def assert_sql_identifier(text):
    if not is_sql_identifier(text):
        raise DbSyntaxError('Bad SQL identifier: "{}"'.format(text))

def unquote(text, quote='"'):
    """Returns the unquoted version of the text.

    Treats doubled quote characters as specifying a single literal quote
    character.  SQL uses this doubling style for escaping quote
    characters.

    """
    result = []
    quote_mode = False
    char_idx = 0
    while char_idx < len(text):
        char = text[char_idx]
        if char == quote:
            if quote_mode:
                # Is this the first of two consecutive quote characters
                # (which encode a single literal quote character) or is
                # this the end of the quoted text?
                if (char_idx + 1 < len(text)
                        and text[char_idx + 1] == quote):
                    result.append(quote)
                    char_idx += 1
                else:
                    quote_mode = False
            else:
                # If it was not quote mode and a quote character is
                # encountered it is now quote mode
                quote_mode = True
        else:
            result.append(char)
        char_idx += 1
    return ''.join(result)

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

    def __getitem__(self, index):
        return self._parts[index]

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def head(self, reverse=False):
        if self._parts:
            if reverse:
                return self._parts[-1]
            else:
                return self._parts[0]
        else:
            return None

    def tail(self, reverse=False):
        if len(self._parts) > 1:
            if reverse:
                return self._delimiter.join(self._parts[0:-1])
            else:
                return self._delimiter.join(self._parts[1:])
        else:
            return None

    def first_rest(self):
        return self.head(), self.tail()

    def rest_last(self):
        return self.tail(reverse=True), self.head(reverse=True)

    def __repr__(self):
        return '{}(name={!r}, delimiter={!r})'.format(
            self.__class__.__name__, self._name, self._delimiter)

    def __str__(self):
        return self.name


class DottedName(CompoundName):

    def __init__(self, text):
        super().__init__(text, '.')
