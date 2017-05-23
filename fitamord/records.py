"""Core record processing"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import collections
import itertools as itools

from . import general
from .collections import NamedItems


class Field:
    """A (name, type) pair with sauce."""

    def __init__(self, name, typ=None):
        self._name = name
        if isinstance(typ, type):
            self._type = typ
        elif typ is not None:
            self._type = type(typ)
        else:
            self._type = object

    @staticmethod
    def make_from(field):
        # `Field` object
        if isinstance(field, Field):
            return field
        # Name
        elif isinstance(field, str):
            return Field(field)
        # Index
        elif isinstance(field, int):
            field = str(field)
            return Field(field)
        # (name, type) pair
        elif hasattr(field, '__iter__'):
            field_tup = tuple(field)
            if len(field_tup) != 2:
                raise ValueError(
                    'Could not interpret as a field: {!r}'
                    .format(field_tup))
            return Field(*field_tup)
        # Error
        else:
            raise ValueError(
                'Could not interpret as a field: {!r}'
                .format(field))

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def typename(self):
        return self.type.__name__

    def isinstance(self, obj):
        return isinstance(obj, self._type)

    def __repr__(self):
        return '{}(name={!r}, typ={})'.format(
            general.fq_typename(self), self.name, self.typename)

    def __eq__(self, other):
        return (type(self) == type(other)
                and self.name == other.name
                and self.type == other.type)

    def __hash__(self):
        return hash((self.name, self.type))


class Header(NamedItems):
    """A definition of a collection of fields where each field has a name
    and a type.

    The type of a record or the element type of a collection of records
    (such as a table).

    """

    def __init__(self, *fields, names=None, types=None, **names2types):
        """Create a header from the given fields.

        The fields can be given as an iterable of `Field`s, an iterable
        of (name, type) pairs, both an iterable of names and a
        corresponding iterable of types, just an iterable of names
        (assumes types are all `object`), or name=type keyword
        arguments.

        """
        # TODO allow fields / types to be strings?
        # Default types if necessary
        if names is not None and types is None:
            types = itools.repeat(None)
        # Construct members
        super().__init__(
            *fields, names=names, items=types, **names2types)
        # Check that at least one field was specified
        if not self:
            raise ValueError('No fields were specified')

    def add_named_item(self, field_def):
        field = Field.make_from(field_def)
        super().add_named_item((field.name, field))

    @property
    def n_fields(self):
        return len(self)

    fields = NamedItems.items

    def types(self):
        return (field.type for field in self.fields())

    names_fields = NamedItems.names_items

    def names_types(self):
        return ((f.name, f.type) for f in self.fields())

    field_at = NamedItems.item_at

    def type_at(self, index):
        return self.field_at(index).type

    field_of = NamedItems.item_of

    def type_of(self, name):
        return self.field_of(name).type

    def isinstance(self, record):
        idx = 0
        for idx, value in enumerate(record):
            if (idx >= len(self)
                    or not self.field_at(idx).isinstance(value)):
                return False
        # Check that the length of the record equals that of the header
        if idx != len(self) - 1:
            return False
        return True

    def __repr__(self):
        return ('{}({})'.format(
            general.fq_typename(self),
            ', '.join(repr(f) for f in self.fields())))

    def __contains__(self, obj):
        # Add `has_index` to superclass implementation
        return self.has_index(obj) or super().__contains__(obj)

    def has_field(self, field):
        if isinstance(field, Field):
            field = field.name
        return self.has_name(field) or self.has_index(field)

    def project(self, *columns):
        return Header(*(self.field_of(col) for col in columns))

    def as_yaml_object(self):
        return collections.OrderedDict(
            (f.name, f.typename) for f in self.fields())


class Record:
    pass


class RecordStream:
    """An iterable of records.  A relation.

    Records can be instances of Record or they can be some other
    iterable of values.  The number and type of values are meant to
    match the header.

    """

    def __init__(
            self, records, name=None, header=None, provenance=None,
            error_handler=None, is_reiterable=False):
        self._records = records
        self._name = name
        self._header = header
        self._provenance = provenance
        self._error_handler = error_handler
        self._is_reiterable = is_reiterable

    def has_name(self):
        return self._name is not None

    @property
    def name(self):
        return self._name if self._name is not None else '<unknown>'

    def has_header(self):
        return self._header is not None

    @property
    def header(self):
        return self._header

    @property
    def provenance(self):
        return (self._provenance
                if self._provenance is not None
                else general.object_name(self._records))

    @property
    def n_cols(self):
        return self.header.n_fields

    @property
    def is_reiterable(self):
        """Whether this stream can be iterated over multiple times."""
        return self._is_reiterable

    def __iter__(self):
        """Iterates over records using self's error handler."""
        return self.records(self._error_handler)

    def records(self, error_handler=None):
        """Iterates over records with optional error handling.

        Each error is handled separately by calling the given error
        handler.  Record processing then continues.  If no error handler
        is specified, then exceptions are allowed to propagate.

        """
        if error_handler is None:
            return self._record_iterator()
        else:
            return self._record_error_iterator(error_handler)

    def _record_iterator(self):
        """Return a plain record iterator without error handling.

        Subclasses should override this method to provide iteration over
        records if something more sophisticated than the default
        `iter(self._records)` is desired.  Note that
        `_record_error_iterator` calls this method for its basic
        iteration so there is no need to reimplement error handling.
        These two methods exist to enable clean subclassing and avoid
        the overhead of an extra generator when no error handling is
        needed.

        """
        return iter(self._records)

    def _record_error_iterator(self, error_handler=None):
        """Return a record iterator with the specified error handling.

        If no error handler is specified, exceptions are allowed to
        propagate.

        """
        # Manually iterate over records in order to catch exceptions.
        # Use an infinite loop to avoid repeating record handling code.
        record_iterator = self._record_iterator()
        record = None
        while True:
            try:
                record = next(record_iterator)
            except StopIteration:
                break
            except Exception as e:
                if error_handler is not None:
                    error_handler(e, record)
                else:
                    raise e
            yield record

    def __repr__(self):
        return ('{}(name={!r}, header={!r}, provenance={!r}, '
                'error_handler={!r}, is_reiterable={!r}, records={!r})'
                .format(general.fq_typename(self), self._name,
                        self._header, self._provenance,
                        self._error_handler, self._is_reiterable,
                        self._records))

    # Queries

    def project(self, *cols):
        """Returns a view of this record stream that includes only the specified
        columns.

        A column is identified by name or index.

        """
        return self # Dummy implementation

    def select(self, predicate):
        """Returns a view of this record stream that includes only those rows
        that match the given predicate.

        """
        return self # Dummy implementation

    def order_by(self, *cols):
        """Returns a record stream that iterates over its rows in the specified
        order.

        An order-by column specification is a column identified by name
        or index, or it is a (column, order) pair where the order is one
        of "asc" or "desc".  If only the column is given, ascending is
        assumed.

        """
        return self # Dummy implementation

    def join(self, table, alias=None):
        """Returns a table-like object that includes rows from this table and
        the given table.

        """
        return self # Dummy implementation

    # Helpers

    def _interpret_column(self, col):
        # Interpret column specification
        if not isinstance(col, (str, int)):
            raise ValueError(
                'Not a column name or index: {!r}'.format(col))
        # Check column exists
        if col not in self.header:
            raise DbError(
                '{}: No such column: {}'.format(self.name, col))
        # Convert column index to name
        if isinstance(col, int):
            col = self.header.name_at(col)
        return col

    def _interpret_order_by_column(self, col):
        # Interpret column specification
        col_spec = None
        if isinstance(col, (str, int)):
            col_spec = (col, 'asc')
        elif (isinstance(col, tuple)
              and len(col) == 2
              and isinstance(col[0], (str, int))
              and isinstance(col[1], str)):
            col_spec = col
        else:
            raise ValueError(
                'Not a (name, "asc"|"desc") order-by column '
                'specification: {!r}'.format(col))
        # Check column specification
        name, order = col_spec
        if name not in self.header:
            raise DbError(
                '{}: No such column: {}'.format(self.name, name))
        if order not in ('asc', 'desc'):
            raise ValueError(
                'Not an ordering keyword ("asc"|"desc"): {!r}'
                .format(order))
        # Convert column index to name
        if isinstance(name, int):
            name = self.header.name_at(name)
        return name, order


class Table(RecordStream):

    def __init__(
            self,
            backend='sqlite',
            ):
        pass


class RecordTransformation:

    def __init__(
            self,
            ):
        pass

    # strip space
