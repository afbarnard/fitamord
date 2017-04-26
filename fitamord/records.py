"""Core record processing"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import itertools as itools

from . import general


class Field:
    """A (name, type) pair."""

    def __init__(self, name, typ=None):
        self._name = name
        if isinstance(typ, type):
            self._type = typ
        elif typ is not None:
            self._type = type(typ)
        else:
            self._type = object

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    def isinstance(self, obj):
        return isinstance(obj, self._type)

    def __repr__(self):
        return 'Field(name={!r}, typ={})'.format(
            self.name, self.type.__name__)

    def __eq__(self, other):
        return (type(self) == type(other)
                and self.name == other.name
                and self.type == other.type)

    def __hash__(self):
        return hash((self.name, self.type))


class Header: # TODO convert to subclass of NamedItems
    """A definition of a collection of fields where each field has a name
    and a type.

    The type of a record or the element type of a collection of records
    (such as a table).

    """

    def __init__(self, fields=None, types=None, **names_to_types):
        """Create a header from the given fields.

        The fields can be given as an iterable of `Field`s, an iterable
        of (name, type) pairs, both an iterable of names and a
        corresponding iterable of types, just an iterable of names
        (assumes types are all `object`), a mapping of names to types,
        or name=type keyword arguments.

        """
        # TODO allow fields / types to be strings?
        # Convert arguments to a standard form
        field_defs = ()
        # Parallel iterables of names and types
        if fields is not None and types is not None:
            field_defs = zip(fields, types)
        # Mapping of names to types
        elif isinstance(fields, dict):
            field_defs = fields.items()
        # Iterable of field definitions
        elif fields is not None:
            field_defs = fields
        # Include keyword arguments if any
        if names_to_types:
            if field_defs:
                field_defs = itools.chain(
                    field_defs, names_to_types.items())
            else:
                field_defs = names_to_types.items()

        # Members: list of fields, their indices
        self._fields = []
        self._names2idxs = {}
        # Process each field definition to construct a field
        for field_def in field_defs:
            field = None
            # `Field`
            if isinstance(field_def, Field):
                field = field_def
            # Name
            elif isinstance(field_def, str):
                field = Field(field_def)
            # (name, type) pair
            elif len(field_def) == 2:
                field = Field(*field_def)
            # Otherwise error
            else:
                raise ValueError(
                    'Could not interpret as a field: {!r}'
                    .format(field_def))
            # Duplicate name is an error
            if field.name in self._names2idxs:
                raise ValueError(
                    'Duplicate field name: {!r}'.format(field.name))
            # Update members
            self._names2idxs[field.name] = len(self._fields)
            self._fields.append(field)

        # Check that at least one field was specified
        if not self._fields:
            raise ValueError('No fields were specified')

    def __len__(self):
        return len(self._fields)

    @property
    def n_fields(self):
        return len(self)

    def fields(self):
        return iter(self._fields)

    def names(self):
        return (field.name for field in self._fields)

    def types(self):
        return (field.type for field in self._fields)

    def field_at(self, index):
        return self._fields[index]

    def __getitem__(self, index):
        if isinstance(index, str):
            return self.field_of(index)
        return self.field_at(index)

    def name_at(self, index):
        return self._fields[index].name

    def type_at(self, index):
        return self._fields[index].type

    def field_of(self, name):
        return self._fields[self._names2idxs[name]]

    def type_of(self, name):
        return self._fields[self._names2idxs[name]].type

    def index_of(self, name):
        return self._names2idxs[name]

    def isinstance(self, record):
        idx = 0
        for idx, value in enumerate(record):
            if (idx >= len(self)
                    or not self._fields[idx].isinstance(value)):
                return False
        # Check that the length of the record equals that of the header
        if idx != len(self) - 1:
            return False
        return True

    def items(self): # TODO for writing config or making dict
        return ()

    def __repr__(self):
        return ('Header((' + ', '.join(repr(f) for f in self._fields)
                + '))')

    def __contains__(self, obj):
        if isinstance(obj, str):
            return obj in self._names2idxs
        if isinstance(obj, int):
            return 0 <= obj < len(self)
        # Treat (name, type) pair as a field as in the constructor
        if isinstance(obj, (tuple, list)) and len(obj) == 2:
            obj = Field(*obj)
        if isinstance(obj, Field):
            return (obj.name in self._names2idxs
                    and obj == self.field_of(obj.name))
        return False


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
