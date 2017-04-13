"""Core record processing

"""
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


class Header:
    """A definition of a collection of fields where each field has a name
    and a type.

    The type of a record or the element type of a collection of records
    (such as a table).

    """

    def __init__(self, fields=None, types=None, **names_to_types):
        """Create a header from the given fields.

        The fields can be given as an interable of `Field`s, an iterable
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


class Record:
    pass


class RecordStream:
    """An iterable of records.

    Records can be instances of Record or they can be some other
    iterable of values.  The number and type of values are meant to
    match the header.

    """

    def __init__(self, records, name=None, header=None, provenance=None,
                 error_handler=None, is_reiterable=False):
        self._records = records
        self._name = name if name is not None else '<unknown>'
        self._header = header
        self._provenance = (provenance
                            if provenance is not None
                            else general.object_name(records))
        self._error_handler = error_handler
        self._is_reiterable = is_reiterable

    @property
    def name(self):
        return self._name

    @property
    def header(self):
        return self._header

    @property
    def provenance(self):
        return self._provenance

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
            return self._records_iterator()
        else:
            return self._records_error_iterator(error_handler)

    def _records_iterator(self):
        """Return a plain record iterator without error handling.

        Subclasses should override this method to provide iteration over
        records if something more sophisticated than the default
        `iter(self._records)` is desired.  Note that
        `_records_error_iterator` calls this method for its basic
        iteration so there is no need to reimplement error handling.
        These two methods exist to enable clean subclassing and avoid
        the overhead of an extra generator when no error handling is
        needed.

        """
        return iter(self._records)

    def _records_error_iterator(self, error_handler=None):
        """Return a record iterator with the specified error handling.

        If no error handler is specified, exceptions are allowed to
        propagate.

        """
        # Manually iterate over records in order to catch exceptions.
        # Use an infinite loop to avoid repeating record handling code.
        record_iterator = self._iterate_records()
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
