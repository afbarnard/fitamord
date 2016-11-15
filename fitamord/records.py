# Core record processing
#
# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

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


class Header:
    """A description of a collection of fields where each field has a name
    and a type.

    The type of a record or the element type of a collection of records
    or a table.

    """

    def __init__(self, names=None, types=None, fields=None):
        self._fields = []
        self._names2idxs = {}
        for name, typ in zip(names, types):
            self._names2idxs[name] = len(self._fields)
            self._fields.append(Field(name, typ))
        for field in fields:
            if not isinstance(field, Field):
                # Unpack iterable (treat as a pair)
                name, typ, *_ = field
                field = Field(name, typ)
            self._names2idxs[field.name] = len(self._fields)
            self._fields.append(field)

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
        for idx, value in enumerate(record):
            if (idx >= len(self._fields)
                or not self._fields[idx].isinstance(value)):
                return False
        return True


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
    pass
