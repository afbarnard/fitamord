"""Delimited text files

Classes and functions for describing, reading, transforming, and
otherwise working with tabular data in delimited text files.

"""
# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import io
import pathlib
from enum import Enum

from . import records


class EscapeStyle(Enum):
    """Enumeration of styles of escaping quotation marks"""
    escaping = 1
    doubling = 2


class Format:
    """Format of a delimited text file"""

    def __init__(
            self,
            comment_char=None,
            delimiter=None,
            quote_char=None,
            escape_char=None,
            escape_style=None,
            skip_blank_lines=None,
            ):
        """Create a new delimited text file format.

        comment_char: String to use as comment start indicator

        delimiter: String that separates fields in a record

        quote_char: String that starts and ends a quoted field

        escape_char: String that removes the special meaning of the
            following character

        escape_style: How literal quote characters are included in
            fields

        skip_blank_lines: Whether blank lines are considered records or
            not.

        """
        self._comment_char = comment_char
        self._delimiter = delimiter
        self._quote_char = quote_char
        self._escape_char = escape_char
        # Interpret escape style names
        self._escape_style = (escape_style
                              if (isinstance(escape_style, EscapeStyle)
                                  or escape_style is None)
                              else EscapeStyle[escape_style])
        self._skip_blank_lines = skip_blank_lines

    @property
    def comment_char(self):
        return self._comment_char

    @property
    def delimiter(self):
        return self._delimiter

    @property
    def quote_char(self):
        return self._quote_char

    @property
    def escape_char(self):
        return self._escape_char

    @property
    def escape_style(self):
        return self._escape_style

    @property
    def skip_blank_lines(self):
        return self._skip_blank_lines

    def items(self):
        """Return fields as an iterable of (name, value) pairs."""
        yield 'comment_char', self._comment_char
        yield 'delimiter', self._delimiter
        yield 'quote_char', self._quote_char
        yield 'escape_char', self._escape_char
        yield 'escape_style', self._escape_style
        yield 'skip_blank_lines', self._skip_blank_lines

    def __repr__(self):
        buffer = io.StringIO()
        buffer.write('Format(')
        for idx, (name, value) in enumerate(self.items()):
            if idx > 0:
                buffer.write(', ')
            buffer.write(name)
            buffer.write('=')
            buffer.write(repr(value))
        buffer.write(')')
        return buffer.getvalue()

    def is_valid(self):
        """Whether this Format is fully specified enough for parsing."""
        return (self._delimiter is not None
                and self._quote_char is not None
                and self._escape_char is not None
                and self._escape_style is not None)

    def derive(self, **kwargs):
        """Return a new Format derived from this format.

        Creates a new Format with the same values as this format, except
        as overridden in `kwargs`.  Any of the Format constructor
        arguments are valid here.

        """
        new_fields = dict(self.items())
        new_fields.update(kwargs)
        return Format(**new_fields)


# Common formats

"""Analog of csv.excel"""
Format.EXCEL_CSV = Format(
    delimiter=',',
    quote_char='"',
    escape_char='\\',
    escape_style='doubling',
    )

"""Analog of csv.excel_tab"""
Format.EXCEL_TAB = Format.EXCEL_CSV.derive(delimiter='\t')

"""Programming language style CSV: commas, quoting with double quotes
only as needed, escaping with backslash not doubling, octothorpe
comments, whitespace insensitive.

"""
Format.PROGRAMMING_CSV = Format(
    comment_char='#',
    delimiter=',',
    quote_char='"',
    escape_char='\\',
    escape_style='escaping',
    skip_blank_lines=True,
    )


class File:

    def __init__(
            self,
            path,
            name=None,
            format=None,
            fingerprint=None,
            header=None,
            ):
        self._path = (path
                      if isinstance(path, pathlib.Path)
                      else pathlib.Path(path))
        self._name = (name
                      if name is not None
                      else self._path.basename)
        self._format = format
        self._fingerprint = fingerprint
        self._header = header

    @property
    def path(self):
        """Filesystem path of this file"""
        return self._path

    @property
    def name(self):
        """Name of the table represented by this file"""
        return self._name

    @property
    def format(self):
        return self._format

    @property
    def fingerprint(self):
        return self._fingerprint

    @property
    def header(self):
        return self._header

    def reader(self, record_transformation='parse'):
        pass

    def init_from_file(self, what='all'):
        pass

    def __eq__(self, other):
        # For comparing Files constructed from configuration to those
        # constructed from the file content
        pass


class Reader(records.RecordStream):

    def __init__(
            self,
            delimited_text_file,
            record_transformation='parse',
            ):
        pass
