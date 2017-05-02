"""Delimited text files

Classes and functions for describing, reading, transforming, and
otherwise working with tabular data in delimited text files.

"""
# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import csv
import io
import pathlib
from enum import Enum

from . import file
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
            format=None,
            name=None,
            header=None,
            fingerprint=None,
            ):
        self._path = (path
                      if isinstance(path, pathlib.Path)
                      else pathlib.Path(path))
        self._name = (name
                      if name is not None
                      else self._path.name.split('.')[0])
        self._format = format
        self._header = header
        self._fingerprint = fingerprint

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

    def reader(self, record_transformation=None):
        return Reader(
            path=self.path,
            format=self.format,
            name=self.name,
            header=self.header,
            record_transformation=record_transformation,
            )

    def init_from_file(self, what='all', sample_size=1048576):
        # Interpret `what` # TODO
        # Read the first part of the file to use as a sample
        with file.open(self.path, 'rt') as csv_file:
            sample = csv_file.read(sample_size)
            # Analyze the sample to determine delimiter and header
            dialect = csv.excel # Default to Excel format
            has_header = False
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                has_header = sniffer.has_header(sample)
            except:
                pass
            csv_file.seek(0)
            csv_reader = csv.reader(csv_file, dialect=dialect)
            row = next(csv_reader)
        # Build format
        if what in ('all', 'format') or 'format' in what:
            self._format = Format(
                comment_char=None,
                delimiter=dialect.delimiter,
                quote_char=dialect.quotechar,
                escape_char=dialect.escapechar,
                escape_style=(EscapeStyle.doubling
                              if dialect.doublequote
                              else EscapeStyle.escaping),
                skip_blank_lines=True,
                )
        # Build header
        if what in ('all', 'header') or 'header' in what:
            if has_header:
                names = row
            else:
                names = ['x' + str(i + 1) for i in range(len(row))]
            self._header = records.Header(names=names)

    def __eq__(self, other):
        # For comparing Files constructed from configuration to those
        # constructed from the file content
        pass


class Reader(records.RecordStream):

    def __init__(
            self,
            path,
            format=None,
            name=None,
            header=None,
            record_transformation=None,
            error_handler=None,
            ):
        self._path = (path
                      if isinstance(path, pathlib.Path)
                      else pathlib.Path(path))
        self._name = (name
                      if name is not None
                      else self._path.name.split('.')[0])
        self._format = format
        self._header = header
        self._rec_txform = record_transformation
        # Initialize superclass
        super().__init__(
            records=None,
            name=self.name,
            header=self.header,
            provenance=self.path,
            error_handler=error_handler,
            is_reiterable=True,
            )

    @property
    def path(self):
        return self._path

    def _record_iterator(self):
        return file.read_delimited_text(
            file=self.path,
            comment_char=self._format.comment_char,
            skip_blank_lines=self._format.skip_blank_lines,
            delimiter=self._format.delimiter,
            quote_char=self._format.quote_char,
            quote_quote_by_doubling=(
                self._format.escape_style == EscapeStyle.doubling),
            escape_char=self._format.escape_char,
            strip_space=True,
            #field_names=list(self.header.names()),
            output=self._rec_txform,
            error_handler=self._error_handler,
            )
