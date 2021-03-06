"""
Delimited text files

Classes and functions for describing, reading, transforming, and
otherwise working with tabular data in delimited text files.
"""

# Copyright (c) 2018 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import collections
import copy
import csv
import io
import itertools as itools
from enum import Enum

from barnapy import files
from barnapy import logging
from barnapy import parse

from . import datatypes
from . import file
from . import general
from . import records


class EscapeStyle(Enum):
    """Enumeration of styles of escaping quotation marks"""
    escaping = 1
    doubling = 2


class Format:
    """Format of a delimited text file"""

    @staticmethod
    def detect(sample_text): # TODO change to return ok rather than exception?
        """Cannot detect escape characters"""
        # Detect delimiter and quoting using the Sniffer internals to
        # avoid defaulting of quote character.  (This follows csv.py.)
        sniffer = csv.Sniffer()
        quote_char, doubling, delimiter, strip = (
            sniffer._guess_quote_and_delimiter(sample_text, None))
        if not delimiter:
            delimiter, strip = sniffer._guess_delimiter(
                sample_text, None)
        if not delimiter:
            raise Exception("Detecting delimiter failed")
        # Detect header
        has_header = sniffer.has_header(sample_text)
        # Interpret the detections
        if quote_char == '':
            quote_char = None
        escape_style = None
        if quote_char and doubling:
            escape_style = EscapeStyle.doubling
        # Return a Format
        return Format(
            delimiter=delimiter,
            quote_char=quote_char,
            escape_style=escape_style,
            data_start_line=(2 if has_header else None),
        )


    def __init__(
            self,
            delimiter=None,
            quote_char=None,
            escape_char=None,
            escape_style=None,
            comment_char=None,
            skip_blank_lines=True,
            data_start_line=None,
    ):
        """
        Create a new delimited text file format

        delimiter: String that separates fields in a record

        quote_char: String that starts and ends a quoted field

        escape_char: String that removes the special meaning of the
            following character

        escape_style: How literal quote characters are included in
            fields

        comment_char: String to use as comment start indicator

        skip_blank_lines: Whether blank lines are considered records or
            not.

        data_start_line: Number of the non-comment line where the data
            starts.  For example, if the file has a header row, this
            should be 2.  The default is 1.
        """
        self._delimiter = delimiter
        self._quote_char = quote_char
        self._escape_char = escape_char
        # Interpret escape style names
        self._escape_style = (escape_style
                              if (isinstance(escape_style, EscapeStyle)
                                  or escape_style is None)
                              else EscapeStyle[escape_style])
        self._comment_char = comment_char
        self._skip_blank_lines = skip_blank_lines
        # Validate data_start_line
        if not (data_start_line is None
                or (isinstance(data_start_line, int)
                    and data_start_line >= 1)):
            raise ValueError(
                'data_start_line: Not an integer >= 1: {}'
                .format(data_start_line))
        self._data_start_line = data_start_line
        # Validate format
        if not self.is_valid():
            raise ValueError('Invalid format: {!r}'.format(self))

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
    def comment_char(self):
        return self._comment_char

    @property
    def skip_blank_lines(self):
        return self._skip_blank_lines

    @property
    def data_start_line(self):
        return (self._data_start_line
                if self._data_start_line is not None
                else 1)

    def as_dict(self):
        return collections.OrderedDict((
            ('delimiter', self.delimiter),
            ('quote_char', self.quote_char),
            ('escape_char', self.escape_char),
            ('escape_style', self.escape_style),
            ('comment_char', self.comment_char),
            ('skip_blank_lines', self.skip_blank_lines),
            ('data_start_line', self.data_start_line),
        ))

    def as_yaml_object(self):
        _dict = self.as_dict()
        # Replace non-YAML values with strings
        if _dict['escape_style'] is not None:
            _dict['escape_style'] = _dict['escape_style'].name
        return _dict

    def __repr__(self):
        fields = []
        for name, value in self.as_dict().items():
            fields.append('{}={!r}'.format(name, value))
        return '{}({})'.format(general.fq_typename(self), ', '.join(fields))

    def is_valid(self):
        """
        Whether this Format is consistent and valid for parsing

        A valid format must define at least the delimiter.  Beyond that,
        quoting and escaping can take various forms: literal (no quoting
        and no escaping), only escaping, only quoting (possibly with
        doubling for escaping), or both quoting and escaping.  Quoting
        without escaping is uncommon (e.g. single-quoted strings in
        Bash), and so is supported mainly as a technical point.
        """
        return (
            # Must specify delimiter no matter what
            self._delimiter is not None and
            (
                # Literal (no quoting and no escaping)
                (self._quote_char is None and
                 self._escape_char is None and
                 self._escape_style is None)
                or
                # Only escaping
                (self._quote_char is None and
                 self._escape_char is not None and
                 self._escape_style == EscapeStyle.escaping)
                or
                # Only quoting or quoting with doubled escaping
                (self._quote_char is not None and
                 self._escape_char is None and
                 self._escape_style != EscapeStyle.escaping)
                or
                # Quoting with an escape character
                (self._quote_char is not None and
                 self._escape_char is not None and
                 self._escape_style == EscapeStyle.escaping)
            )
        )

    def derive(self, **kwargs):
        """
        Return a new Format derived from this format

        Creates a new Format with the same values as this format, except
        as overridden in `kwargs`.  Any of the Format constructor
        arguments are valid here.
        """
        new_fields = self.as_dict()
        new_fields.update(kwargs)
        return Format(**new_fields)

    def csv_dialect(self):
        class dialect(csv.Dialect):
            delimiter = self.delimiter
            quotechar = self.quote_char
            escapechar = self.escape_char
            doublequote = self.escape_style is EscapeStyle.doubling
            quoting = (csv.QUOTE_MINIMAL
                       if self.quote_char
                       else csv.QUOTE_NONE)
            skipinitialspace = True
            lineterminator = '\n'
        return dialect


# Common formats

"""Analog of csv.excel"""
Format.EXCEL_CSV = Format(
    delimiter=',',
    quote_char='"',
    escape_style='doubling',
)

"""Analog of csv.excel_tab"""
Format.EXCEL_TAB = Format.EXCEL_CSV.derive(delimiter='\t')

"""
Programming language style CSV: commas, quoting with double quotes
only as needed, escaping with backslash not doubling, octothorpe
comments, whitespace insensitive.
"""
Format.PROGRAMMING_CSV = Format(
    delimiter=',',
    quote_char='"',
    escape_char='\\',
    escape_style='escaping',
    comment_char='#',
    skip_blank_lines=True,
)


def infer_header(format, sample): # make method of Format?
    # Read records in sample
    csv_reader = csv.reader(
        io.StringIO(sample), dialect=format.csv_dialect())
    # Detect column names
    row = next(csv_reader)
    if format.data_start_line > 1:
        names = row
        row = next(csv_reader)
    else:
        names = ['x' + str(i + 1) for i in range(len(row))]
    # Guess types by parsing the first non-header rows
    col_types = [
        collections.Counter((type(parse_literal(field)),))
        for field in row]
    for row in itools.islice(csv_reader, 100):
        for col_idx, col_val in enumerate(row):
            col_type = type(parse_literal(col_val))
            col_types[col_idx][col_type] += 1
    # Choose the most common type (except NoneType) as the type.
    # Default to any parseable atom.  Use a text-aware data types.
    types = [datatypes.Atom] * len(col_types)
    for idx, counter in enumerate(col_types):
        for col_type, frequency in counter.most_common():
            if col_type != type(None):
                types[idx] = datatypes.types2datatypes[col_type]
                break
    # Build header and return
    return records.Header(names=names, types=types)


class File:

    # TODO sort out header in the file from header object created to interpret file

    def __init__(
            self,
            path,
            format=None,
            name=None,
            header=None,
    ):
        self._path = files.new(path)
        self._name = name if name is not None else self._path.stem
        self._format = format
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
    def header(self):
        return self._header

    def reader(self, is_missing=None, error_handler=None):
        return Reader(
            path=self.path,
            format=self.format,
            name=self.name,
            header=self.header,
            is_missing=is_missing,
            error_handler=error_handler,
        )

    def init_from_file(self, sample_size=(2 ** 20)):
        logger = logging.getLogger(general.fq_typename(self))
        # Read the first part of the file to use as a sample
        logger.info('Reading sample from: {}', self.path)
        with self.path.open('rt') as csv_file:
            sample = csv_file.read(sample_size)
        # Detect format
        if self.format is None:
            logger.info('Detecting format of: {}', self.path)
            self._format = Format.detect(sample)
        # Infer header
        if self.header is None:
            logger.info('Detecting header of: {}', self.path)
            self._header = infer_header(self.format, sample)

    def __str__(self):
        return str(self._path)


class Reader(records.RecordStream): # TODO enable to be context manager?

    def __init__(
            self,
            path,
            format=None,
            name=None,
            header=None,
            is_missing=None,
            error_handler=None,
    ):
        self._path = files.new(path)
        self._name = name if name is not None else self._path.stem
        self._format = format
        self._header = header
        self._is_missing = is_missing
        self._inv_projection = None
        super().__init__(
            records=None,
            name=self._name,
            header=self._header,
            provenance=self._path,
            error_handler=error_handler,
            is_reiterable=True,
        )

    @property
    def path(self):
        return self._path

    def project(self, *columns):
        # Must have a header in order to project
        if self.header is None:
            raise ValueError(
                'Cannot project using invalid header: None')
        # Create inverse projection (new field idxs to old field idxs)
        new2old_field_idxs = []
        for column in columns:
            # Verify column name or index
            if not self.header.has_field(column):
                raise ValueError('No such column: {}'.format(column))
            old_idx = self.header.index_of(column)
            # Compose with existing projection (if any)
            if self._inv_projection is not None:
                old_idx = self._inv_projection[old_idx]
            new2old_field_idxs.append(old_idx)
        # Create projected header
        header = self.header.project(*columns)
        # Create new record stream and update it with the projection
        records = copy.copy(self)
        records._header = header
        records._inv_projection = new2old_field_idxs
        return records

    def _record_iterator(self):
        # Must have a format in order to read the file
        if self._format is None:
            raise ValueError(
                'Cannot read file using invalid format: None')
        # Create reader for content
        reader = file.ContentReader(
            str(self.path),
            comment_char=self._format.comment_char,
            skip_blank_lines=self._format.skip_blank_lines,
            )
        # Create CSV reader
        csv_reader = csv.reader(
            reader, dialect=self._format.csv_dialect())
        # Skip to the first data record (to avoid trying to parse the
        # file header)
        if self._format.data_start_line:
            csv_reader = iter(csv_reader)
            for line_num in range(self._format.data_start_line - 1):
                next(csv_reader, None)
        # Set up field parsing
        parsers = []
        for typ in self.header.types():
            if isinstance(typ, datatypes.TextValueType):
                parsers.append(typ.parse)
            else:
                if typ in datatypes.types2datatypes:
                    parsers.append(datatypes.types2datatypes[typ].parse)
                else:
                    parsers.append(None)
        # Make record processing loop and return
        return project_transform_records(
            csv_reader,
            parsers,
            self._is_missing,
            self._error_handler,
            self._inv_projection,
            )


def project_transform_records(
        records_,
        transformers,
        is_missing=None,
        error_handler=None,
        inv_projection=None,
):
    n_fields = len(transformers)
    for record in records_:
        err = None
        new_record = [None] * n_fields
        for out_idx in range(n_fields):
            # Find the input index corresponding to the output index
            in_idx = (inv_projection[out_idx]
                      if inv_projection is not None
                      else out_idx)
            # Leave field as None if it doesn't exist in record
            if in_idx >= len(record):
                continue
            field = record[in_idx]
            # Leave field as None if missing
            if is_missing is not None and is_missing(field):
                continue
            # Transform the field if defined
            transformer = transformers[out_idx]
            if transformer is None:
                new_record[out_idx] = field
            else:
                new_field, err = transformer(field)
                if err is None:
                    new_record[out_idx] = new_field
                else:
                    err = records.RecordError(
                        'Bad record: {}: {}'.format(record, err))
                    if error_handler is not None:
                        error_handler(err)
                        break
                    else:
                        raise err
        if err is None:
            yield new_record


def parse_literal(text):
    # Short circuit parsing if not text
    if not isinstance(text, str):
        return text
    # Treat whitespace as None
    if parse.is_empty(text):
        return None
    # Try to parse as a common type of literal
    value, err = parse.atom_err(text)
    # If successful, return parsed value, otherwise just return the
    # original text
    if err is None:
        return value
    else:
        return text
