"""File utilities"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import collections
import csv
import io
import pathlib
import re

from .include.barnapy import parse

from . import records


class Fingerprint:

    def __init__(self):
        pass

    def path(self):
        return None

    def size(self):
        return None

    def modification_time(self):
        return None

    def items(self): # Especially for writing to config
        return ()

    def init_from_file(self):
        pass

    def __eq__(self, other):
        return False


# Functional file API


def open(file, mode='rt', compression='auto'):
    """Open a file with optional (de)compression.

    file: Filename (str) or pathlib.Path object

    mode: Mode as in the standard io.open

    compression: Compression type.  Expects one of {None, 'auto',
        'gzip', 'bzip2', 'lzma', <extension>:str}.  None means do not
        perform any compression, 'auto' means detect the type of
        compression from the filename extension, and the others specify
        the specific compression to use, either as the compression name
        or a commonly-associated filename extension.

    """
    # Check and convert arguments
    path = None
    if isinstance(file, str):
        path = pathlib.Path(file)
    elif isinstance(file, pathlib.Path):
        path = file
    else:
        raise TypeError('Not a file: {}'.format(file))
    # Detect compression type by extension
    auto = (compression == 'auto')
    if auto:
        compression = path.suffix.lstrip('.')
        if not compression:
            raise ValueError(
                'Cannot detect compression type from filename '
                'extension: {}'.format(path.suffix))

    # Open file based on compression type.  Only import modules as
    # needed.  (I know buried imports are bad style, but it makes this
    # function more compatible with older Pythons that didn't have
    # support for various compressions in the standard library.)
    filename = str(path)
    if compression is None:
        return io.open(filename, mode=mode)
    # Use gzip for all common Lempel-Ziv compression suffixes
    elif compression in ('gz', 'z', 'Z', 'gzip'):
        import gzip
        return gzip.open(filename, mode=mode)
    elif compression in ('bz2', 'bzip2'):
        import bz2
        return bz2.open(filename, mode=mode)
    elif compression in ('xz', 'lzma'):
        import lzma
        return lzma.open(filename, mode=mode)
    elif auto:
        # No compression detected
        return io.open(filename, mode=mode)
    else:
        raise ValueError(
            'Unrecognized compression type: {}'.format(compression))


def read_lines(file, compression='auto'):
    """Open a text file for reading lines and automatically close when done.

    Returns an iterator over lines after opening the file with
    `open(file, mode='rt', compression)`.  See `open` in this module for
    details.

    """
    with open(file, mode='rt', compression=compression) as lines:
        for line in lines:
            yield line
    # File closes as soon as reading is finished


def read_content_lines(
        file,
        compression='auto',
        comment_char='#',
        skip_blank_lines=True,
        enumerate_lines=False,
        ):
    """Read the non-comment, non-blank lines of a file.

    Returns an iterator over the non-comment, non-blank lines of a file
    with optional line numbers.

    file: Filename (str), pathlib.Path object, or iterable of strings
        (e.g. open text file)

    compression: Compression type.  See `open` in this module.

    comment_char: Initial character (after whitespace) that identifies
        comment lines

    skip_blank_lines: Whether to skip blank lines or treat them as
        records

    enumerate_lines: Whether to return (line number, line) pairs.

    """
    # Open file if needed
    if isinstance(file, (str, pathlib.Path)):
        file = read_lines(file, compression=compression)
    # Otherwise assume open file or other iterable of lines

    # Create pattern for detecting comments.  Quote the comment
    # character to avoid regex injection.
    comment_pattern = re.compile('\s*' + re.escape(comment_char))

    # Iterate over lines in the file.  Line numbers start at 1.
    line_num = 0
    for line in file:
        line_num += 1
        # Ignore comment lines
        if comment_pattern.match(line) is not None:
            continue
        # Ignore blank lines (if desired)
        elif skip_blank_lines and line.isspace():
            continue
        # Return content lines with or without line numbers
        elif enumerate_lines:
            yield (line_num, line)
        else:
            yield line


class ContentReader:
    """Reader for the non-comment, non-blank lines of a file.

    This is basically a class version of `read_content_lines`.  The
    difference is that the reader exposes the line number as a property
    whereas it is part of the iterable values in `read_content_lines`.

    """

    def __init__(
            self,
            file,
            compression='auto',
            comment_char='#',
            skip_blank_lines=True,
            ):
        """Create a new ContentReader.

        file: Filename (str), pathlib.Path object, or iterable of
            strings (e.g. open text file)

        compression: Compression type.  See `open` in this module.

        comment_char: Initial character (after whitespace) that
            identifies comment lines

        skip_blank_lines: Whether to skip blank lines or treat them as
            records

        """
        self._file = file
        self._compression = compression
        self._comment_char = comment_char
        self._skip_blank_lines = skip_blank_lines
        self._line_num = 0

    def __iter__(self):
        """Return an iterator over non-comment, non-blank lines."""
        # Open file if needed
        if isinstance(self._file, (str, pathlib.Path)):
            self._file = read_lines(
                self._file, compression=self._compression)
        # Otherwise assume open file or other iterable of lines

        # Create pattern for detecting comments.  Quote the comment
        # character to avoid regex injection.
        comment_pattern = (
            re.compile('\s*' + re.escape(self._comment_char))
            if self._comment_char
            else None)

        # Iterate over lines in the file.  Line numbers start at 1.
        self._line_num = 0
        for line in self._file:
            self._line_num += 1
            # Ignore comment lines
            if (comment_pattern is not None
                    and comment_pattern.match(line) is not None):
                continue
            # Ignore blank lines (if desired)
            elif self._skip_blank_lines and line.isspace():
                continue
            # Return content lines
            else:
                yield line

    @property
    def line_num(self):
        return self._line_num


# TODO redo parsing in terms of `is_*` and constructors b/c need to accomodate various forms of None and bool and extending to other types. LiteralParser class?
# TODO add convenience methods to parse: int_or_none, int_or_orig, etc.

# TODO move exceptions to appropriate module

class RecordException(Exception):

    def __init__(self, source=None, index=None, record=None, *args, **kwargs):
        self._source = source
        self._index = index
        self._record = record

    def __str__(self):
        return 'Bad record in {} at {}: {}'.format(self._source, self._index, self._record)


class RecordValidationException(RecordException):
    pass


class RecordFormatException(RecordException):
    pass


def parse_literal(text):
    if not text or text.isspace():
        return None
    else:
        return parse.literal(text)


def make_record_transformer(header, transformation):
    def record_transformer(record, line_num):
        output = [None] * len(transformation)
        for idx, (field, transform) in enumerate(
                transformation.items()):
            # Handle line_num specially
            if field == 'line_num':
                output[idx] = line_num
                continue
            # Fields can be identified by index or by name
            field_idx = (field
                         if isinstance(field, int)
                         else header[field])
            # Input records can have various lengths, so only transform
            # a field that exists in the input.  Otherwise leave output
            # as None.
            if field_idx < len(record):
                output[idx] = (transform(record[field_idx])
                               if transform is not None
                               else record[field_idx])
        return output
    return record_transformer


# Note: this is just a prototype.  For a proper implementation see
# `delimited.Reader`.
def read_delimited_text( # TODO remove; replace with convenience function `delimited.read`?
        # Input
        file,
        compression='auto',
        input_name=None,
        # File format
        comment_char='#',
        skip_blank_lines=True,
        delimiter=',',
        quote_char='"',
        quote_quote_by_doubling=False,
        escape_char='\\',
        strip_space=False,
        # Input records
        field_names=None,
        # Output records
        output='parse',
        field_parser=parse_literal,
        # Record validation
        validator=None,
        # Error handling
        error_handler=RecordException,
        ):
    """Read delimited text as an iterable of records."""
    # Default input name if not given
    if input_name is None:
        if isinstance(file, str):
            input_name = file
        elif isinstance(file, io.IOBase):
            input_name = file.name
        elif isinstance(file, pathlib.Path):
            input_name = str(file)
        else:
            input_name = general.object_name(file)
    # Read file according to specified format
    reader = ContentReader(
        file,
        compression=compression,
        comment_char=comment_char,
        skip_blank_lines=skip_blank_lines,
        )
    # Create CSV reader with proper format dialect.  Get its iterator
    # right away so that the first record can be treated as a header if
    # specified.
    csvreader = iter(csv.reader(
        reader,
        delimiter=delimiter,
        quotechar=quote_char,
        doublequote=quote_quote_by_doubling,
        escapechar=escape_char,
        skipinitialspace=strip_space,
        ))
    # Interpret field_names
    names2idxs = None
    if field_names is None or isinstance(field_names, dict):
        names2idxs = field_names
    elif field_names == 'header':
        record = next(csvreader)
        names2idxs = dict(
            (name.strip(), idx) for (idx, name) in enumerate(record))
    else:
        names2idxs = dict(
            (name, idx) for (idx, name) in enumerate(field_names))
    # Set up output record processing
    if output is None:
        record_transformer = lambda record, line_num: record
    elif output == 'parse':
        record_transformer = (
            lambda record, line_num:
                [field_parser(field) for field in record])
    else:
        fields = collections.OrderedDict(output)
        # Check output format so that later errors are more likely
        # related to records than setup
        for field, transform in fields.items():
            # Check field names correspond
            if (not isinstance(field, int)
                    and field != 'line_num'
                    and field not in names2idxs):
                raise ValueError(
                    'Field specified in output but not in field'
                    ' names: {}'.format(field))
            # Check transforms are callable
            if (not hasattr(transform, '__call__')
                    and transform is not None):
                raise ValueError(
                    'Not a callable output transformation: {}'
                    .format((field, transform)))
        # Make record transformer
        record_transformer = make_record_transformer(names2idxs, fields)

    # Process each record
    for record in csvreader:
        try:
            # Transform record by projecting and converting fields
            output_record = record_transformer(record, reader.line_num)
            # Validate
            if validator and not validator(output_record):
                # TODO replace raising exception with validation handler to avoid overloading error handler?
                raise RecordValidationException(
                    input_name, reader.line_num, record)
            # Output
            yield output_record
        except Exception as e:
            if error_handler is None:
                pass
            elif (isinstance(error_handler, type)
                  and issubclass(error_handler, Exception)):
                # If the exception is already of the desired type, just
                # raise
                if isinstance(e, error_handler):
                    raise
                # If the desired type is RecordException, construct with
                # field instead of message
                elif issubclass(error_handler, RecordException):
                    raise error_handler(
                        input_name, reader.line_num, record) from e
                # Otherwise raise a new exception from the current one
                else:
                    raise error_handler(
                        'Bad record in {} at line {}: {}'.format(
                            input_name, reader.line_num, record)
                    ) from e
            elif hasattr(error_handler, '__call__'):
                error_handler(e, record, input_name, reader.line_num)
            else:
                raise
