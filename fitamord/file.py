# Reading and processing tabular file formats
#
# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import io
import pathlib
import re

from . import records


class Format:
    pass


class DelimitedText(records.RecordStream):
    pass


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
