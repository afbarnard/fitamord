"""Program to transform relational data into feature vectors"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import itertools as itools
import re
import sys

from barnapy import files
from barnapy import logging
from barnapy import unixutils

from . import config
from . import delimited
from . import records
from .engines import sqlite


_na_pattern = re.compile(r'\W*not\s*a(?:vail|pplic)able', re.IGNORECASE)

def is_na(text):
    return _na_pattern.match(text) is not None


def extension_combinations(extensions, *extension_collections):
    ext_colls = [sorted(extensions)]
    ext_colls += [[None] + sorted(ec) for ec in extension_collections]
    for tup in itools.product(('',), *ext_colls):
        yield '.'.join(str(e) for e in tup if e is not None)


def main(args=None): # TODO split into outer main that catches and logs exceptions and inner main that raises exceptions
    # Default args to sys.argv
    if args is None:
        args = sys.argv[1:]
    base_directory = files.File(args[0] if args else '.')

    # Definitions which should be configurable
    tabular_extensions = {'csv'}
    compression_extensions = {'gz', 'bz2', 'xz'}
    config_filename = 'fitamord_config.yaml'
    generated_config_filename = 'fitamord_config.generated.yaml'
    db_filename = 'fitamord.sqlite'

    # Start logging
    logging.default_config()
    logger = logging.getLogger('main')

    # Check base directory exists
    if not base_directory.is_readable_directory():
        logger.error('Not a readable directory: {}', base_directory)
        return

    # Read config # TODO process command line, create environment
    config_file = base_directory.join(config_filename)
    if config_file.is_readable_file():
        # Load configuration
        logger.info('Loading configuration from: {}', config_file)
        config_obj = config.load(config_file)
        if not config_obj.tables:
            logger.error('No tables defined in: {}', config_file)
            return
    else:
        # Guess configuration
        ext_combs = list(extension_combinations(
            tabular_extensions, compression_extensions))
        logger.info(
            'No configuration file specified: '
            'Detecting configuration from files matching: {}/*{{{}}}',
            base_directory,
            ','.join(ext_combs))
        config_obj = config.detect(base_directory, ext_combs)
        if not config_obj.tables:
            logger.error('No tables detected in: {}', base_directory)
            return

    # Write config
    gen_config_file = base_directory.join(generated_config_filename)
    logger.info('Writing configuration to: {}', gen_config_file)
    config.save(config_obj, gen_config_file)

    # Connect to DB
    db_file = base_directory.join(db_filename)
    db = sqlite.SqliteDb(db_file.path) # TODO separate establishing connection from construction to enable context manager

    # Load tabular files into DB
    for table in config_obj.tables:
        # Check if file exists
        table_file = base_directory.join(table.filename)
        if not table_file.is_readable_file():
            logger.error(
                'Loading failed: Not a readable file: {}', table_file)
            continue
        # Set up for reading
        tabular_file = delimited.File(
            path=table_file,
            format=table.format,
            name=table.name,
            header=table.header,
            )
        # Detect format and header if needed # TODO replace with reusable per-file config detection
        if tabular_file.format is None or tabular_file.header is None:
            tabular_file.init_from_file()
            if tabular_file.format is None or tabular_file.header is None:
                logger.error(
                    'Loading failed: '
                    'Format or header detection failed: {}',
                    table_file)
                continue
        is_missing = lambda: False # FIXME
        # Read delimited file
        reader = tabular_file.reader()
        # Project
        if table.use_columns:
            reader = reader.project(*table.use_columns)
        elif table.treat_as == 'events' and len(reader.header) > 3:
            reader = reader.project(*range(3))
        elif table.treat_as == 'examples' and len(reader.header) > 4:
            reader = reader.project(*range(4))
        # Bulk load records from file into table
        table = db.make_table(reader.name, reader.header)
        table.add_all(reader)
        logger.info(
            'Loaded {} records from {} into {}'
            .format(table.count_rows(), table_file.path, table.name))

    # Above: ahdb.  Below: fitamord.

    # So, the proper way to treat tabular files is as a `Table` and not
    # as a `RecordStream`.  That is, the table would know some things
    # about the file (format and header) and basically be implemented in
    # terms of iterating over records using the `csv` module.  (The only
    # data structure is the file.)  For this to work somewhat well, the
    # column types should be something other than `str` and the values
    # should be parsed according to that type (if possible, otherwise
    # just return original field text).  In addition to knowing how to
    # parse each column, the table will need to know what values
    # represent missing values so they can be parsed as `None`.  This
    # much will essentially reproduce SQLite behavior in that columns
    # will have type affinity (the text in their fields can be expected
    # to parse as the given type) but will actually store any object,
    # falling back to the raw text as needed.  For efficiency, the table
    # should push down projection so that it occurs before parsing.  The
    # record iterator for the table should have a line number attribute
    # and potentially a source line (text) attribute to facilitate error
    # handling.

    # include relational rename in projection like in SQL?

    # Define events

    # Guess at fields that make up an event: first integer field is
    # patient ID, first float field is time / age, first str field is
    # event ID.  Or require and use names?

    # Types of tables: demographics, events (drugs, conds, procs,
    # symptoms), events with values (labs, vitals), labels

    # Recognize study periods by (patient ID, start, stop, dose, label)
    # format

    # Clean data: drop events without valid patient IDs, event IDs, or
    # times / ages

    # Collect event types

    # Define features

    # Merge-collect records based on patient ID
    key_fields = ('patient_id',)

    # Make a feature vector for each patient

    # Output feature vector (in dense or sparse form)

    # Cleanup # TODO write and use context manager
    db.close()

    # Subsequent: scikit


if __name__ == '__main__':
    main()
