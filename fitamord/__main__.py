"""Program to transform relational data into feature vectors"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import os.path
import sys

from barnapy import logging
from barnapy import unixutils

from . import delimited
from . import records
from .engines import sqlite


def main(args=None):
    # Default args to sys.argv
    if args is None:
        args = sys.argv[1:]
    base_directory = (args[0] if args else '.')

    # Start logging
    logging.default_config()
    logger = logging.getLogger('main')

    # Read config, process command line, create environment

    # Write assembled config (as "shadow")

    # Search for tabular files
    compression_extensions = {'gz', 'bz2', 'xz'}
    tabular_extensions = {'csv'}
    filenames = []
    for filename in unixutils.ls(base_directory)[1]:
        relative_filename = os.path.join(base_directory, filename)
        # TODO replace following with `looks_like_tabular_file`
        components = [piece.lower() for piece in filename.split('.')]
        if (components[0] != ''
                and ((len(components) > 1
                      and components[-1] in tabular_extensions)
                     or (len(components) > 2
                         and components[-2] in tabular_extensions
                         and components[-1] in compression_extensions))
                and os.path.isfile(relative_filename)):
            filenames.append(relative_filename)
    assert filenames # what is appropriate error, if any?

    # Connect to DB
    db_filename = '.fitamord.sqlite'
    db = sqlite.SqliteDb(db_filename) # TODO separate establishing connection from construction to enable context manager

    # Load tabular files into DB
    for filename in filenames:
        # Table name
        table_name = os.path.basename(filename).split('.')[0]
        # Determine file format and table schema
        tabular_file = delimited.File(filename)
        tabular_file.init_from_file()
        # Set up loading transformations (?) # TODO
        # Read delimited file
        reader = tabular_file.reader()

        # Can records be interpreted as patient events?
        header = reader.header
        if header.has_field('age'):
            # Guess event is first non-ID, non-age field
            idxs = (header.index_of('patient_id'),
                    header.index_of('age'))
            for idx in range(len(header)):
                if idx not in idxs:
                    break
            event_name = header.name_at(idx)
            reader = reader.project('patient_id', 'age', event_name)

        # Bulk load records from file into table
        table = db.make_table(reader.name, reader.header)
        table.add_all(reader)
        logger.info(
            'Loaded {} records from {} into {}'
            .format(table.count_rows(), filename, table.name))

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
