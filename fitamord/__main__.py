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
        pipe_separated = delimited.Format(delimiter='|')
        header = records.Header(
            ('patient_id', int), ('age', float), ('event', str))
        # Set up loading transformations (?)
        # Read delimited file
        tabular_file = delimited.File(
            filename, table_name, pipe_separated, header)
        reader = tabular_file.reader() # TODO enable readers to be context manager
        # Bulk load records from file into table
        table = db.make_table(table_name, header)
        table.add_all(reader)
        # commit?
        # close file?

    # Above: ahdb.  Below: fitamord.

    # Define events

    # Clean data

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
