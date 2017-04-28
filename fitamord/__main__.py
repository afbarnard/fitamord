"""Program to transform relational data into feature vectors"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import sys

from barnapy import logging

from .engines import sqlite


def main(args=None):
    # Default args to sys.argv
    if args is None:
        args = sys.argv[1:]

    # Start logging
    logging.default_config()

    # Read config, process command line, create environment

    # Write assembled config (as "shadow")

    # Search for tabular files
    recognized_extensions = {'csv', 'tsv'}
    filenames = []

    # Determine schemas of tabular files

    # Set up loading transformations

    # Load tabular files into DB
    db_filename = '.fitamord.sqlite'
    db = sqlite.SqliteDb(db_filename) # TODO separate establishing connection from construction to enable context manager

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
