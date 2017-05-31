"""Program to transform relational data into feature vectors"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import itertools as itools
import sys

from barnapy import files
from barnapy import logging

from . import config
from . import delimited
from . import relational
from . import version
from .engines import sqlite


# Constants that may need to be parameterized

# Events
_ev_len = 3
_pt_id_idx = 0
_time_idx = 1
_evtype_idx = 2
# Examples
_ex_len = 4
_time_start_idx = 1
_time_stop_idx = 2
_label_idx = 3


# Data validation # TODO should this go elsewhere?


def make_is_missing(missing_values):
    missing_strs = {
        (str(val).strip().lower() if val is not None else None)
        for val in missing_values}
    def is_missing(text):
        return text.strip().lower() in missing_strs
    return is_missing


def is_valid_fact(record):
    return record[0] is not None


def is_valid_event(record):
    return all(record[i] is not None for i in range(3))


def is_valid_example(record):
    return (all(record[i] is not None for i in range(4))
            and type(record[1]) == type(record[2]) # TODO replace with "can type 1 be ordered wrt type 2?"
            and record[1] <= record[2])


def make_discard_logger(logger, message):
    def discard_logger(record):
        logger.info(message, record)
    return discard_logger


def make_record_filter(filter, discard_handler):
    def record_filter(record):
        if filter(record):
            return True
        else:
            discard_handler(record)
            return False
    return record_filter


# Command line API


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
    logger.info('Fitamord {}', version.__version__)
    logging.log_runtime_environment(logger)

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
    config.save(config_obj, gen_config_file, insert_defaults=True)

    # Validate data treatments.  This has to be done after writing the
    # configuration to make sure there is a configuration to refer to in
    # the case of configuration detection.
    config_obj.validate_data_treatments()

    # TODO end: generate and validate configuration

    # Create function for recognizing missing values
    is_missing = (make_is_missing(config_obj.is_missing)
                  if config_obj.is_missing
                  else None)

    # Connect to DB
    db_file = base_directory.join(db_filename)
    db = sqlite.SqliteDb(db_file.path) # TODO separate establishing connection from construction to enable context manager

    # Load tabular files into DB
    for table_cfg in config_obj.tables:
        # Check if file exists
        table_file = base_directory.join(table_cfg.filename)
        if not table_file.is_readable_file():
            logger.error(
                'Loading failed: Not a readable file: {}', table_file)
            continue
        # Set up for reading
        tabular_file = delimited.File(
            path=table_file,
            format=table_cfg.format,
            name=table_cfg.name,
            header=table_cfg.header,
            )
        # Detect format and header if needed # TODO replace with reusable per-file config detection
        if tabular_file.format is None or tabular_file.header is None:
            tabular_file.init_from_file()
            if (tabular_file.format is None
                    or tabular_file.header is None):
                logger.error(
                    'Loading failed: '
                    'Format or header detection failed: {}',
                    table_file)
                continue
        # Read delimited file
        reader = tabular_file.reader(is_missing)
        # Project (this is pushed down below field parsing)
        if table_cfg.use_columns:
            reader = reader.project(*table_cfg.use_columns)
        if (table_cfg.treat_as == 'events'
                and len(reader.header) > _ev_len):
            reader = reader.project(*range(_ev_len))
        elif (table_cfg.treat_as == 'examples'
              and len(reader.header) > _ex_len):
            reader = reader.project(*range(_ex_len))
        # Bulk load records from file into table
        table = db.make_table(reader.name, reader.header)
        table.add_all(reader)
        logger.info(
            'Loaded {} records from {} into {}',
            table.count_rows(), table_file.path, table.name)

    # Above: ahdb.  Below: fitamord.  (Except validation pushed up
    # before data loading as much as possible.)

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
    # *** No, just assume based on data treatment ("treat as") and
    # position or names ("use") as given in config
    # TODO convert config to interpret "event(patient_id, age, dx_code)" and the like

    # Types of tables: demographics, events (drugs, conds, procs,
    # symptoms), events with values (labs, vitals), labels

    # Recognize study periods by (patient ID, start, stop, dose, label)
    # format

    # Look up tables and data treatment
    db_tables = {}
    tables2treats = {}
    for table_cfg in config_obj.tables:
        table = db.table(table_cfg.name)
        db_tables[table.name] = table
        tables2treats[table.name] = table_cfg.treat_as
    tables = dict(db_tables)

    # Record filters with logging discarders # TODO upgrade to add line numbers (from original file) to error messages
    validation_logger = logging.getLogger('clean data')
    filters = {
        'facts': make_record_filter(
            is_valid_fact,
            make_discard_logger(
                validation_logger,
                'Discarding: Not a valid fact: {}')),
        'events': make_record_filter(
            is_valid_event,
            make_discard_logger(
                validation_logger,
                'Discarding: Not a valid event: {}')),
        'examples': make_record_filter(
            is_valid_example,
            make_discard_logger(
                validation_logger,
                'Discarding: Not a valid example: {}')),
        }

    # Clean data: drop events without valid patient IDs, event IDs, or
    # times / ages
    for name, table in tables.items():
        treatment = tables2treats[name]
        # Retain only valid records, discard others
        tables[name] = table.select(filters[treatment])

    # Collect event types

    # Define features

    # Merge-collect records based on patient ID
    for record_collection in relational.MergeCollect(
            *(tables[k] for k in sorted(tables.keys())),
            key=_pt_id_idx):
        print(record_collection)

    # Make a feature vector for each patient

    # Output feature vector (in dense or sparse form)

    # Cleanup # TODO write and use context manager
    db.close()

    # Subsequent: scikit


if __name__ == '__main__':
    main()
