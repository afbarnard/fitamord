"""Program to transform relational data into feature vectors"""

# Copyright (c) 2018 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


# TODO convert to script (running this module as main prevents debugging with `-m pdb`)


import collections
import itertools as itools
import sys

from barnapy import files
from barnapy import logging
import barnapy.general

from . import __version__
from . import config
from . import database
from . import delimited
from . import features
from . import file
from . import general
from . import records
from . import relational
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


# Data treatment # TODO should this go elsewhere?


def make_recognizer(matching_values): # TODO move to general?
    matches = set()
    for val in matching_values:
        # Canonicalize string values
        if isinstance(val, str):
            matches.add(val.strip().lower())
        # Match other values as is
        else:
            matches.add(val)
    def recognizer(obj):
        if isinstance(obj, str):
            return obj.strip().lower() in matches
        return obj in matches
    return recognizer


# Data validation # TODO should this go elsewhere?


def is_valid_fact(record):
    return record[0] is not None


def is_valid_event(record):
    return (record[0] is not None and
            record[1] is not None and
            record[2] is not None)


def is_valid_example(record):
    # See if "from" is before "upto"
    is_ordered = record[1] is None or record[2] is None
    # Both values are given so check their order
    if not is_ordered:
        try:
            is_ordered = record[1] <= record[2]
        except TypeError as e:
            is_ordered = False
    return (record[0] is not None and
            record[3] is not None and
            is_ordered)


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


# Data interpretation


def interpret_facts(record_collection, fact_table_names, headers):
    facts = []
    for table_name in fact_table_names:
        for record in record_collection[table_name]:
            for field_idx, field_name in enumerate(
                    headers[table_name].names()):
                facts.append(((table_name, field_name),
                              record[field_idx]))
    return facts


def interpret_events(record_collection, event_table_names, headers):
    events = []
    for table_name in event_table_names:
        field_name = headers[table_name].name_at(2)
        for record in record_collection[table_name]:
            if len(record) == 3:
                _, when, what = record
                value = None
            elif len(record) == 4:
                _, when, what, value = record
            else:
                raise ValueError('Uninterpretable event record: {!r}'
                                 .format(record))
            events.append((when, (table_name, field_name, what), value))
    return events


def interpret_examples(record_collection, example_table_names):
    examples = []
    for table_name in example_table_names:
        examples.extend(record_collection[table_name])
    return examples


# Input / Output


def print_as_svmlight(label, feature_vector_dict, file=sys.stdout):
    print(label, end='', file=file)
    for idx in sorted(feature_vector_dict.keys()):
        print(' ', idx, ':', feature_vector_dict[idx],
              sep='', end='', file=file)
    print(file=file)


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
    generated_features_filename = 'features.generated.csv'
    db_filename = 'fitamord.sqlite'

    # Start logging
    logging.default_config()
    logger = logging.getLogger('main')
    logger.info('Fitamord {}', __version__)
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
    is_missing = (make_recognizer(config_obj.is_missing)
                  if config_obj.is_missing
                  else None)

    # Connect to DB
    db_file = base_directory.join(db_filename)
    db = sqlite.SqliteDb(db_file.path) # TODO separate establishing connection from construction to enable context manager

    # Create, if needed, a table for tracking loading of tables from
    # delimited files.  A fingerprint consists of the size and
    # modification time of the file.  The additional items needed are
    # the header and a flag for whether the table was successfully
    # loaded.
    load_dlms_name = '_fitamord_load_dlm'
    load_dlms = None
    load_dlms_hdr = records.Header(
        ('name', str),
        ('size', int),
        ('mtime', str), # Use str b/c mtime_ns may overflow 64-bit int
        ('header', str),
        ('loaded', int),
    )
    if (not db.exists(load_dlms_name) or
            db.typeof(load_dlms_name) != database.DbObjectType.table):
        load_dlms = db.make_table(load_dlms_name, load_dlms_hdr)
    else:
        load_dlms = db.table(load_dlms_name)
        # Attach header because parsing header from SQL is not implemented (TODO)
        load_dlms._header = load_dlms_hdr

    # Load tabular files into DB
    reader_logger = logging.getLogger(
        general.fq_typename(delimited.Reader))
    for table_cfg in config_obj.tables:
        logger.info("Loading '{}'", table_cfg.name)
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
        # Project header to create a header for the loaded data.  The
        # header in the config applies to the tabular file, not
        # necessarily to the data loaded in the DB, which is projected
        # through the "use:" config attribute.
        data_header = tabular_file.header
        if table_cfg.use_columns:
            data_header = data_header.project(*table_cfg.use_columns)
        # Check if table has already been loaded
        fingerprint = file.Fingerprint.from_path(tabular_file.path.path)
        logger.info("File '{}' has fingerprint: {}", tabular_file.path, fingerprint)
        #rows = load_dlms.select(lambda r: r['name'] == table_cfg.name) # TODO implement predicates as expressions or as functions ("row predicates")
        rows = list(db.execute_query(
            'select size, mtime, header, loaded '
            'from {} where name = ?'.format(load_dlms_name),
            (tabular_file.name,)))
        logger.info("DB has loaded '{}': {}", tabular_file.name, rows)
        if len(rows) > 1:
            raise Exception('Multiple tables found with name: {}'
                            .format(tabular_file.name))
        elif len(rows) == 1:
            row = rows[0]
            if (row[0] == fingerprint.size and
                    row[1] == str(fingerprint.mtime_ns) and
                    row[2] == str(data_header) and
                    row[3] == 1):
                # Patch in header due to SQLite implementation not setting header (FIXME)
                table = db.table(table_cfg.name)
                table._header = data_header
                # Skip this table as it has already been loaded
                logger.info("Skipping '{}': Already loaded", table_cfg.name)
                continue
        else:
            # Create entry to track loading of this table
            crsr = db.execute_query('insert into {} (name) values (?)'.format(load_dlms_name), (tabular_file.name,))
            crsr.connection.commit()
        # Update row for this table
        crsr = db.execute_query('update {} set size = ?, mtime = ?, header = ?, loaded = ? where name = ?'.format(load_dlms_name), (fingerprint.size, str(fingerprint.mtime_ns), str(data_header), 0, tabular_file.name))
        crsr.connection.commit()
        # Read delimited file logging all errors
        reader = tabular_file.reader(
            is_missing,
            lambda e: reader_logger.error("'{}': {}", table_file, e))
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
        # Record that the table successfully loaded
        crsr = db.execute_query('update {} set loaded = ? where name = ?'.format(load_dlms_name), (1, tabular_file.name))
        crsr.connection.commit()
        logger.info(
            "Loaded {} records from '{}' into '{}'",
            table.count_rows(), table_file.path, table.name)
        logger.info("Done loading '{}'", table_cfg.name)

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

    # Guess at fields that make up an event: first integer field is
    # patient ID, first float field is time / age, first str field is
    # event ID.  Or require and use names?
    # *** No, just assume based on data treatment ("treat as") and
    # position or names ("use") as given in config
    # TODO convert config to interpret "event(patient_id, age, dx_code)" and the like

    # Look up tables and organize by table data treatment
    db_tables = {}
    treats2tables = collections.defaultdict(list)
    tables2treats = {}
    for table_cfg in config_obj.tables:
        table = db.table(table_cfg.name)
        db_tables[table.name] = table
        treats2tables[table_cfg.treat_as].append(table.name)
        tables2treats[table.name] = table_cfg.treat_as
    tables = dict(db_tables)
    # Sort table names
    for table_names in treats2tables.values():
        table_names.sort()

    # Load features if defined
    if 'features' in treats2tables and treats2tables['features']:
        feats_table_name = treats2tables['features'][0]
        logger.info('Loading features from: {}', feats_table_name)
        feats = features.load(tables[feats_table_name])
        if not feats:
            logger.error('No features defined in: {}', feats_table_name)
            return
        # Make sure features are numeric
        if config_obj.numeric_features:
            feats = features.encode_categorical_features(feats, tables)
    # Otherwise detect features
    else:
        fact_tables_names = sorted(treats2tables['facts'])
        event_tables_names = sorted(treats2tables['events'])
        logger.info(
            'No features table specified: '
            'Detecting features in tables: {}',
            fact_tables_names + event_tables_names)
        feats = features.detect(
            [tables[name] for name in fact_tables_names],
            [tables[name] for name in event_tables_names],
            fact_key_field=_pt_id_idx,
            event_type_field=_evtype_idx,
            positive_label=config_obj.positive_label,
            numeric_features=config_obj.numeric_features,
            features_are_counts=config_obj.features_are_counts,
            )
        if not feats:
            logger.error('No features detected in: {}', feats_tables_names)
            return
    # Write features
    feats_file = base_directory.join(generated_features_filename)
    logger.info('Writing features to: {}', feats_file)
    features.save(feats, feats_file)

    # Make index of features
    feats_key2idx = {}
    for idx, feat in enumerate(feats):
        key = feat.key
        assert key not in feats_key2idx
        feats_key2idx[key] = idx

    # TODO end: feature generation

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
        if treatment in filters:
            tables[name] = table.select(filters[treatment])

    # Generate and print all feature vectors
    for feature_vector in barnapy.general.track_iterator(
            generate_feature_vectors(
                tables, treats2tables, feats, feats_key2idx),
            lambda count: logger.info(
                'Generated feature vectors: {}', count),
            track_every=100,
            track_init=True,
            track_end=True):
        label = feature_vector.get(2, 0) # FIXME look up label feature; don't assume numeric values
        print_as_svmlight(label, feature_vector)

    # Cleanup # TODO write and use context manager
    db.close()


def generate_feature_vectors(
        tables,
        treatments2tables,
        feats,
        feats_key2idx,
        pt_id_idx=_pt_id_idx, # TODO ensure used
        time_idx=_time_idx, # TODO ensure used
):
    # Data tables
    data_table_names = (treatments2tables['facts']
                        + treatments2tables['events']
                        + treatments2tables['examples'])
    headers = {name: table.header for (name, table) in tables.items()}
    # Merge-collect records based on patient ID
    for record_collection in relational.MergeCollect(
            *(tables[n] for n in data_table_names),
            key=pt_id_idx):
        # Interpret the records
        facts = interpret_facts(
            record_collection, treatments2tables['facts'], headers)
        events = interpret_events(
            record_collection, treatments2tables['events'], headers)
        examples = interpret_examples(
            record_collection, treatments2tables['examples'])
        # Generate feature vectors from this collection of records
        yield from features.generate_feature_vectors2(
            record_collection.groupby_key,
            facts,
            events,
            examples,
            feats,
            feats_key2idx)


if __name__ == '__main__':
    main()
