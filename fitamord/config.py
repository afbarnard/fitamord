"""Configuration objects"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import collections
import datetime

import yaml
from barnapy import files
from barnapy import logging
from barnapy import parse
from barnapy import unixutils

from . import delimited
from . import records


# Configuration as state needed to perform a specific execution.
# Command line arguments mapped into configuration.  Configuration files
# mapped into configuration.  Configuration saved to configuration
# files.



class Configuration:

    def __init__(self, *args, **kwargs):
        pass


default = Configuration([
    # arg-name, env-key, default, type, constructor, help, validator
    ('config', ['fitamord.yaml']),
    ('log', 'stderr'),
    ('log-level', 'info'),
    ('help', 'no'),
    ('version', 'no'),
    ('dump-config', 'no'),
    ('data-pattern', '*.csv'),
    ('delimiter', 'null'),
    ('features', 'fitamord_features.csv'),
    ('sqlitedb', 'fitamord_db.sqlite'),
    ('labels', 'labels'),
    ('bad-records', 'collect'),
    ('mode', 'gen-data'),
    ('data', []),
    ('format', []),
    ('files', []),
    ('tables', []),
    ('extension', []),
    ('engine.threads', 'auto'),
    ('engine.max_mem', 'auto'),
    ('',),
    ])

# access values via dot notation (?)

# default environment defines all keys (only load defined keys? what about (re)writing configuration?)
# nope, only support writing configuration if asked. use default key order

# flat or nested key structure? tree or just dotted keys? need ways to both list all keys and get a subtree

# forget about provenance for now -> great if making some package at some point

# I still like the idea of a declarative specification with defaults to which environment objects are added.  This supports provenance.
# Would validators that errored while parsing and knew their location remove need for provenance?
# Strategies: load everything then interpret; load and interpret as you go (parsing)

# Environment as list of environments searched sequentially or as a single environment where values are aggregated and searched sequentially?

# How differentiate between declared structure and structured objects found in Yaml?

# Ideal of just reading in some config files and the command line arguments and having a magical environment with types, etc.

# this is all too big for right now; need simplest thing that could work in order to get loading of data running; no configuration at all and just assume parsing and loading of all CSV files?

# work bottom up in order to know what needs to be defined / configured (?)


# ===== Forget about all of above for now =====


# Make yaml load with ordered dicts because it is important to preserve
# the order of fields / columns.
yaml.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    lambda loader, node: collections.OrderedDict(
        loader.construct_pairs(node)),
    )


# Write out ordered dicts as regular dicts
yaml.add_representer(
    collections.OrderedDict,
    lambda dumper, data: yaml.nodes.MappingNode(
        'tag:yaml.org,2002:map',
        [(dumper.represent_data(key), dumper.represent_data(val))
         for (key, val) in data.items()]),
    )


def load(file):
    file = files.new(file)
    with file.open('rt') as yaml_file:
        yaml_tree = yaml.load(yaml_file)
    return FitamordConfig(yaml_tree, file.path)


def save(config_obj, file, insert_defaults=False):
    if isinstance(config_obj, FitamordConfig):
        config_obj = config_obj.as_yaml_object(insert_defaults)
    file = files.new(file)
    with file.open('wt') as yaml_file:
        yaml.dump(config_obj,
                  yaml_file,
                  version=(1, 2),
                  explicit_start=True,
                  explicit_end=True,
                  default_flow_style=False,
                  )


def detect(directory, extensions): # TODO make reusable per-file config detection
    logger = logging.getLogger(__name__)
    # List the directory
    directory = files.new(directory)
    dirnames, filenames = unixutils.ls(directory.path)
    filenames = sorted(filenames)
    # Search for tabular files
    patterns = ['*' + e for e in extensions]
    tabular_files = unixutils.glob(
        filenames, *patterns, case_sensitive=False)
    # Detect formats and headers of tabular files
    tables = collections.OrderedDict()
    for filename in tabular_files:
        file = directory.join(filename)
        # Determine file format and table schema
        tabular_file = delimited.File(file.path)
        tabular_file.init_from_file()
        if tabular_file.format is None or tabular_file.header is None:
            logger.error(
                'Format or header detection failed: {}', tabular_file)
            continue
        # Build configuration for table
        table_cfg = collections.OrderedDict()
        table_cfg['file'] = filename
        table_cfg['format'] = (
            collections.OrderedDict(
                tabular_file.format.as_yaml_object())
            if tabular_file.format
            else None)
        table_cfg['columns'] = (
            collections.OrderedDict(
                tabular_file.header.as_yaml_object())
            if tabular_file.header
            else None)
        table_cfg['id'] = None
        table_cfg['use'] = None
        table_cfg['treat as'] = None
        tables[file.stem] = table_cfg
    # Build configuration
    cfg = collections.OrderedDict()
    cfg['tables'] = tables
    return FitamordConfig(cfg, '<config detection>')


def build_list(obj, err_msg=None, *contexts):
    if obj is None:
        return None
    elif isinstance(obj, list):
        return obj
    elif isinstance(obj, str):
        return [s.strip() for s in obj.split(',')]
    elif isinstance(obj, (int, float)):
        return [obj]
    else:
        if err_msg is None:
            raise ConfigError('Not an item or list', obj, *contexts)
        else:
            raise ConfigError(
                'Not an item or list', obj, err_msg, *contexts)


class ConfigError(Exception):

    def __init__(self, message, value=None, *contexts):
        components = [str(c) for c in reversed(contexts)]
        components.append(str(message))
        if value is not None:
            components.append(repr(value))
        msg = ': '.join(components)
        super().__init__(msg)


class FitamordConfig:

    default_is_missing = ['', '?', 'na', 'nil', 'none', 'null']

    def __init__(self, dict_, *contexts):
        if not dict_:
            raise ConfigError('Empty configuration', dict_, *contexts)
        elif not isinstance(dict_, dict):
            raise ConfigError(
                'Configuration not a dictionary', dict_, *contexts)
        self._dict = dict_
        self._is_missing = (
            self._build_is_missing(
                dict_['is_missing'], 'is_missing', *contexts)
            if 'is_missing' in dict_
            else None)
        self._is_positive = (
            self._build_is_positive(
                dict_['is_positive'], 'is_positive', *contexts)
            if 'is_positive' in dict_
            else None)
        self._tables = (
            self._build_tables(dict_['tables'], 'tables', *contexts)
            if 'tables' in dict_
            else None)
        self._contexts = contexts

    def _build_is_missing(self, obj, *contexts):
        if obj is None:
            return []
        elif obj == 'default':
            return self.default_is_missing
        elif isinstance(obj, str):
            return [obj]
        elif isinstance(obj, list):
            return [(str(o) if o is not None else None) for o in obj]
        else:
            raise ConfigError(
                'Not a list of strings that indicate missing',
                obj, *contexts)

    def _build_is_positive(self, obj, *contexts):
        if obj is None:
            return None
        elif isinstance(obj, str):
            return [obj]
        elif isinstance(obj, list):
            return obj
        else:
            raise ConfigError(
                'Not a list of positive labels', obj, *contexts)

    def _build_tables(self, dict_, *contexts):
        return tuple(TabularFileConfig(k, v, k, *contexts)
                     for (k, v) in dict_.items())

    @property
    def is_missing(self):
        if self._is_missing is None:
            return self.default_is_missing
        return self._is_missing

    @property
    def is_positive(self):
        return self._is_positive

    @property
    def tables(self):
        return self._tables

    @property
    def numeric_features(self): # TODO allow to be configured
        return True

    def as_dict(self):
        return self._dict

    def as_yaml_object(self, insert_defaults=False):
        if insert_defaults:
            dict_ = dict(self._dict)
            if 'is_missing' not in dict_:
                dict_['is_missing'] = self.default_is_missing
            if 'tables' not in dict_:
                dict_['tables'] = None
            return dict_
        else:
            return self._dict

    def validate_data_treatments(self):
        treatments = set(t.treat_as for t in self.tables)
        if 'facts' not in treatments and 'events' not in treatments:
            raise ConfigError(
                'No tables are treated as facts or events',
                None, *self._contexts)
        if 'examples' not in treatments:
            raise ConfigError(
                'No tables are treated as examples',
                None, *self._contexts)


_names2types = {
    'bool': bool,
    'boolean': bool,
    'char': str,
    'date': datetime.date,
    'datetime': datetime.datetime,
    'double': float,
    'float': float,
    'int': int,
    'integer': int,
    'real': float,
    'str': str,
    'string': str,
    'time': datetime.time,
    'varchar': str,
    }


class TabularFileConfig:

    treatments = {'facts', 'features', 'events', 'examples'}

    def __init__(self, name, dict_=None, *contexts):
        self._name = name
        self._dict = dict_ if isinstance(dict_, dict) else {}
        self._filename = self._dict.get('file', None)
        self._format = self._build_format(
            self._dict.get('format', None), 'format', *contexts)
        self._cols = self._build_column_defs(
            self._dict.get('columns', None), 'columns', *contexts)
        self._use_cols = self._build_column_refs(
            self._dict.get('use', None), 'use', *contexts)
        self._id_cols = self._build_column_refs(
            self._dict.get('id', None), 'id', *contexts)
        treat_as = self._dict.get('treat as', None)
        if treat_as is not None and treat_as not in self.treatments:
            raise ConfigError(
                'Unrecognized treatment', treat_as,
                'treat as', *contexts)
        self._treat_as = treat_as
        self._header = None

    def _build_format(self, fmt_def, *contexts):
        if fmt_def is None or fmt_def == 'detect':
            return None
        elif isinstance(fmt_def, str):
            # Look up format by name # TODO
            raise ConfigError(
                'Format lookup by name not implemented',
                fmt_def, *contexts)
        elif isinstance(fmt_def, dict):
            try:
                return delimited.Format(**fmt_def)
            except TypeError as e:
                raise ConfigError(
                    'Bad format definition', fmt_def, *contexts) from e
        else:
            raise ConfigError(
                'Unrecognized format definition', fmt_def, *contexts)

    def _build_column_defs(self, cols, *contexts):
        if cols is None:
            return None
        elif isinstance(cols, int):
            return tuple((i, None, None) for i in range(cols))
        elif isinstance(cols, dict):
            col_defs = []
            col_idx = 0
            for key, val in cols.items():
                col_name = None
                col_type = None
                if isinstance(key, int):
                    col_idx = key - 1
                    words = build_list(
                        val, 'Bad column definition', key, *contexts)
                    if words is not None:
                        if len(words) >= 1:
                            col_name = words[0]
                        if len(words) >= 2:
                            col_type = words[1]
                elif isinstance(key, str):
                    col_name = key
                    col_type = val
                else:
                    raise ConfigError(
                        'Not a column reference', key, *contexts)
                # Validate and interpret type name
                if col_type is not None:
                    if not isinstance(col_type, str):
                        raise ConfigError(
                            'Not a type name', col_type, key, *contexts)
                    if col_type not in _names2types:
                        raise ConfigError(
                            'Unrecognized type', col_type, key, *contexts)
                col_type = _names2types.get(col_type, None)
                col_defs.append((col_idx, col_name, col_type))
                col_idx += 1
            return tuple(col_defs)
        elif isinstance(cols, list):
            col_defs = []
            col_idx = 0
            for col in cols:
                if isinstance(col, int):
                    col_idx = col - 1
                    col_defs.append((col_idx, None, None))
                elif isinstance(col, str):
                    col_defs.append((None, col, None))
                else:
                    raise ConfigError(
                        'Not a column reference', col, *contexts)
                col_idx += 1
            return tuple(col_defs)
        else:
            raise ConfigError('Unrecognized definition', cols, *contexts)

    def _build_column_refs(self, cols, *contexts):
        refs = None
        if cols is None:
            return None
        elif isinstance(cols, int):
            refs = list(range(1, cols + 1))
        elif isinstance(cols, str):
            refs = [parse.int(x, x)
                    for x in build_list(cols, *contexts)]
        elif isinstance(cols, list):
            refs = cols
        else:
            raise ConfigError(
                'Unrecognized definition', cols, *contexts)
        # Interpret and validate columns
        columns = self.columns
        if columns is None:
            columns = ()
        col_names = set(col[1] for col in columns if col[1] is not None)
        for idx, ref in enumerate(refs):
            if isinstance(ref, int):
                # Assume index is valid.  Just convert it from 1-based
                # to 0-based.
                refs[idx] -= 1
            elif isinstance(ref, str):
                if ref not in col_names:
                    raise ConfigError(
                        'Column not found', ref, *contexts)
            else:
                raise ConfigError(
                    'Not a column reference', ref, *contexts)
        return tuple(refs)

    @property
    def name(self):
        return self._name

    @property
    def filename(self):
        return (self._filename
                if self._filename is not None
                else self.name + '.csv')

    @property
    def format(self):
        return self._format

    @property
    def columns(self):
        return self._cols

    @property
    def header(self):
        if self._header is None and self._cols is not None:
            self._header = records.Header(
                *((name, typ) for (idx, name, typ) in self.columns))
        return self._header

    @property
    def id_columns(self):
        return self._id_cols if self._id_cols is not None else (0,)

    @property
    def use_columns(self):
        return self._use_cols

    @property
    def treat_as(self):
        return (self._treat_as
                if self._treat_as is not None
                else 'events')
