"""Configuration objects"""
# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import collections
from collections import OrderedDict as ordict


# Configuration as state needed to perform a specific execution.
# Command line arguments mapped into configuration.  Configuration files
# mapped into configuration.  Configuration saved to configuration
# files.



class Configuration:

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
