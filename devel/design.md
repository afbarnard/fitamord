Design
======

* inputs
  * a collection ("database") of delimited text files
  * how each file corresponds to a table
    * format
      * compression
      * file type (only csv-like delimited text for now)
      * comments
      * whitespace
      * delimiter
      * quoting
    * column names
    * column data types
    * projection
    * custom processing
    * table can have multiple files
  * how each table corresponds to a fact (a la Prolog)
    * type of fact
    * projection
    * custom processing (e.g. computed fields)
  * table defining study (subject_id, label, [start, end])
  * list of features
  * custom features (and other code) implemented in Python
  * optional JSON configuration
  * generated file prefix or names of individual output files

* outputs
  * data in feature vector form for ML methods
  * support various output formats
  * map for translating feature numbers to descriptions (JSON?)
  * JSON configuration for reuse

* operational modes {gen, mk, make}?
  * only generate feature set (mk_features)
  * only guess at and generate JSON configuration for a set of files
    (make_config)
    * no files results in generic JSON config for bootstrapping
  * generate feature vector data (gen_data)
  * merge_collect passes groups off to custom code (collect)

* CLI
  * data syntax: --data=<table_name>=<filename>
  * delimiter syntax: --delimiter=<delimiter> (global) or
    --delimiter=<table_name>=<delimiter> for a particular table

* engine
  * read data with csv module
  * implement data storage and manipulation in terms of Sqlite
    * per http://sqlite.org/whentouse.html multiple concurrent reads
      should be performant (try out to see how this works from Python)
    * also: PRAGMA cache_size=-kibibytes; memory per database file
    * also: PRAGMA threads=n; extra worker threads per query
  * assemble collections of facts using merge_collect

* desiderata
  * re-runnable, including only on new data
  * proper logging
  * customizable error handling (log, exception, function)
  * collect errored / discarded records for inspection, fixing, and
    reprocessing
  * each feature vector should be output with a subject ID and a study /
    label ID to support debugging


Copyright (c) 2016 Aubrey Barnard.  This is free software released under
the MIT License.  See `LICENSE.txt` for details.
