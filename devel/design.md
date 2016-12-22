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
  * optional configuration
  * generated file prefix or names of individual output files

* outputs
  * data in feature vector form for ML methods
  * support various output formats
  * map for translating feature numbers to descriptions
  * configuration for reuse

* operational modes {gen, mk, make}?
  * only generate feature set (mk_features)
  * only guess at and generate configuration for a set of files
    (make_config)
    * no files results in generic configuration for bootstrapping
  * only load data into Sqlite (load)
  * generate feature vector data (gen_data)
  * merge_collect passes groups off to custom code (collect)
  * generate random data for testing (mk_randdata)

* CLI
  * data syntax: --data=<table_name>=<filename>
  * delimiter syntax: --delimiter=<delimiter> (global) or
    --delimiter=<filename>=<delimiter> for a particular file

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

* YAML and config are separate, config is interpreted version of YAML
* constructor objects to help transform YAML into config


File Formats
------------

I have decided that all configuration will be done in YAML.  This is for
a few reasons: JSON is not intended for human use due to quoting
verbosity and lack of support for comments, writing configuration
directly in Python is not portable to other languages, and INI format
does not support any type of nesting.  Unfortunately, Python does not
come with a YAML parser (unlike INI and JSON), and so this decision
introduces an external dependency on PyYaml.


What is a record transformation, anyway?
----------------------------------------

Most generally, a record transformation is a function that takes a
record as input and produces a record as output.  The input header is
assumed to be known and the transformation defines the output header.

A transformation is projection --- add, delete, or rearrange fields ---
plus (optionally) element-wise transformations.  Technically, each
output field is a function of zero or more input fields, and the
projection defines their order.  Thus, a record transformation
corresponds to the head of a SQL select statement wherein the output
columns are defined.

But how does one construct record transformations in a convenient and
user-friendly manner?  One could construct it like a collection with an
`add` method that adds each successive column along with its
transformation function.  A sequence of element-wise transformations
could also be specified with each element-wise transformation having
varying complexity.  The most general (and user-unfriendly case) is
providing a function that maps records to records.

Element-wise transformations as tuples:
* (name,): Copy field to output
* (name, name): Rename field in output
* (name, type, func(field)): Apply unary function to field
* (name, type, func(field), name): Apply field function with rename
* (name, type, func(header, record)): Apply record function with rename

After thinking about how to specify element-wise transformations, it
seems appropriate to have a type, perhaps FieldConstructor, whose
constructor can deal with the complexity and provide a uniform interface
to the record transformation.  For convenience, RecordTransformation
could have a method with keyword arguments corresponding to the field
transformation constructor.

Practical implementation considerations:
* `row_num` is always accessible as a virtual field (0- or 1-based?)
* Treat delimited text as columns of `str`
* Support headers with only a partial set of names as not all names are
  necessarily needed and fields are always accessible by index
* Handle accessess of nonexistent columns by configuring an error or
  default value.  But where to configure?  Record, Header,
  Transformation?  Record or Header could always have a method similar
  to dict.get(key, default), but calling that would still have to be
  configured somewhere.
* The result of any record transformation needs to be type-checked
  against the output header, including for nullability and custom
  constraints [future features].

After consulting Wiktionary, the terminology will be "transformation"
not "transform" because "transform" as a noun has a specific
mathematical meaning.  (My uncertainty over the terminology is probably
due to exposure to the mathematical use.)


Loading a File into SQLite
--------------------------

* delete and define table, needs DB, table definition / schema (header)
* read delimited file, needs filename and format
* transform delimited file, needs transformation (or "parse")
* loop over transformed records and insert each into table
* need object to represent DB ("backend") with API (a la Spark?) for
  querying catalog, deleting table, creating table, inserting, reading,
  and sorting records


Copyright (c) 2016 Aubrey Barnard.  This is free software released under
the MIT License.  See `LICENSE.txt` for details.
