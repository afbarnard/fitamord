Class Design
============


Overview
--------

This is a design sketch of classes and functions for general-purpose
data processing.  There is also some application-specific pieces.

The design thinking is based on tables in databases.  Such tables are a
collection of records (rows) with the same fields.

NamedTuples work well if you know ahead of time what records your
application will use.  However, for general data processing software
that is not the case, and there is a need for general objects.

Provenance of records from files will be tracked by adding an extra
field (line_n) to the records and setting the source in the header to
the filename.


Field
-----

A field is a (name, type) pair.  A class for this is probably not
necessary but having an actual type and methods may prove useful.

* construct from (name, type) pair
* construct from (name, value) pair using type of value
* construct from name alone, assuming object type
* isinstance(obj)
* name()
* type()


Header
------

A description of a collection of fields with names and types.  A table
header.

Call RecordType because it's the type of a record?  Header is pretty
table-specific.  What about metaclasses and actually constructing
classes for records?  That would be like dynamically-defined namedtuples
or general heterogeneous collections.  Probably best to leave
heterogeneous collections until later.

* __len__()
* fields()::Iterable
* names()::Iterable
* types()::Iterable
* field_at(index)::Field
* name_at(index)::str
* type_at(index)::type
* type_of(name)::type
* isinstance(record)::bool


Record
------

A fixed-length heterogeneous collection (like a struct) for storing
information like database rows.

* construct from iterable of values and iterable of names (infer types)
* construct from dict (infer types)
* construct from iterable of (name, value) pairs
* construct from iterable of value and Header
* header::Header
* record::Indexable


RecordStream
------------

An iterable of records with a header that describes the fields of the
records.  For these purposes a record can be any iterable of values, not
necessarily a Record.

* name()::str # name of relation / table / stream / etc.
* provenance()::str # filename, name of other table, name of generator function, etc.
* header()::Header
* __iter__()


Table <: RecordStream
---------------------

Abstract notion of a table.  A Table can be considered a RecordStream
with much more functionality.

* n_cols()::int
* n_rows()::int
* __len__()
* __iter__()
* row(index)::Record
* rows(indices::Union(Iterable(int), Range)::RecordStream
* col(which::Union(int, str))::RecordStream
* cols(which::Union(Iterable(int), Iterable(str), Range)::RecordStream
* add(row::Record)
* add_all(rows::Iterable(Record))
* sort_by(name, reverse=False)::RecordStream
* sql(sql::str)::RecordStream
* __getitem__(key)::RecordStream # treat like a matrix


SqliteTable <: Table
--------------------

A proxy object for a table in a Sqlite DB.

* load(records::RecordStream)


DelimitedTextFile <: RecordStream
---------------------------------

Treat a delimited text file as a table.  Records from files will include
a special field (line_n) containing the line number of the record.  Use
csv module for parsing files.

* filename
* format


DelimitedTextFile.Format
------------------------

The format of a delimited text file.

* delimiter::str
* quoting_style::QuotingStyle
* quote_char::str
* escape_style::EscapeStyle
* escape_char::str


Enum QuotingStyle
-----------------

* none
* always
* as_needed


Enum EscapeStyle
----------------

* char
* doubling
