"""SQLite DB engine / backend"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import copy
import sqlite3
import sys

from barnapy import logging

from .. import db
from .. import general
from .. import records as recs


def placeholders_for_params(n_params):
    return '(?' + (', ?' * (n_params - 1)) + ')'

def gen_fetchmany(cursor):
    rows = cursor.fetchmany()
    while rows:
        yield from rows
        rows = cursor.fetchmany()

python2sqlite_types = {
    bytes: 'blob',
    float: 'real',
    int: 'integer',
    # SQLite allows a column definition without a specific type
    object: '',
    str: 'text',
    }


class SqliteDb(db.Database):
    """Represents a SQLite database and provides schema-level operations"""

    def __init__(self, filename=None): # TODO max mem, max threads, other options
        self._filename = (filename
                          if filename is not None
                          else ':memory:')
        repr_id = repr(self) + '@' + hex(id(self))
        self._logger = logging.getLogger(repr_id)
        self._logger.info(
            'Opening connection to Sqlite DB: {}'
            .format(self._filename))
        # TODO open and maintain a connection at the object level but what about committing, closing the connection, etc.?
        self._connection = sqlite3.connect(self._filename)
        self._logger.info('Connected')
        self._tables = {} # References to tables are circular

    def close(self):
        """Close and invalidate the database connection"""
        # Bail if already closed or not set up
        if self._connection is None:
            return
        self._logger.info('Closing DB connection')
        # Clear circular references to tables
        if self._tables:
            for table in self._tables.values():
                table.disconnect()
            self._tables.clear()
        # Close the DB connection
        try:
            self._connection.close()
        except Exception as e:
            self._logger.exception(
                'Failed to close DB connection: {}'.format(e))
        else:
            self._logger.info('DB connection closed')
        finally:
            # Release reference no matter what
            self._connection = None

    __del__ = close

    @property
    def name(self):
        return self._filename

    def __repr__(self):
        return '{}({!r})'.format(
            general.fq_typename(self), self._filename)

    def execute_query(self, query, parameters=None):
        self._logger.info(
            'Executing query: {}; parameters: {}'
            .format(query, parameters))
        cursor = self._connection.cursor()
        if parameters is None:
            cursor.execute(query)
        else:
            cursor.execute(query, parameters)
        return cursor

    def execute_many(self, query, parameters):
        self._logger.info(
            'Executing query: {}; parameters: {}'
            .format(query, parameters))
        cursor = self._connection.cursor()
        cursor.executemany(query, parameters)
        return cursor

    def _catalog_table_name(self, namespace):
        # Construct and check the catalog table name
        catalog_table_name = namespace + '.sqlite_master'
        return self.interpret_sql_name(catalog_table_name)

    def interpret_sql_name(self, name):
        dotted_name = super().interpret_sql_name(name)
        # Check that the dotted name has no more than 2 components
        if len(dotted_name) > 2:
            raise db.DbSyntaxError(
                'Too many name components: {}'.format(name))
        return dotted_name

    def _process_name(self, name):
        # Parse and check name
        dotted_name = self.interpret_sql_name(name)
        # Split à la dirname, basename
        namespace, obj_name = dotted_name.rest_last()
        # Default to `main` namespace
        if namespace is None:
            namespace = 'main'
        # Return it all
        return dotted_name, namespace, obj_name

    def namespaces(self):
        yield from (db_name
                    for db_id, db_name, filename
                    in gen_fetchmany(
                        self.execute_query('pragma database_list')))

    _list_objects_sql = 'select name, type, sql from {}'
    _list_objects_with_types_sql = (
        'select name, type, sql from {} where type in {}')

    def objects(self, types=None, namespace=None):
        """Return an iterable of DB objects as (name, type, sql) tuples"""
        # Default to `main` namespace
        if namespace is None:
            namespace = 'main'
        catalog_table_name = self._catalog_table_name(namespace)
        # Handle no type, a single type, or an iterable of types
        if types is None:
            parameters = None
            query = self._list_objects_sql.format(
                db.quote_name(catalog_table_name.name))
        elif hasattr(types, '__iter__') and not isinstance(types, str):
            parameters = tuple(
                db.DbObjectType.convert(o).name for o in types)
            query = self._list_objects_with_types_sql.format(
                db.quote_name(catalog_table_name.name),
                placeholders_for_params(len(parameters)))
        else:
            parameters = (db.DbObjectType.convert(types).name,)
            query = self._list_objects_with_types_sql.format(
                db.quote_name(catalog_table_name.name),
                placeholders_for_params(len(parameters)))
        # Generate the objects as (name, type, sql) tuples
        yield from gen_fetchmany(self.execute_query(query, parameters))

    _schema_sql = 'select sql from {} where name = ?'

    def schema(self, name):
        dotted_name, namespace, obj_name = self._process_name(name)
        catalog_table_name = self._catalog_table_name(namespace)
        query = self._schema_sql.format(
            db.quote_name(catalog_table_name.name))
        schemas = list(
            gen_fetchmany(self.execute_query(query, (obj_name,))))
        if len(schemas) == 0:
            raise db.DbError('Object not found: {}'.format(name))
        elif len(schemas) > 1:
            raise db.DbError(
                'Multiple schemas for object: {}'.format(name))
        else:
            return schemas[0][0]

    _create_table_sql = 'create table {} ({})'

    def create_table(self, name, header):
        dotted_name, namespace, obj_name = self._process_name(name)
        # Create the fields definition from the header
        fields = []
        for field in header:
            field_def = db.quote_name(field.name)
            field_type = self.translate_type(field.type)
            if field_type:
                field_def += ' ' + field_type
            fields.append(field_def)
        fields_def = ', '.join(fields)
        # Build and run the query
        query = self._create_table_sql.format(
            db.quote_name(name), fields_def)
        rows = list(gen_fetchmany(self.execute_query(query)))
        if rows:
            raise db.DbError('Create returned rows: {}'.format(rows))
        # Create proxy object
        table = Table(self, dotted_name.name, header)
        self._tables[dotted_name] = table
        return table

    _drop_table_sql = 'drop table {}'

    def drop_table(self, name):
        dotted_name, namespace, obj_name = self._process_name(name)
        if dotted_name in self._tables:
            self._tables[dotted_name].disconnect()
            del self._tables[dotted_name]
        query = self._drop_table_sql.format(db.quote_name(name))
        rows = list(gen_fetchmany(self.execute_query(query)))
        if rows:
            raise db.DbError('Drop table returned rows: {}'.format(rows))

    def table(self, name):
        dotted_name, namespace, obj_name = self._process_name(name)
        # Return the table from the cache if it exists
        if dotted_name in self._tables:
            return self._tables[dotted_name]
        # Get schema
        schema_str = self.schema(name)
        # Parse schema
        # Create header
        header = None # FIXME actually parse header
        # Create proxy object
        table = Table(self, dotted_name.name, header)
        self._tables[dotted_name] = table
        return table

    def translate_type(self, python_type):
        # Use a text representation if no other specific type
        return python2sqlite_types.get(python_type, 'text')

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()


class Table(db.Table):

    # Construction

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._project_cols = None
        self._filter_predicate = None
        self._order_by_cols = None
        self._transformation = None

    # Reading

    _count_rows_sql = 'select count(*) from {}'

    def count_rows(self):
        self.assert_connected()
        query = self._count_rows_sql.format(db.quote_name(self.name))
        rows = list(gen_fetchmany(self._db.execute_query(query)))
        if len(rows) != 1:
            raise db.DbError('Not a single row: {}'.format(rows))
        n_rows = rows[0][0]
        self._n_rows = n_rows
        return n_rows

    _select_rows_sql = 'select {} from {}'

    def _record_iterator(self): # FIXME semantically incorrect because breaks nesting, but good enough for now
        self.assert_connected()
        # Apply projection
        cols = (', '.join(db.quote_name(col)
                          for col in self._project_cols)
                if self._project_cols
                else '*')
        query = self._select_rows_sql.format(
            cols, db.quote_name(self.name))
        # Apply sorting
        if self._order_by_cols:
            cols = ', '.join(
                db.quote_name(name) + ' ' + order
                for (name, order) in self._order_by_cols)
            query += ' order by ' + cols
        rows = gen_fetchmany(self._db.execute_query(query))
        # Apply filtering
        if self._filter_predicate is not None:
            rows = filter(self._filter_predicate, rows)
        # Apply transformation
        if self._transformation is not None:
            rows = map(self._transformation, rows)
        return rows

    # Queries

    def project(self, *cols):
        # Interpret and validate columns
        columns = [self._interpret_column(col) for col in cols]
        # Create new table with desired columns
        table = copy.copy(self)
        table._header = self.header.project(*cols)
        table._project_cols = columns
        return table

    def select(self, predicate):
        self._filter_predicate = predicate
        return self

    def order_by(self, *cols):
        self._order_by_cols = [
            self._interpret_order_by_column(col) for col in cols]
        return self

    def transform(self, header, transformation): # FIXME semantically incorrect because breaks nesting, but good enough for now
        # Create a new table for the transformed records
        table = copy.copy(self)
        table._header = header
        table._transformation = transformation
        return table

    # Writing

    _add_all_sql = 'insert into {} {} values {}'

    def add_all(self, records):
        self.assert_connected()
        header = (records.header
                  if isinstance(records, recs.RecordStream)
                  else self.header)
        # Assemble query
        col_names = '(' + ', '.join(db.quote_name(n)
                                    for n in header.names()) + ')'
        param_placeholder = placeholders_for_params(self.n_cols)
        query = self._add_all_sql.format(
            db.quote_name(self.name), col_names, param_placeholder)
        # Run query
        cursor = self._db.execute_many(query, records)
        rows = list(gen_fetchmany(cursor))
        if rows:
            self._db.rollback()
            raise db.DbError('Insert returned rows: {}'.format(rows))
        self._db.commit()
        if cursor.rowcount > 0:
            self._n_rows += cursor.rowcount

    _clear_sql = 'delete from {}'

    def clear(self):
        self.assert_connected()
        query = self._clear_sql.format(db.quote_name(self.name))
        cursor = self._db.execute_query(query)
        rows = list(gen_fetchmany(cursor))
        if rows:
            self._db.rollback()
            raise db.DbError('Delete returned rows: {}'.format(rows))
        self._db.commit()
        if cursor.rowcount > 0:
            self._n_rows -= cursor.rowcount

    # Bookkeeping

    def is_connected(self):
        return self._db is not None

    def assert_connected(self):
        if not self.is_connected():
            raise db.DbError(
                'Operation not possible because the '
                'table is not connected to a database.')

    def disconnect(self):
        self._db = None

    # Remove circular references
    __del__ = disconnect
