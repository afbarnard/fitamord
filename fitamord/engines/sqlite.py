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
                catalog_table_name.name)
        elif hasattr(types, '__iter__') and not isinstance(types, str):
            parameters = tuple(
                db.DbObjectType.convert(o).name for o in types)
            query = self._list_objects_with_types_sql.format(
                catalog_table_name.name,
                placeholders_for_params(len(parameters)))
        else:
            parameters = (db.DbObjectType.convert(types).name,)
            query = self._list_objects_with_types_sql.format(
                catalog_table_name.name,
                placeholders_for_params(len(parameters)))
        # Generate the objects as (name, type, sql) tuples
        yield from gen_fetchmany(self.execute_query(query, parameters))

    _schema_sql = 'select sql from {} where name = ?'

    def schema(self, name):
        dotted_name, namespace, obj_name = self._process_name(name)
        catalog_table_name = self._catalog_table_name(namespace)
        query = self._schema_sql.format(catalog_table_name.name)
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
            field_def = '{} {}'.format(field.name, field.type.__name__)
            fields.append(field_def)
        fields_def = ', '.join(fields)
        # Build and run the query
        query = self._create_table_sql.format(name, fields_def)
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
        query = self._drop_table_sql.format(name)
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


class Table(db.Table):

    # Construction

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._order_by_cols = None

    # Reading

    _count_rows_sql = 'select count(*) from {}'

    def count_rows(self):
        self.assert_connected()
        query = self._count_rows_sql.format(self.name)
        rows = list(gen_fetchmany(self._db.execute_query(query)))
        if len(rows) != 1:
            raise db.DbError('Not a single row: {}'.format(rows))
        n_rows = rows[0][0]
        self._n_rows = n_rows
        return n_rows

    _select_rows_sql = 'select * from {}'

    def _record_iterator(self):
        self.assert_connected()
        query = self._select_rows_sql.format(self.name)
        if self._order_by_cols:
            cols = ', '.join(
                name + ' ' + order
                for (name, order) in self._order_by_cols)
            query += ' order by ' + cols
        return gen_fetchmany(self._db.execute_query(query))

    # Queries

    def order_by(self, *cols):
        self.assert_connected()
        # Interpret and validate columns
        columns = self._interpret_order_by_columns(cols)
        # Create a return new table with ordering columns
        table = copy.copy(self)
        table._order_by_cols = columns
        return table

    # Writing

    _add_all_sql = 'insert into {} {} values {}'

    def add_all(self, records):
        self.assert_connected()
        header = (records.header
                  if isinstance(records, recs.RecordStream)
                  else self.header)
        # Assemble query
        col_names = '(' + ', '.join(header.names()) + ')'
        param_placeholder = placeholders_for_params(self.n_cols)
        query = self._add_all_sql.format(
            self.name, col_names, param_placeholder)
        # Run query
        cursor = self._db.execute_many(query, records)
        rows = list(gen_fetchmany(cursor))
        if rows:
            raise db.DbError('Insert returned rows: {}'.format(rows))
        if cursor.rowcount > 0:
            self._n_rows += cursor.rowcount

    _clear_sql = 'delete from {}'

    def clear(self):
        self.assert_connected()
        query = self._clear_sql.format(self.name)
        cursor = self._db.execute_query(query)
        rows = list(gen_fetchmany(cursor))
        if rows:
            raise db.DbError('Delete returned rows: {}'.format(rows))
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
