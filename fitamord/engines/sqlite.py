"""SQLite DB engine / backend"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import sqlite3
import sys

from barnapy import logging

from .. import db
from .. import general


def placeholders_for_params(params):
    return '(?' + (', ?' * (len(params) - 1)) + ')'

class SqliteDb(db.Database):
    """Represents a SQLite database and provides schema-level operations"""

    def __init__(self, filename=None): # TODO max mem, max threads, other options
        self._filename = (filename
                          if filename is not None
                          else ':memory:')
        self._logid = hex(id(self))
        self._logger = logging.getLogger(general.fq_typename(self))
        self._logger.info(
            '{}: Opening connection to Sqlite DB: {}'
            .format(self._logid, self._filename))
        # TODO open and maintain a connection at the object level but what about committing, closing the connection, etc.?
        self._connection = sqlite3.connect(self._filename)
        self._logger.info('{}: Connected'.format(self._logid))

    def __del__(self):
        """Close and invalidate the database connection"""
        self._logger.info(
            '{}: Closing DB connection'.format(self._logid))
        try:
            self._connection.close()
        except Exception as e:
            self._logger.exception(
                '{}: Failed to close DB connection: {}'
                .format(self._logid, e))
        else:
            self._logger.info(
                '{}: DB connection closed'.format(self._logid))
        finally:
            # Release reference no matter what
            self._connection = None

    def _execute_query(self, query, parameters=None):
        self._logger.info(
            '{}: Executing query: {}; parameters: {}'
            .format(self._logid, query, parameters))
        cursor = self._connection.cursor()
        if parameters is None:
            cursor.execute(query)
        else:
            cursor.execute(query, parameters)
        rows = cursor.fetchmany()
        while rows:
            yield from rows
            rows = cursor.fetchmany()

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

    def namespaces(self):
        yield from (db_name
                    for db_id, db_name, filename
                    in self._execute_query('pragma database_list'))

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
                placeholders_for_params(parameters))
        else:
            parameters = (db.DbObjectType.convert(types).name,)
            query = self._list_objects_with_types_sql.format(
                catalog_table_name.name,
                placeholders_for_params(parameters))
        # Generate the objects as (name, type, sql) tuples
        yield from self._execute_query(query, parameters)

    _get_schema_sql = 'select sql from {} where name = ?'

    def schema(self, name):
        # Parse and check name
        dotted_name = self.interpret_sql_name(name)
        namespace, obj_name = dotted_name.rest_last()
        # Default to `main` namespace
        if namespace is None:
            namespace = 'main'
        catalog_table_name = self._catalog_table_name(namespace)
        query = self._get_schema_sql.format(catalog_table_name.name)
        schemas = list(self._execute_query(query, (obj_name,)))
        if len(schemas) == 0:
            raise db.DbError('Object not found: {}'.format(name))
        elif len(schemas) > 1:
            raise db.DbError(
                'Multiple schemas for object: {}'.format(name))
        else:
            return schemas[0][0]

    def create_table(self, name, header): # TODO
        pass

    def drop_table(self, name): # TODO
        pass
