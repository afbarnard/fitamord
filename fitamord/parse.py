"""Parsing"""

# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


# TODO reconcile with barnapy.parse


class TextLocation:

    def __init__(self, name=None, line=None, column=None):
        self._name = name
        self._line = line
        self._column = column

    @property
    def name(self):
        return self._name

    @property
    def line(self):
        return self._line

    @property
    def column(self):
        return self._column

    def __repr__(self):
        return ('TextLocation(name={!r}, line={!r}, col={!r})'
                .format(self._name, self._line, self._column))

    def __str__(self):
        name = self._name if self._name is not None else '<unknown>'
        return '{}@({},{})'.format(name, self._line, self._column)
