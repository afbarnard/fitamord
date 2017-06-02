"""Tests db.py"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import string
import unittest

from .. import db


class IdentifierParseTest(unittest.TestCase):

    _simple_identifiers = (
        # Plain
        ('', False),
        ('a', True),
        ('A', True),
        ('_', True),
        ('0', False),
        ('_0', True),
        ('azAZ09_', True),
        ('_90ZAza', True),
        # Other Unicode letters should be included but the pattern for
        # that is too complicated to worry about for now

        # Quotes
        ('"', False),
        ('""', False),
        ('"""', False),
        ('""""', True),
        ('"""""', False),
        ('""""""', True),
        ('"ab', False),
        ('a"b', False),
        ('ab"', False),
        ('""ab', False),
        ('"a"b', False),
        ('"ab"', True),
        ('a""b', False),
        ('a"b"', False),
        ('"a"b"', False),
        ('"a""to""z"', True),
        ('a$', False),
        ('"a$"', True),
        ('$#@!', False),
        ('"$#@!"', True),
        )

    def test_match_simple_identifier(self):
        for text, is_valid in IdentifierParseTest._simple_identifiers:
            match = db._sql_identifier_pattern.fullmatch(text)
            if is_valid:
                self.assertIsNotNone(match)
            else:
                self.assertIsNone(match)

    _compound_identifiers = (
        # Plain
        ('.', False),
        ('.a', False),
        ('a.', False),
        ('a.b', True),
        ('.a.b', False),
        ('a.b.', False),
        ('a.b.c', True),
        ('.'.join(string.ascii_letters), True),
        ('a.bc.def.ghij', True),

        # Quotes
        ('"a.b.c"', True),
        ('"a"."b"."c"', True),
        ('"a.b".c', True),
        ('a."b.c"', True),
        ('a."b".c', True),
        ('"""a"".""b"".""c"""', True),
        ('"""."""', True),
        ('""a.b', False),
        ('"a".b', True),
        ('"a."b', False),
        ('"a.b"', True),
        ('a"".b', False),
        ('a"."b', False),
        ('a".b"', False),
        ('a.""b', False),
        ('a."b"', True),
        )

    def test_match_compound_identifier(self):
        for text, is_valid in IdentifierParseTest._compound_identifiers:
            match = db._sql_identifier_pattern.fullmatch(text)
            if is_valid:
                self.assertIsNotNone(match)
            else:
                self.assertIsNone(match)

    _quote_unquote = (
        ('', ''),
        ('a', 'a'),
        ('""a', 'a'),
        ('"a"', 'a'),
        ('a""', 'a'),
        ('"a"b', 'ab'),
        ('"ab"', 'ab'),
        ('a""b', 'ab'),
        ('a"b"', 'ab'),
        ('a"b"c', 'abc'),
        ('""', ''),
        ('""""', '"'),
        ('""""""""""""', '"""""'),
        ('"""a"""', '"a"'),
        ('"a""b""c"', 'a"b"c'),
        )

    def test_unquote(self):
        for text, expected in IdentifierParseTest._quote_unquote:
            actual = db.unquote(text)
            self.assertEqual(expected, actual)
