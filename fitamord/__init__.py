"""Fitamord is a framework for the intelligent transformation and
modeling of relational data.

Copyright (c) 2016 Aubrey Barnard.  This is free software released under
the MIT License.  See `LICENSE.txt` for details.

"""

# Module Organization
# -------------------
#
# Related functionality is placed in files.  All files are treated as
# modules.  The exception is the core functionality (records and tables)
# which is exported here at the top-level.


# Bring in core classes related to records and tables
from .records import *
