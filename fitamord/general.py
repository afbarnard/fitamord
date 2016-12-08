"""General-purpose functionality, utilities, and other things that don't
fit elsewhere

"""
# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


def fq_typename(obj):
    """Return the fully-qualified type name of an object or type."""
    typ = type(obj) if not isinstance(obj, type) else obj
    return '{}.{}'.format(typ.__module__, typ.__qualname__)


def object_name(obj):
    """Return a unique name for the specific object."""
    return '{}@{}'.format(fq_typename(obj), hex(id(obj)))


def check_type(obj, typ, msg_template='Expected: {}, but got: {}'):
    """Check the type of an object and raise TypeError if incorrect."""
    if not isinstance(obj, typ):
        raise TypeError(msg_template.format(typ, obj))
