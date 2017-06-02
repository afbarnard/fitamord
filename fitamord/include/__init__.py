"""Import management for third-party software.

Sets up the subdirectories of the `include` directory as submodules of
this module so that third-party packages / modules can be imported using
statements like:

    import .include.foo
    import .include.foo.bar
    from .include import foo
    from .include.foo import bar

"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import os
import os.path


# Project directory structure
include_pkg_dir = os.path.dirname(__file__)
src_dir = os.path.dirname(include_pkg_dir)
base_dir = os.path.dirname(src_dir)
include_dir = os.path.join(base_dir, 'include')

# Add all subdirectories of `include_dir` to `__path__` so that they can
# be accessed as subpackages / submodules.
dir_lister = os.walk(include_dir)
_, directories, _ = next(dir_lister)
for directory in directories:
    __path__.append(os.path.join(include_dir, directory))
