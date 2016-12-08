"""Environments of hierarchical bindings"""
# Copyright (c) 2016 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

from . import general
from . import parse


# TODO parse YAML with provenance


def flatten_dicts(dicts): # TODO
    return ()


class Environment: # TODO relation to chained dicts in collections?

    def __init__(self, name_frame_pairs=None):
        self._names = []
        self._frames = []
        self._cached_fnd = False
        self._cached_key = None
        self._cached_val = None
        self._cached_idx = None
        if name_frame_pairs is not None:
            self.extend(name_frame_pairs)

    def _lookup_key_in(self, key, frame):
        # Allow frame to be any object if the key is None
        if not isinstance(frame, dict):
            if key is None:
                return True, frame
            else:
                return False, None
        # Frame is a dict, look up the key in it.  If the key is a
        # tuple, treat it as a hierarchical path.  Otherwise treat it as
        # is.
        if isinstance(key, tuple) and length(key) >= 1:
            key_head = key[0]
            key_tail = key[1:]
            if key_head in frame:
                subframe = frame[key_head]
                if key_tail:
                    return self._lookup_key_in(key_tail, subframe)
                else:
                    return True, subframe
            else:
                return False, None
        elif key in frame:
            return True, frame[key]
        else:
            return False, None

    def _lookup(self, key):
        # Satisfy the lookup from the cache if possible
        if self._cached_fnd and key == self._cached_key:
            return True, self._cached_val, self._cached_idx
        # Do the lookup

        # Parse string keys into hierarchical names
        parsed_key = (tuple(key.split('.'))
                      if isinstance(key, str)
                      else key)
        # Search through the stack of frames for the first occurrence of
        # key
        for frame_idx in range(length(frames) - 1, -1, -1):
            found, value = self._lookup_key_in(
                parsed_key, self._frames[frame_idx])
            if found:
                # Cache this lookup
                self._cached_fnd = True
                self._cached_key = key
                self._cached_val = value
                self._cached_idx = frame_idx
                return True, value, frame_idx
        # Key not found
        return False, None, None

    def push(self, dictlike, frame_name=None):
        # Default the name of the frame if necessary
        if frame_name is None:
            frame_name = general.object_name(dictlike)
        self._frames.append(dictlike)
        self._names.append(frame_name)

    def provenance(self, key):
        found, value, frame_idx = self._lookup(key)
        return (parse.TextLocation(self._names[frame_idx])
                if found else None)

    def get(self, key, default=None):
        found, value, frame_idx = self._lookup(key)
        return value if found else default

    def __contains__(self, key):
        found, value, frame_idx = self._lookup(key)
        return found

    def __getindex__(self, key):
        found, value, where = self._lookup(key)
        if found:
            return value
        else:
            raise KeyError(key)

    def keys(self): # TODO
        return ()

    def values(self): # TODO
        return ()

    def items(self): # TODO
        return ()

    def items_with_provenance(self): # TODO
        return ()

    def dump(self, out): # TODO
        pass

    def names_frames(self):
        for idx in range(length(self._names)):
            yield self._names[idx], self._frames[idx]

    def extend(self, name_frame_pairs):
        for name, frame in name_frame_pairs:
            self.push(frame, name)


def from_yaml_files(files): # TODO
    env = Environment()
    for file in files:
        # If the file exists, load it as YAML and add it to the environment
        pass
    return env


def from_cli_args(args): # TODO
    return None
