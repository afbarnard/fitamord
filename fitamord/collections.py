"""General-purpose collections"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.

import itertools as itools

from . import general


class NamedItems:
    """An ordered collection of items that are accessible by name or index.

    """

    def __init__(
            self, *named_items, names=None, items=None, **names2items):
        self._names = []
        self._items = []
        self._names2idxs = {}
        named_items_iterables = []
        if named_items:
            named_items_iterables.append(named_items)
        if names and items:
            named_items_iterables.append(zip(names, items))
        if names2items:
            named_items_iterables.append(names2items.items())
        for named_item in itools.chain(*named_items_iterables):
            self.add_named_item(named_item)

    def add_named_item(self, named_item):
        self.add(*named_item)

    def add(self, name, item):
        if name in self._names2idxs:
            raise ValueError('Duplicate name: {!r}'.format(name))
        self._names2idxs[name] = len(self._names)
        self._names.append(name)
        self._items.append(item)

    def names(self):
        return iter(self._names)

    def items(self):
        return iter(self._items)

    def names_items(self):
        return zip(self._names, self._items)

    def name_at(self, index):
        return self._names[index]

    def item_at(self, index):
        return self._items[index]

    def item_of(self, name):
        return self._items[self._names2idxs[name]]

    def index_of(self, name):
        if isinstance(name, int):
            return name
        return self._names2idxs[name]

    def __len__(self):
        return len(self._names)

    def __getitem__(self, index):
        if isinstance(index, str):
            return self.item_of(index)
        return self.item_at(index)

    # Imitate a list here by iterating over items, not (name, item)
    # pairs.  This also matches the default iteration behavior one gets
    # from defining `__len__` and `__getitem__`.
    __iter__ = items

    def __repr__(self):
        return ('{}({})'.format(
            general.fq_typename(self),
            ', '.join(repr(n_i) for n_i in self.names_items())))

    def has_name(self, name):
        return name in self._names2idxs

    def has_item(self, item):
        return item in self._items

    def has_named_item(self, named_item):
        if not hasattr(named_item, '__iter__'):
            return False
        named_item = tuple(named_item)
        if len(named_item) != 2:
            return False
        name, item = named_item
        return name in self._names2idxs and item == self.item_of(name)

    def has_index(self, index):
        return isinstance(index, int) and 0 <= index < len(self)

    def __contains__(self, obj):
        """Whether this collection contains the given object.

        The object is first tried as a name, then as a (name, item)
        pair, and then as an item.

        """
        return (self.has_name(obj)
                or self.has_named_item(obj)
                or self.has_item(obj))

    def __eq__(self, obj):
        return (type(self) == type(obj)
                and self._names == obj._names
                and self._items == obj._items)
