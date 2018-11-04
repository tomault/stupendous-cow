from stupendous_cow.db.core import \
    execute_count, execute_select, execute_insert, execute_update, \
    execute_delete, next_item_id, ResultSet, OneColumnResultSet

class Table:
    def __init__(self, type_name, db, table_name, columns, item_constructor,
                 column_value_extractor):
        self._type_name = type_name
        self._db = db
        self._table_name = table_name
        self._columns = columns
        self._id_column = columns[0]
        self._create_item = item_constructor
        self._get_column_value = column_value_extractor

        self._retrieve_ids_sql = \
            'SELECT %s FROM %s' % (self._id_column, self._table_name)
        self._item_exists_sql = \
            'SELECT count(*) FROM %s WHERE %s = ?' % (table_name, columns[0])

    @property
    def ids(self):
        def return_id(args):
            return args[self._id_column]

        with OneColumnResultSet(self._db.cursor(), lambda x: x) as rs:
            rs.init(self._retrieve_ids_sql, ())
            return [ i for i in rs ]

    @property
    def all(self):
        with execute_select(self._db, self._table_name, self._columns, { },
                            self._create_item) as items:
            return [ x for x in items ]

    def count(self, **criteria):
        return execute_count(self._db, self._table_name,
                             self._normalize_criteria(criteria))

    def retrieve(self, **criteria):
        return execute_select(self._db, self._table_name, self._columns,
                              self._normalize_criteria(criteria),
                              self._create_item)

    def with_id(self, id):
        with execute_select(self._db, self._table_name, self._columns,
                            { self._id_column : id },
                            self._create_item) as results:
            try:
                return next(results)
            except StopIteration:
                return None
        
    def add(self, item):
        item_id = self._get_column_value(item, self._id_column)
        if item_id:
            msg = 'Cannot add %s if it already has an id' % self._type_name
            raise ValueError(msg)

        item_id = next_item_id(self._db, self._table_name)
        values = self._get_column_values(item)
        self._set_defaults_for_write(values)
        values[self._id_column] = item_id
        execute_insert(self._db, self._table_name, values)
        return self.with_id(item_id)

    def update(self, item):
        item_id = self._get_column_value(item, self._id_column)
        if not item_id:
            raise ValueError('Item has no id.  Use the add() method to add ' + \
                             'new items to the database')
        elif not self._exists(item_id):
            msg = 'No %s with id %s exists in the database.' % (self._type_name,
                                                                item_id)
            raise ValueError(msg)
        else:
            values = self._get_column_values(item)
            self._set_defaults_for_write(values)
            execute_update(self._db, self._table_name, values,
                           { self._id_column : item_id })

    def delete(self, item_id):
        execute_delete(self._db, self._table_name,
                       { self._id_column : item_id })

    def _exists(self, item_id):
        return execute_count(self._db, self._table_name,
                             {self._id_column : item_id })

    def _normalize_criteria(self, criteria):
        return criteria

    def _get_column_values(self, item):
        return dict((c, self._get_column_value(item, c)) \
                          for c in self._columns[1:])

    def _set_defaults_for_write(self, values):
        pass

class EnumTable:
    def __init__(self, type_name, db, table_name, columns, item_constructor,
                 column_value_extractor):
        def id_for(x):
            return self._get_column_value(x, columns[0])
        def name_for(x):
            return self._get_column_value(x, columns[1])
        
        self._type_name = type_name
        self._db = db
        self._table_name = table_name
        self._columns = columns
        self._id_column = columns[0]
        self._name_column = columns[1]
        self._create_item = item_constructor
        self._get_column_value = column_value_extractor

        with execute_select(db, table_name, columns, { },
                            item_constructor) as results:
            self._all = [ x for x in results ]
        self._by_id = dict((id_for(x), x) for x in self._all)
        self._by_name = dict((name_for(x), x) for x in self._all)

    @property
    def all(self):
        return self._all

    def with_id(self, item_id):
        return self._by_id.get(item_id, None)

    def with_name(self, item_name):
        return self._by_name.get(item_name, None)

    def count_references_to(self, item):
        item_id = self._get_column_value(item, self._id_column)
        return self._count_references_to(item_id)

    def add(self, item):
        item_id = self._get_column_value(item, self._id_column)
        item_name = self._get_column_value(item, self._name_column)

        if item_id:
            msg = '%s with id %s already exists' % (self._type_name, item_id)
            raise ValueError(msg)
        if item_name in self._by_name:
            msg = '%s with name "%s" already exists' % (self._type_name,
                                                        item_id)
            raise ValueError(msg)
        values = dict((x, self._get_column_value(item, x)) \
                          for x in self._columns[1:])
        item_id = next_item_id(self._db, self._table_name)
        values[self._id_column] = item_id
        execute_insert(self._db, self._table_name, values)
        with execute_select(self._db, self._table_name, self._columns,
                            { self._id_column : item_id },
                            self._create_item) as results:
            new_item = next(results)
        self._all.append(new_item)
        self._by_id[item_id] = new_item

        item_name = self._get_column_value(new_item, self._name_column)
        self._by_name[item_name] = new_item

        return new_item

    def update(self, item):
        item_id = self._get_column_value(item, self._id_column)
        item_name = self._get_column_value(item, self._name_column)
        
        if not item_id:
            msg = 'Cannot update %s with no id' % self._type_name
            raise ValueError(msg)
        if not item_id in self._by_id:
            msg = 'Cannot update non-existent %s with id %s'
            msg = msg % (self._type_name, item_id)
            raise ValueError(msg)
        old_item_name = self._by_id[item_id].name

        values = dict((x, self._get_column_value(item, x)) \
                          for x in self._columns[1:])
        id_criteria = { self._id_column : item_id }
        execute_update(self._db, self._table_name, values, id_criteria)

        with execute_select(self._db, self._table_name, self._columns,
                            id_criteria, self._create_item) as results:
            new_item = next(results)

        for (n, item) in enumerate(self._all):
            if self._get_column_value(item, self._id_column) == item_id:
                self._all[n] = new_item
                break
        self._by_id[item_id] = new_item

        if item_name != old_item_name:
            del self._by_name[old_item_name]
        self._by_name[item_name] = new_item

    def delete(self, item):
        def id_for(x):
            return self._get_column_value(x, self._id_column)
        
        item_id = id_for(item)
        item_name = self._get_column_value(item, self._name_column)

        if not item_id:
            msg = 'Cannot delete %s with no id' % self._type_name
            raise ValueError(msg)

        if not item_id in self._by_id:
            msg = 'Cannot delete non-existent %s "%s"'
            msg = msg % (self._type_name, item.name)
            raise ValueError(msg)

        if self._by_id[item_id].name != item_name:
            msg = 'Cannot delete %s with id %s -- the item with that id ' + \
                  'has name "%s" but the name of the %s passed to ' + \
                  'delete() is "%s" -- the two must be the same.'
            msg = msg % (self._type_name, item_id, self._by_id[item_id].name,
                         self._type_name, item_name)
            raise ValueError(msg)
        
        if self._count_references_to(item_id):
            msg = 'Cannot delete %s "%s" because there are still ' + \
                  'references to it'
            msg = msg % (self._type_name, item.name)
            raise ValueError(msg)

        execute_delete(self._db, self._table_name,
                       { self._id_column : item_id })
        self._all = [ x for x in self._all if id_for(x) != item_id ]
        del self._by_id[item_id]
        del self._by_name[item_name]

    def _count_references_to(self, item_id):
        raise RuntimeError('_EnumTable._count_references_to not implemented')
