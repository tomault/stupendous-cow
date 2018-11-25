from stupendous_cow.data_model import Article, ArticleType, Venue, Category
from stupendous_cow.util import normalize_title
from stupendous_cow.db.core import OneColumnResultSet, create_id_sequence_table
from stupendous_cow.db.tables import Table, EnumTable

import datetime
import os
import os.path
import sqlite3

    
class _Articles(Table):
    _UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0)
    table_columns = ('id', 'title', 'normalized_title', 'abstract',
                     'content', 'year', 'priority', 'downloaded_as', 'pdf_file',
                     'summary', 'is_read', 'created_at',
                     'last_updated_at', 'last_indexed_at',
                     'article_type_id', 'category_id', 'venue_id')
    
    def __init__(self, db, article_types, categories, venues):
        Table.__init__(self, 'Article', db, 'articles', self.table_columns,
                       self._create_article, self._get_column_value)
        self._article_types = article_types
        self._categories = categories
        self._venues = venues

        self._special_columns = {
            'normalized_title' : lambda a: normalize_title(a.title),
            'is_read' : lambda a: 'Y' if a.is_read else 'N',
            'article_type_id' :
                lambda a: a.article_type.id if a.article_type else None,
            'category_id' : lambda a: a.category.id if a.category else None,
            'venue_id' : lambda a: a.venue.id if a.venue else None
        }
        self._id_columns = { 'article_type' : ArticleType,
                             'category' : Category,
                             'venue' : Venue }

    def need_reindexing(self):
        sql = "SELECT id FROM articles " + \
              "WHERE (last_updated_at > last_indexed_at) OR " + \
                    "(last_indexed_at IS NULL)"
        rs = OneColumnResultSet(self._db.cursor(), lambda x: x)
        return rs.init(sql, ())

    def _create_article(self, id, title, normalized_title, abstract, content,
                        year, priority, downloaded_as, pdf_file, summary,
                        is_read, created_at, last_updated_at, last_indexed_at,
                        article_type_id, category_id, venue_id):
        def lookup_by_id(table, item_id):
            if item_id == None:
                return None
            return table.with_id(item_id)
        
        def to_dt(delta):
            if delta == None:
                return None
            return self._UNIX_EPOCH + datetime.timedelta(seconds = delta)
        
        is_read_value = is_read == 'Y'
        article_type = lookup_by_id(self._article_types, article_type_id)
        category = lookup_by_id(self._categories, category_id)
        venue = lookup_by_id(self._venues, venue_id)

        return Article(title, abstract, content, year, priority,
                       downloaded_as, pdf_file, article_type, category, venue,
                       summary, is_read_value, to_dt(created_at),
                       to_dt(last_updated_at), to_dt(last_indexed_at), id)

    def _get_column_value(self, article, column_name):
        get_value = self._special_columns.get(column_name,
                                              lambda a: getattr(a, column_name))
        return get_value(article)
    
    def _normalize_criteria(self, criteria):
        def extract_ids(constraint_name, items, item_type):
            if isinstance(items, item_type):
                return items.id
            elif isinstance(items, tuple) or isinstance(items, list):
                return [ i.id for i in items ]
            else:
                msg = 'Invalid Article constraint "%s" -- must be a single ' + \
                      '%s, a tuple of %s or a list of %s'
                msg = msg % (constraint_name, item_type, item_type, item_type)
                raise ValueError(msg)

        def normalize_constraint(name, value):
            if name in self._id_columns:
                id_column = name + '_id'
                item_type = self._id_columns[name]
                if id_column in criteria:
                    msg = 'Invalid Article constraints -- cannot specify ' + \
                          '"%s" and "%s" constraints simultaneously'
                    msg = msg % (name, id_column)
                    raise ValueError(msg)
                return (id_column, extract_ids(name, value, item_type))
            elif name == 'is_read':
                return ('is_read', 'Y' if value else 'N')
            else:
                return (name, value)

        return dict(normalize_constraint(n, v) \
                        for (n, v) in criteria.iteritems())

    def _set_defaults_for_write(self, values):
        now = datetime.datetime.now().replace(microsecond = 0)
        if not values['normalized_title']:
            values['normalized_title'] = normalize_title(values['title'])
        if not values['created_at']:
            values['created_at'] = now
        values['last_updated_at'] = now

    @staticmethod
    def create_table(cursor):
        cursor.execute("""
            CREATE TABLE articles (
                id NUMBER,
                title VARCHAR(512),
                normalized_title VARCHAR(512),
                abstract TEXT,
                content TEXT,
                year NUMBER,
                priority NUMBER,
                downloaded_as VARCHAR(128),
                pdf_file VARCHAR(2048),
                summary TEXT,
                is_read CHAR(1),
                created_at NUMBER NOT NULL,
                last_updated_at NUMBER NOT NULL,
                last_indexed_at NUMBER,
                article_type_id NUMBER,
                category_id NUMBER,
                venue_id NUMBER,
                PRIMARY KEY(id),
                FOREIGN KEY(article_type_id) REFERENCES article_types(id),
                FOREIGN KEY(category_id) REFERENCES categories(id),
                FOREIGN KEY(venue_id) REFERENCES venues(id)
            );""")

class _ArticleTypes(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'ArticleType', db, 'article_types',
                           ('id', 'name'), ArticleType, getattr)
        self._articles = None

    def _count_references_to(self, article_type_id):
        return self._articles.count(article_type_id = article_type_id)

    @staticmethod
    def create_table(cursor):
        cursor.execute("""
            CREATE TABLE article_types (
                id NUMBER,
                name VARCHAR(256) UNIQUE,
                PRIMARY KEY (id)
            )""")

class _Venues(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'Venue', db, 'venues',
                           ('id', 'name', 'abbreviation'),
                           Venue, getattr)
        self._articles = None

    def with_abbreviation(self, abbreviation):
        # TODO: Create an abbreviation map and maintain it in add(), update()
        #       and delete().  Linear scan is good enough for now
        tmp = [ x for x in self._all if x.abbreviation == abbreviation ]
        return tmp[0] if tmp else None

    def _count_references_to(self, venue_id):
        return self._articles.count(venue_id = venue_id)

    @staticmethod
    def create_table(cursor):
        cursor.execute("""
            CREATE TABLE venues (
                id NUMBER,
                name VARCHAR(256) UNIQUE,
                abbreviation VARCHAR(32) UNIQUE,
                PRIMARY KEY(id)
            )""")

class _Categories(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'Category', db, 'categories', ('id', 'name'),
                           Category, getattr)
        self._articles = None

    def _count_references_to(self, category_id):
        return self._articles.count(category_id = category_id)

    @staticmethod
    def create_table(cursor):
        cursor.execute("""
            CREATE TABLE categories (
                id NUMBER,
                name VARCHAR(256) UNIQUE,
                PRIMARY KEY (id)
            )""")

class Database:
    __slots__ = ('filename', '_db', '_articles', '_venues', '_categories')

    def __init__(self, filename, db = None):
        self.filename = filename
        if db:
            self._db = db
        else:
            self._db = sqlite3.connect(filename)
        self._article_types = _ArticleTypes(self._db)
        self._categories = _Categories(self._db)
        self._venues = _Venues(self._db)
        self._articles = _Articles(self._db, self._article_types,
                                   self._categories, self._venues)

        self._article_types._articles = self._articles
        self._categories._articles = self._articles
        self._venues._articles = self._articles

    @property
    def articles(self):
        return self._articles

    @property
    def article_types(self):
        return self._article_types
    
    @property
    def categories(self):
        return self._categories

    @property
    def venues(self):
        return self._venues

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

    def close(self):
        self._db.close()
        self._db = None
        
    @staticmethod
    def create_new(filename):
        if os.path.exists(filename):
            os.unlink(filename)
        db = sqlite3.connect(filename)
        try:
            cursor = db.cursor()
            try:
                Database._create_tables(cursor)
            finally:
                cursor.close()
        finally:
            db.close()

        db = Database(filename)

        Database._populate_article_types(db)
        Database._populate_categories(db)
        Database._populate_venues(db)

        db.commit()
        db.close()
        
        return Database(filename)

    @staticmethod
    def _create_tables(cursor):
        create_id_sequence_table(cursor)
        _ArticleTypes.create_table(cursor)
        _Categories.create_table(cursor)
        _Venues.create_table(cursor)
        _Articles.create_table(cursor)

    @staticmethod
    def _populate_article_types(db):
        db.article_types.add(ArticleType(name = ''))
        db.article_types.add(ArticleType(name = 'Poster'))
        db.article_types.add(ArticleType(name = 'Oral'))
        db.article_types.add(ArticleType(name = 'Spotlight'))
        db.article_types.add(ArticleType(name = 'Long Academic'))
        db.article_types.add(ArticleType(name = 'Long Industry'))
        db.article_types.add(ArticleType(name = 'Short Academic'))
        db.article_types.add(ArticleType(name = 'Short Industry'))
        db.article_types.add(ArticleType(name = 'Journal'))

    @staticmethod
    def _populate_categories(db):
        db.categories.add(Category(name = ''))
    
    @staticmethod
    def _populate_venues(db):
        db.venues.add(Venue(name = 'Association for Computational Linguistics',
                            abbreviation = 'ACL'))
        db.venues.add(Venue(name = 'ACM International Conference on Information and Knowledge Management',
                            abbreviation = 'CIKM'))
        db.venues.add(Venue(name = 'International Conference on Learning Representations',
                            abbreviation = 'ICLR'))
        db.venues.add(Venue(name = 'International Conference on Machine Learning',
                            abbreviation = 'ICML'))
        db.venues.add(Venue(name = 'ACM SIGKDD Conference On Knowledge Discovery and Data Mining',
                            abbreviation = 'KDD'))
        db.venues.add(Venue(name = 'Conference of the North American Chapter of the Association for Computational Linguistics',
                            abbreviation = 'NAACL'))
        db.venues.add(Venue(name = 'Neural Information Processing Systems',
                            abbreviation = 'NIPS'))
        db.venues.add(Venue(name = 'International ACM SIGIR Conference on Research and Development in Information Retrieval',
                            abbreviation = 'SIGIR'))
