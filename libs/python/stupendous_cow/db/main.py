from stupendous_cow.data_model import Article, ArticleType, Venue, Category
from stupendous_cow.db.core import OneColumnResultSet
from stupendous_cow.db.tables import Table, EnumTable

import datetime
import os
import os.path
import sqlite3

    
class _Articles(Table):
    def __init__(self, db, article_types, categories, venues):
        Table.__init__(self, 'Article', db, 'articles',
                       ('id', 'title', 'abstract', 'content', 'year',
                        'priority', 'downloaded_as', 'pdf_file',
                        'summary', 'is_read', 'created_at',
                        'last_updated_at', 'last_indexed_at',
                        'article_type_id', 'category_id', 'venue_id'),
                       self._create_article, self._get_column_value)
        self._article_types = article_types
        self._categories = categories
        self._venues = venues

        self._special_columns = {
            'article_type_id' : lambda a: a.article_type.id,
            'category_id' : lambda a: a.category.id,
            'venue_id' : lambda a: a.venue.id
        }
        self._id_columns = ('article_type', 'category', 'venue')

    def need_reindexing(self):
        sql = 'SELECT id FROM articles WHERE last_indexed_at > last_updated_at'
        rs = OneColumnResultSet(self._db.cursor(), lambda x: x)
        return rs.init(sql, ())

    def _create_article(self, id, title, abstract, content, year, priority,
                        downloaded_as, pdf_file, summary, is_read, created_at,
                        last_updated_at, last_indexed_at, article_type_id,
                        category_id, venue_id):
        article = self._article_types.with_id(article_type_id)
        category = self._categories.with_id(category_id)
        venue = self._venues.with_id(venue_id)

        return Article(title, abstract, content, year, priority,
                       downloaded_as, pdf_file, summary, is_read, created_at,
                       last_updated_at, last_indexed_at, article, category,
                       venue, id)

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

        def normalize_constraint(self, name, value):
            if name in self._id_columns:
                id_column = name + '_id'
                if id_column in criteria:
                    msg = 'Invalid article constraints -- cannot specify ' + \
                          '"%s" and "%s" constraints simultaneously'
                    msg = msg % (name, id_column)
                    raise ValueError(msg)
                return (id_column, extract_ids(value))
            return (name, value)

        return dict((n, normalize_constraint(v)) \
                        for (n, v) in criteria.iteritems())

    def _set_defaults_for_write(self, values):
        now = datetime.datetime.now()
        if not values['created_at']:
            values['created_at'] = now
        values['last_updated_at'] = now

class _ArticleTypes(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'ArticleType', db, 'article_types',
                           ('id', 'name'), ArticleType, getattr)
        self._articles = None

    def _count_references_to(self, article_type_id):
        return self._articles.count(article_type_id = article_type_id)

class _Venues(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'Venue', db, 'venues',
                           ('id', 'name', 'abbreviation'),
                           Venue, getattr)
        self._articles = None

    def _count_references_to(self, venue_id):
        return self._articles.count(venue_id = venue_id)

class _Categories(EnumTable):
    def __init__(self, db):
        EnumTable.__init__(self, 'Category', db, 'categories', ('id', 'name'),
                           Category, getattr)
        self._articles = None

    def _count_references_to(self, category_id):
        return self._articles.count(category_id = category_id)
        
class Database:
    __slots__ = ('filename', '_db', '_articles', '_venues', '_categories')

    def __init__(self, filename):
        self.filename = filename
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
                cursor.execute("""
                    CREATE TABLE id_sequence (
                        table_name VARCHAR(256) PRIMARY KEY,
                        id NUMBER
                    );
                """)
                cursor.execute("""
                    CREATE TABLE article_types (
                        id NUMBER,
                        name VARCHAR(256) UNIQUE,
                        PRIMARY KEY (id)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE categories (
                        id NUMBER,
                        name VARCHAR(256) UNIQUE,
                        PRIMARY KEY(id)
                    );""")
                cursor.execute("""
                    CREATE TABLE venues (
                        id NUMBER,
                        name VARCHAR(256) UNIQUE,
                        abbreviation VARCHAR(32) UNIQUE,
                        PRIMARY KEY(id)
                    );""")
                cursor.execute("""
                    CREATE TABLE articles (
                        id NUMBER,
                        title VARCHAR(512),
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
                        FOREIGN KEY(article_type_id)
                            REFERENCES article_types(id),
                        FOREIGN KEY(category_id) REFERENCES categories(id),
                        FOREIGN KEY(venue_id) REFERENCES venues(id)
                    );""")
            finally:
                cursor.close()
        finally:
            db.close()

        db = Database(filename)
        db.article_types.add(ArticleType(name = 'Poster'))
        db.article_types.add(ArticleType(name = 'Oral'))
        db.article_types.add(ArticleType(name = 'Spotlight'))
        db.article_types.add(ArticleType(name = 'Long Academic'))
        db.article_types.add(ArticleType(name = 'Long Industry'))
        db.article_types.add(ArticleType(name = 'Short Academic'))
        db.article_types.add(ArticleType(name = 'Short Industry'))
        db.article_types.add(ArticleType(name = 'Journal'))

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
        db.commit()
        db.close()
        
        return Database(filename)
    
    
    
