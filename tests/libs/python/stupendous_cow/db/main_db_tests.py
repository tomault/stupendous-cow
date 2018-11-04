"""Unit tests for stupendous_cow.db.Database and its associated classes."""
from stupendous_cow.db.core import ResultSet, create_id_sequence_table
from stupendous_cow.db.constraints import GreaterEqual, InRange, NotNull
from stupendous_cow.db.main import Database, _Articles, _ArticleTypes, \
                                   _Categories, _Venues
from stupendous_cow.data_model import Article, ArticleType, Category, Venue
from stupendous_cow.testing import DatabaseTestCase
import datetime
import unittest

class EnumTypeWrapper:
    def __init__(self, id_map):
        self.id_map = id_map

    def with_id(self, id):
        return self.id_map.get(id, None)

_UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0)

class ArticleTableTests(DatabaseTestCase):
    article_fields = ('id', 'title', 'abstract', 'content', 'year', 'priority',
                      'downloaded_as', 'pdf_file', 'article_type', 'category',
                      'venue', 'summary', 'is_read', 'created_at',
                      'last_updated_at', 'last_indexed_at')
    
    article_types = [ ArticleType('Oral', 1), ArticleType('Poster', 2),
                      ArticleType('Spotlight', 3) ]
    id_to_article_type = dict((t.id, t) for t in article_types)

    categories = [ Category('Architecture', 1), Category('RL', 2),
                   Category('GANs', 3) ]
    id_to_category = dict((c.id, c) for c in categories)

    venues = [ Venue('International Conference on Machine Learning', 'ICML',
                     1),
               Venue('Neural Information Processing Systems', 'NIPS', 2),
               Venue('Association for Computational Linguistics', 'ACL', 3) ]
    id_to_venue = dict((v.id, v) for v in venues)
    
    def setUp(self):
        def to_seconds(dt):
            if dt == None:
                return dt
            return (dt - _UNIX_EPOCH).total_seconds()
        
        def insert_article(article):
            stmt = "INSERT INTO articles VALUES (:1, :2, :3, :4, :5, :6, " + \
                                                ":7, :8, :9, :10, :11, " + \
                                                ":12, :13, :14, :15, :16)"
            is_read = 'Y' if article.is_read else 'N'            
            data = (article.id, article.title, article.abstract,
                    article.content, article.year, article.priority,
                    article.downloaded_as, article.pdf_file, article.summary,
                    is_read, to_seconds(article.created_at),
                    to_seconds(article.last_updated_at),
                    to_seconds(article.last_indexed_at),
                    article.article_type.id, article.category.id,
                    article.venue.id)
            cursor.execute(stmt, data)

        self.all_articles = [ ]
        pdf_dir = '/home/tomault/conferences/downloads'
        cursor = self.db.cursor()
        try:
            cr_dt = datetime.datetime(2018, 10, 10, 12, 34, 56)
            upd_dt = datetime.datetime(2018, 10, 12, 03, 22, 19)
            idx_dt = datetime.datetime(2018, 10, 12, 04, 01, 00)
            article = Article('Cows Are Cool', 'Cows are really cool.',
                              'Cows are really cool.  Yes they are.', 2018, 9,
                              'CowsAreCool', pdf_dir + 'CowsAreCool.pdf',
                              self.article_types[0], self.categories[1],
                              self.venues[0],
                              'The authors show how cows are cool.', False,
                              cr_dt, upd_dt, idx_dt, id = 1)
            insert_article(article)
            self.all_articles.append(article)

            cr_dt = datetime.datetime(2017, 10, 8,  9, 10, 11)
            upd_dt = cr_dt
            idx_dt = cr_dt
            article = Article('Penguins Are Cute', 'Penguins are really cute.',
                              'Penguins are the cutest animal in the ' + \
                              'world.  No animals are cuter than penguins.',
                              2017, 11, 'PenguinsAreCute',
                              pdf_dir + 'PenguinsAreCute.pdf',
                              self.article_types[1], self.categories[0],
                              self.venues[2], 'Proof that penguins are cute.',
                              True, cr_dt, upd_dt, idx_dt, id = 2)
            insert_article(article)
            self.all_articles.append(article)
            
            cr_dt = datetime.datetime(2018, 6, 1,  12, 19, 04)
            upd_dt = datetime.datetime(2018, 6, 3, 15, 34, 19)
            idx_dt = datetime.datetime(2018, 6, 2, 12,  0,  0)
            article = Article('Moo moo you you', 'Watashi wa ushi desu.',
                              'Watashi wa ushi desu.  I am a cow.  I am ' + \
                              'the best cow in the world.',
                              2018, 5, 'MooMooYouYou',
                              pdf_dir + 'MooMooYouYou.pdf',
                              self.article_types[0], self.categories[1],
                              self.venues[2],
                              "The author's experience as a cow.", False,
                              cr_dt, upd_dt, idx_dt, id = 3)
            insert_article(article)
            self.all_articles.append(article)

            cr_dt = datetime.datetime(2016, 5, 17,  8, 30, 39)
            upd_dt = datetime.datetime(2018, 7, 1, 19, 15, 13)
            idx_dt = None
            article = Article('Fun On A Bun', 'Fun with hot dogs.',
                              'Hot dogs are yummy.  So are sausages.  ' + \
                              'Bratwurst is the best!',
                              2016, 3, 'FunOnABun', pdf_dir + 'FunOnABun.pdf',
                              self.article_types[2], self.categories[2],
                              self.venues[1],
                              "So much fun to be had on a bun.", True,
                              cr_dt, upd_dt, idx_dt, id = 4)
            insert_article(article)
            self.all_articles.append(article)

            cursor.execute("INSERT INTO id_sequence VALUES('articles', 5)")
            self.db.commit()
        finally:
            cursor.close()


        self.table = _Articles(self.db,
                               EnumTypeWrapper(self.id_to_article_type),
                               EnumTypeWrapper(self.id_to_category),
                               EnumTypeWrapper(self.id_to_venue))

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM articles")
            cursor.execute("DELETE FROM id_sequence")
            self.db.commit()
        finally:
            cursor.close()

    def test_all_ids_property(self):
        self.assertEqual([ x.id for x in self.all_articles ],
                         sorted(self.table.ids))

    def test_all_articles_property(self):
        self._verify_articles(self.all_articles,
                              sorted(self.table.all, key = lambda a: a.id))

    def test_count(self):
        self.assertEqual(2, self.table.count(is_read = True))

        values = (self.article_types[0], self.article_types[2])
        self.assertEqual(3, self.table.count(article_type = values))

        dt = datetime.datetime(2018, 9, 1, 0, 0, 0)
        n = self.table.count(last_indexed_at = GreaterEqual(dt))
        self.assertEqual(1, n)

        n = self.table.count(last_indexed_at = None)
        self.assertEqual(1, n)

        n = self.table.count(priority = None)
        self.assertEqual(0, n)

    def test_retrieve(self):
        with self.table.retrieve(venue = self.venues[2], year = 2018) as rs:
            result = [ x for x in rs ]
        self._verify_articles([ self.all_articles[2] ], result)

    def test_need_reindexing(self):
        with self.table.need_reindexing() as rs:
            ids = [ x for x in rs ]
        self.assertEqual([ 3, 4 ], ids)

    def test_add(self):
        new_article = Article(\
            'Cows On The Run',
            'Some cows are on the run after pulling a bank heist.',
            'Ushi and Daisy are bored with their life on the firm, so they ' + \
            'decide to plan a bank heist.',
            2016, 5, 'CowsOnTheRun',
            '/home/tomault/conferences/downloads/CowsOnTheRun.pdf',
            self.article_types[2], self.categories[2], self.venues[0],
            'Some cows plan a bank heist.')
        now = datetime.datetime.now()
        created = self.table.add(new_article)

        self.assertEqual(5, created.id)
        self.assertEqual(new_article.title, created.title)
        self.assertEqual(new_article.abstract, created.abstract)
        self.assertEqual(new_article.content, created.content)
        self.assertEqual(new_article.year, created.year)
        self.assertEqual(new_article.priority, created.priority)
        self.assertEqual(new_article.downloaded_as, created.downloaded_as)
        self.assertEqual(new_article.pdf_file, created.pdf_file)
        self.assertEqual(new_article.article_type, created.article_type)
        self.assertEqual(new_article.category, created.category)
        self.assertEqual(new_article.venue, created.venue)
        self.assertEqual(new_article.summary, created.summary)
        self.assertEqual(new_article.is_read, created.is_read)
        self.assertIsNone(created.last_indexed_at)

        delta = (created.created_at - now).total_seconds()
        self.assertLessEqual(delta, 0.1)

        delta = (created.last_updated_at - now).total_seconds()
        self.assertLessEqual(delta, 0.1)
        self.assertEqual(created.created_at, created.last_updated_at)

        self._verify_articles(self.all_articles + [ created ],
                              self._retrieve_all())

    def test_update(self):
        article = self.all_articles[3]
        orig_id = article.id
        orig_created_at = article.created_at
        orig_updated_at = article.last_updated_at

        now = datetime.datetime.now()
        index_time = \
            (now - datetime.timedelta(seconds = 300)).replace(microsecond = 0)
        article.last_indexed_at = index_time

        self.table.update(article)
        new_article = self.table.with_id(article.id)

        self.assertEqual(orig_id, article.id)
        self.assertEqual(article.title, new_article.title)
        self.assertEqual(article.abstract, new_article.abstract)
        self.assertEqual(article.content, new_article.content)
        self.assertEqual(article.year, new_article.year)
        self.assertEqual(article.priority, new_article.priority)
        self.assertEqual(article.downloaded_as, new_article.downloaded_as)
        self.assertEqual(article.pdf_file, new_article.pdf_file)
        self.assertEqual(article.article_type, new_article.article_type)
        self.assertEqual(article.category, new_article.category)
        self.assertEqual(article.venue, new_article.venue)
        self.assertEqual(article.summary, new_article.summary)
        self.assertEqual(article.is_read, new_article.is_read)
        self.assertEqual(orig_created_at, new_article.created_at)
        self.assertEqual(index_time, new_article.last_indexed_at)

        delta = (new_article.last_updated_at - now).total_seconds()
        self.assertGreaterEqual(new_article.last_updated_at, orig_updated_at)
        self.assertLessEqual(delta, 0.1)

        article.last_updated_at = new_article.last_updated_at
        self._verify_articles(self.all_articles, self._retrieve_all())        

    def _retrieve_all(self):
        def create_article(id, title, abstract, content, year, priority,
                           downloaded_as, pdf_file, summary, is_read,
                           created_at, last_updated_at, last_indexed_at,
                           article_type_id, category_id, venue_id):
            def lookup_by_id(items, item_id):
                if item_id == None:
                    return None
                return [ i for i in items if i.id == item_id ][0]

            def to_dt(delta):
                if delta == None:
                    return None
                return _UNIX_EPOCH + datetime.timedelta(seconds = delta)

            article_type = lookup_by_id(self.article_types, article_type_id)
            category = lookup_by_id(self.categories, category_id)
            venue = lookup_by_id(self.venues, venue_id)

            return Article(title, abstract, content, year, priority,
                           downloaded_as, pdf_file, article_type, category,
                           venue, summary, is_read == 'Y', to_dt(created_at),
                           to_dt(last_updated_at), to_dt(last_indexed_at), id)

        with ResultSet(self.db.cursor(), create_article) as rs:
            rs.init('SELECT * FROM articles', _Articles.table_columns, ())
            return sorted([ a for a in rs ], key = lambda x: x.id)

    def _verify_articles(self, truth, articles):
        self._verify_item_list('articles', truth, articles,
                               self._compute_article_diffs)

    def _compute_article_diffs(self, truth, article):
        return self._compute_item_diffs(truth, article, self.article_fields)

#    def _verify_articles(self, truth, articles):
#        i = 0
#        for (true_article, article) in zip(truth, articles):
#            diffs = self._compute_article_diffs(true_article, article)
#            if diffs:
#                self.fail('Articles at index %d differ: %s' % (i, ', '.join(diffs)))
#        if len(articles) > len(truth):
#            self.fail('There are extra articles: %s' % ', '.join(repr(a) for a in articles[len(truth):]))
#        if len(article) < len(truth):
#            self.fail('There are missing articles: %s' % ', '.join(repr(a) for a in truth[len(articles):]))

#    def _compute_article_diffs(self, truth, article):
#        diffs = [ ]
#        for field in ('id', 'title', 'abstract', 'content', 'year',
#                      'priority', 'downloaded_as', 'pdf_file', 'article_type',
#                      'category', 'venue', 'summary', 'is_read', 'created_at',
#                      'last_updated_at', 'last_indexed_at'):
#            left = getattr(truth, field)
#            right = getattr(article, field)
#            if left != right:
#                diffs.append('%s (%s != %s)' % (field, repr(left), repr(right)))
#        return diffs
    
    @classmethod
    def setUpDatabase(cls, cursor):
        create_id_sequence_table(cursor)
        _Articles.create_table(cursor)

class ArticleTypeTableTests(DatabaseTestCase):
    def setUp(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("INSERT INTO article_types VALUES (1, 'Oral')")
            cursor.execute("INSERT INTO article_types VALUES (2, 'Poster')")
            cursor.execute("INSERT INTO article_Types VALUES (3, 'Spotlight')")

            cursor.execute("INSERT INTO id_sequence VALUES('article_types', 4)")
            self.db.commit()
        finally:
            cursor.close()

        self.table = _ArticleTypes(self.db)
        self.all_types = [ ArticleType('Oral', 1), ArticleType('Poster', 2),
                           ArticleType('Spotlight', 3) ]

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM article_types")
            cursor.execute("DELETE FROM id_sequence")
            self.db.commit()
        finally:
            cursor.close()

    def test_all_article_types(self):
        self._verify_article_types(self.all_types,
                                   sorted(self.table.all, key = lambda x: x.id))

    def _verify_article_types(self, truth, article_types):
        self._verify_enum_constant_list('article_types', truth, article_types)

    @classmethod
    def setUpDatabase(cls, cursor):
        create_id_sequence_table(cursor)
        _ArticleTypes.create_table(cursor)

class CategoryTableTests(DatabaseTestCase):
    def setUp(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("INSERT INTO categories VALUES (1, 'Architecture')")
            cursor.execute("INSERT INTO categories VALUES (2, 'RL')")
            cursor.execute("INSERT INTO categories VALUES (3, 'Clustering')")

            cursor.execute("INSERT INTO id_sequence VALUES('categories', 4)")
            self.db.commit()
        finally:
            cursor.close()

        self.table = _Categories(self.db)
        self.all_categories = [ Category('Architecture', 1),
                                Category('RL', 2),
                                Category('Clustering', 3) ]

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM categories")
            cursor.execute("DELETE FROM id_sequence")
            self.db.commit()
        finally:
            cursor.close()

    def test_all_categories(self):
        self._verify_categories(self.all_categories,
                                sorted(self.table.all, key = lambda x: x.id))

    def _verify_categories(self, truth, categories):
        self._verify_enum_constant_list('categories', truth, categories)

    @classmethod
    def setUpDatabase(cls, cursor):
        create_id_sequence_table(cursor)
        _Categories.create_table(cursor)

class VenueTableTests(DatabaseTestCase):
    def setUp(self):
        def insert_venue(venue):
            cursor.execute("INSERT INTO venues VALUES (:1, :2, :3)",
                           (venue.id, venue.name, venue.abbreviation))

        cursor = self.db.cursor()
        self.all_venues = [ ]
        try:
            venue = Venue('International Conference on Machine Learning',
                          'ICML', 1)
            insert_venue(venue)
            self.all_venues.append(venue)

            venue = Venue('Neural Information Processing Systems', 'NIPS', 2)
            insert_venue(venue)
            self.all_venues.append(venue)

            venue = Venue('Association for Computational Linguistics', 'ACL', 3)
            insert_venue(venue)
            self.all_venues.append(venue)

            cursor.execute("INSERT INTO id_sequence VALUES('venues', 4)")
            self.db.commit()
        finally:
            cursor.close()

        self.table = _Venues(self.db)

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM venues")
            cursor.execute("DELETE FROM id_sequence")
            self.db.commit()
        finally:
            cursor.close()

    def test_all_venues(self):
        self._verify_venues(self.all_venues,
                            sorted(self.table.all, key = lambda x: x.id))

    def _verify_venues(self, truth, venues):
        self._verify_enum_constant_list('venues', truth, venues)

    @classmethod
    def setUpDatabase(cls, cursor):
        create_id_sequence_table(cursor)
        _Venues.create_table(cursor)

class MainDatabaseTests(DatabaseTestCase):
    article_fields = ('id', 'title', 'abstract', 'content', 'year', 'priority',
                      'downloaded_as', 'pdf_file', 'article_type', 'category',
                      'venue', 'summary', 'is_read', 'created_at',
                      'last_updated_at', 'last_indexed_at')

    def setUp(self):
        self.main_db = Database(':memory:', self.db)

        Database._populate_article_types(self.main_db)
        Database._populate_categories(self.main_db)
        Database._populate_venues(self.main_db)

        self.default_article_types = [ ArticleType('Poster', 1),
                                       ArticleType('Oral', 2),
                                       ArticleType('Spotlight', 3),
                                       ArticleType('Long Academic', 4),
                                       ArticleType('Long Industry', 5),
                                       ArticleType('Short Academic', 6),
                                       ArticleType('Short Industry', 7),
                                       ArticleType('Journal', 8) ]
        self.default_categories = [ ]
        self.default_venues = [
            Venue('Association for Computational Linguistics', 'ACL', 1),
            Venue('ACM International Conference on Information and ' +
                  'Knowledge Management', 'CIKM', 2),
            Venue('International Conference on Learning Representations',
                  'ICLR', 3),
            Venue('International Conference on Machine Learning', 'ICML', 4),
            Venue('ACM SIGKDD Conference On Knowledge Discovery and ' + \
                  'Data Mining', 'KDD', 5),
            Venue('Conference of the North American Chapter of the ' + \
                  'Association for Computational Linguistics', 'NAACL', 6),
            Venue('Neural Information Processing Systems', 'NIPS', 7),
            Venue('International ACM SIGIR Conference on Research and ' + \
                  'Development in Information Retrieval', 'SIGIR', 8) ]
        self.default_articles = [ ]

    def tearDown(self):
        cursor = self.db.cursor()
        try:
            cursor.execute("DELETE FROM articles")
            cursor.execute("DELETE FROM venues")
            cursor.execute("DELETE FROM categories")
            cursor.execute("DELETE FROM article_types")
            cursor.execute("DELETE FROM id_sequence")
            self.db.commit()
        finally:
            cursor.close()

    def test_default_article_types(self):
        self._verify_article_types(self.default_article_types,
                                   sorted(self.main_db.article_types.all,
                                          key = lambda x: x.id))

    def test_default_categories(self):
        self._verify_categories(self.default_categories,
                                sorted(self.main_db.categories.all,
                                       key = lambda x: x.id))

    def test_default_venues(self):
        self._verify_venues(self.default_venues,
                            sorted(self.main_db.venues.all,
                                   key = lambda x: x.id))

    def test_add_article(self):
        new_article = Article('My Title', 'My Abstract', 'My Content', 2017,
                              4, 'MyDocument',
                              '/home/tomault/conferences/downloads/MyDoc.pdf',
                              self.default_article_types[3], None,
                              self.default_venues[1], 'My summary')
        now = datetime.datetime.now().replace(microsecond = 0)
        created = self.main_db.articles.add(new_article)
        self.db.commit()

        self.assertEqual(1, created.id)
        self.assertEqual(new_article.title, created.title)
        self.assertEqual(new_article.abstract, created.abstract)
        self.assertEqual(new_article.content, created.content)
        self.assertEqual(new_article.year, created.year)
        self.assertEqual(new_article.priority, created.priority)
        self.assertEqual(new_article.downloaded_as, created.downloaded_as)
        self.assertEqual(new_article.pdf_file, created.pdf_file)
        self.assertEqual(new_article.article_type, created.article_type)
        self.assertIsNone(created.category)
        self.assertEqual(new_article.venue, created.venue)
        self.assertEqual(new_article.summary, created.summary)
        self.assertIsNone(created.last_indexed_at)

        delta = (created.created_at - now).total_seconds()
        self.assertLess(delta, 0.1)

        delta = (created.last_updated_at - now).total_seconds()
        self.assertLess(delta, 0.1)

        self._verify_articles([ created ], self.main_db.articles.all)

    def test_add_article_and_rollback(self):
        new_article = Article('My Title', 'My Abstract', 'My Content', 2017,
                              4, 'MyDocument',
                              '/home/tomault/conferences/downloads/MyDoc.pdf',
                              self.default_article_types[3], None,
                              self.default_venues[1], 'My summary')
        now = datetime.datetime.now().replace(microsecond = 0)
        created = self.main_db.articles.add(new_article)

        self.assertEqual(1, created.id)
        self.assertEqual(new_article.title, created.title)
        self.assertEqual(new_article.abstract, created.abstract)
        self.assertEqual(new_article.content, created.content)
        self.assertEqual(new_article.year, created.year)
        self.assertEqual(new_article.priority, created.priority)
        self.assertEqual(new_article.downloaded_as, created.downloaded_as)
        self.assertEqual(new_article.pdf_file, created.pdf_file)
        self.assertEqual(new_article.article_type, created.article_type)
        self.assertIsNone(created.category)
        self.assertEqual(new_article.venue, created.venue)
        self.assertEqual(new_article.summary, created.summary)
        self.assertIsNone(created.last_indexed_at)

        delta = (created.created_at - now).total_seconds()
        self.assertLess(delta, 0.1)

        delta = (created.last_updated_at - now).total_seconds()
        self.assertLess(delta, 0.1)

        self._verify_articles([ created ], self.main_db.articles.all)

        self.main_db.rollback()
        self._verify_articles([], self.main_db.articles.all)

    def test_count_references_to(self):
        cat1 = self.main_db.categories.add(Category('Architecture'))
        cat2 = self.main_db.categories.add(Category('RL'))

        new_article = Article('My Title', 'My Abstract', 'My Content', 2017,
                              4, 'MyDocument',
                              '/home/tomault/conferences/downloads/MyDoc.pdf',
                              self.default_article_types[3], cat2,
                              self.default_venues[1], 'My summary')
        now = datetime.datetime.now().replace(microsecond = 0)
        created = self.main_db.articles.add(new_article)
        self.main_db.commit()

        art_type = self.default_article_types[1]
        n = self.main_db.article_types.count_references_to(art_type)
        self.assertEqual(0, n)

        art_type = self.default_article_types[3]
        n = self.main_db.article_types.count_references_to(art_type)
        self.assertEqual(1, n)

        n = self.main_db.categories.count_references_to(cat1)
        self.assertEqual(0, n)

        n = self.main_db.categories.count_references_to(cat2)
        self.assertEqual(1, n)

        n = self.main_db.venues.count_references_to(self.default_venues[0])
        self.assertEqual(0, n)

        n = self.main_db.venues.count_references_to(self.default_venues[1])
        self.assertEqual(1, n)

    def _verify_articles(self, truth, articles):
        def compute_article_diffs(left, right):
            return self._compute_item_diffs(left, right, self.article_fields)
        return self._verify_item_list('articles', truth, articles,
                                      compute_article_diffs)
     
    def _verify_categories(self, truth, categories):
        self._verify_enum_constant_list('categories', truth, categories)

    def _verify_article_types(self, truth, article_types):
        self._verify_enum_constant_list('article_types', truth, article_types)

    def _verify_venues(self, truth, venues):
        def compute_venue_diffs(left, right):
            return self._compute_item_diffs(left, right, ('id', 'name',
                                                          'abbreviation'))
        self._verify_item_list('venues', truth, venues, compute_venue_diffs)

    @classmethod
    def setUpDatabase(cls, cursor):
        Database._create_tables(cursor)
        
if __name__ == '__main__':
    unittest.main()
