from stupendous_cow.importer.builders import *
from stupendous_cow.importer.extractors import ExtractedDocument
from stupendous_cow.importer.spreadsheets import SpreadsheetPath
from stupendous_cow.data_model import ArticleType, Category, Venue
import logging
import unittest

class MockTable:
    def __init__(self, items, item_factory):
        self._items = list(items)
        self._id_to_item = dict((i.id, i) for i in items)
        self._name_to_item = dict((i.name, i) for i in items)
        self._next_id = max(self._id_to_item) + 1
        self._create_item = item_factory

    def with_id(self, id):
        return self._id_to_item.get(id, None)

    def with_name(self, name):
        return self._name_to_item.get(name, None)

    def add(self, item):
        new_item = self._create_item(item.name, self._next_id)
        self._next_id += 1
        
        self._items.append(new_item)
        self._id_to_item[new_item.id] = new_item
        self._name_to_item[new_item.name] = new_item
        return new_item

    def __getitem__(self, i):
        return self._items[i]

class MockDatabase:
    def __init__(self):
        self.article_types = MockTable((ArticleType('', 1),
                                        ArticleType('Oral', 2)), ArticleType)

        self.categories = MockTable((Category('', 1),
                                     Category('Architecture', 2)), Category)
        self.venues = MockTable((Venue('', '', 1), Venue('ICML', 'ICML', 2)),
                                Venue)

class MockRow:
    def __init__(self, values):
        self.values = values

    def __len__(self):
        return len(self.values)

    def __getitem__(self, i):
        return self.values[i]
    
class ArticleBuilderTests(unittest.TestCase):
    def setUp(self):
        self.db = MockDatabase()

    def test_build_article(self):
        builder = ArticleBuilder(self.db)
        builder.set_ss_info('alpha', 2)
        builder.set_title('Cows Are Cool')
        builder.set_abstract('We analyze why cows are so cool.')
        builder.set_content('Cows are really cool.  Everyone thinks so.')
        builder.set_year(2018)
        builder.set_priority(4)
        builder.set_downloaded_as('CowsAreCool')
        builder.set_pdf_file('/home/user/conferences/CowsAreCool.pdf')
        builder.set_article_type(self.db.article_types.with_name('Oral'))
        builder.set_category(self.db.categories.with_name('Architecture'))
        builder.set_venue(self.db.venues.with_name('ICML'))
        builder.set_summary('The authors investigate why cows are cool.')
        builder.set_is_read(True)
        article = builder.build()

        self.assertEqual('alpha', builder.ss_sheet_name)
        self.assertEqual(2, builder.ss_row_index)

        self.assertIsNotNone(article)
        self.assertEqual('Cows Are Cool', article.title)
        self.assertEqual('We analyze why cows are so cool.', article.abstract)
        self.assertEqual('Cows are really cool.  Everyone thinks so.',
                         article.content)
        self.assertEqual(2018, article.year)
        self.assertEqual(4, article.priority)
        self.assertEqual('CowsAreCool', article.downloaded_as)
        self.assertEqual('/home/user/conferences/CowsAreCool.pdf',
                         article.pdf_file)
        self.assertEqual(self.db.article_types[1], article.article_type)
        self.assertEqual(self.db.categories[1], article.category)
        self.assertEqual(self.db.venues[1], article.venue)
        self.assertEqual('The authors investigate why cows are cool.',
                         article.summary)
        self.assertTrue(article.is_read)
        self.assertIsNone(article.id)
        self.assertIsNone(article.created_at)
        self.assertIsNone(article.last_updated_at)
        self.assertIsNone(article.last_indexed_at)

    def test_build_article_without_title(self):
        builder = ArticleBuilder(self.db)
        builder.set_ss_info('alpha', 2)
        builder.set_abstract('We analyze why cows are so cool.')
        builder.set_content('Cows are really cool.  Everyone thinks so.')
        builder.set_year(2018)
        builder.set_priority(4)
        builder.set_downloaded_as('CowsAreCool')
        builder.set_pdf_file('/home/user/conferences/CowsAreCool.pdf')
        builder.set_article_type(self.db.article_types.with_name('Oral'))
        builder.set_category(self.db.categories.with_name('Architecture'))
        builder.set_venue(self.db.venues.with_name('ICML'))
        builder.set_summary('The authors investigate why cows are cool.')
        builder.set_is_read(True)
        article = builder.build()

        self.assertIsNone(article)

    def test_build_article_without_year(self):
        builder = ArticleBuilder(self.db)
        builder.set_ss_info('alpha', 2)
        builder.set_title('Cows Are Cool')
        builder.set_abstract('We analyze why cows are so cool.')
        builder.set_content('Cows are really cool.  Everyone thinks so.')
        builder.set_priority(4)
        builder.set_downloaded_as('CowsAreCool')
        builder.set_pdf_file('/home/user/conferences/CowsAreCool.pdf')
        builder.set_article_type(self.db.article_types.with_name('Oral'))
        builder.set_category(self.db.categories.with_name('Architecture'))
        builder.set_venue(self.db.venues.with_name('ICML'))
        builder.set_summary('The authors investigate why cows are cool.')
        builder.set_is_read(True)
        article = builder.build()

        self.assertIsNone(article)

    def test_build_article_without_venue(self):
        builder = ArticleBuilder(self.db)
        builder.set_ss_info('alpha', 2)
        builder.set_title('Cows Are Cool')
        builder.set_abstract('We analyze why cows are so cool.')
        builder.set_content('Cows are really cool.  Everyone thinks so.')
        builder.set_year(2018)
        builder.set_priority(4)
        builder.set_downloaded_as('CowsAreCool')
        builder.set_pdf_file('/home/user/conferences/CowsAreCool.pdf')
        builder.set_article_type(self.db.article_types.with_name('Oral'))
        builder.set_category(self.db.categories.with_name('Architecture'))
        builder.set_summary('The authors investigate why cows are cool.')
        builder.set_is_read(True)
        article = builder.build()

        self.assertIsNone(article)

class PropertyExtractorTests(unittest.TestCase):
    def setUp(self):
        self.ss_rows = { 'alpha' : MockRow(('a', 'b', 'c', 'd')),
                         'beta' : MockRow(('qqq', 'zzz', 'aaa')) }
        self.document = \
            ExtractedDocument('Cows Are Cool', (),
                              'The authors investigate why cows are cool.',
                              'Cows are really cool.  Everyone knows this.')

    def test_constant_extractor(self):
        extractor = ConstantPropertyExtractor('cow')
        self.assertEqual('cow', extractor(self.ss_rows, self.document))

    def test_boolean_property_extractor_with_numbers(self):
        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(1))
        self.assertTrue(extractor(self.ss_rows, self.document))

        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(0))
        self.assertFalse(extractor(self.ss_rows, self.document))
        
        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(long(1)))
        self.assertTrue(extractor(self.ss_rows, self.document))

        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(long(0)))
        self.assertFalse(extractor(self.ss_rows, self.document))

        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(1.0))
        self.assertTrue(extractor(self.ss_rows, self.document))

        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(0.0))
        self.assertFalse(extractor(self.ss_rows, self.document))

    def test_boolean_property_extractor_with_string(self):
        for v in ('y', 'yes', 'true', 't'):
            extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(v))
            self.assertTrue(extractor(self.ss_rows, self.document))

            extractor = BooleanPropertyExtractor(\
                            ConstantPropertyExtractor(v.upper()))
            self.assertTrue(extractor(self.ss_rows, self.document))
            
        for v in ('', 'n', 'no', 'false', 'f'):
            extractor = BooleanPropertyExtractor(ConstantPropertyExtractor(v))
            self.assertFalse(extractor(self.ss_rows, self.document))

            extractor = BooleanPropertyExtractor(\
                            ConstantPropertyExtractor(v.upper()))
            self.assertFalse(extractor(self.ss_rows, self.document))

        extractor = BooleanPropertyExtractor(ConstantPropertyExtractor('bad'))
        with self.assertRaises(PropertyExtractionError):
            extractor(self.ss_rows, self.document)

    def test_document_title_extractor(self):
        self.assertEqual(self.document.title,
                         DocumentPropertyExtractor.TITLE(self.ss_rows,
                                                         self.document))

    def test_document_abstract_extractor(self):
        self.assertEqual(self.document.abstract,
                         DocumentPropertyExtractor.ABSTRACT(self.ss_rows,
                                                            self.document))

    def test_database_property_extractor(self):
        db = MockDatabase()
        base_extractor = ConstantPropertyExtractor('Architecture')
        extractor = DatabasePropertyExtractor(db.categories, base_extractor,
                                              Category)
        self.assertEqual(db.categories[1], extractor(self.ss_rows,
                                                     self.document))

    def test_database_property_extractor_new_item(self):
        db = MockDatabase()
        base_extractor = ConstantPropertyExtractor('RL')
        extractor = DatabasePropertyExtractor(db.categories, base_extractor,
                                              Category)
        category = extractor(self.ss_rows, self.document)
        self.assertEqual(3, len(db.categories._items))
        self.assertEqual(3, category.id)
        self.assertEqual('RL', category.name)
        self.assertEqual(db.categories[2], category)

    def test_extract_int_from_int_or_long(self):
        extractor = IntPropertyExtractor(ConstantPropertyExtractor(5))
        self.assertEqual(5, extractor(self.ss_rows, self.document))

        extractor = IntPropertyExtractor(ConstantPropertyExtractor(long(3)))
        self.assertEqual(3, extractor(self.ss_rows, self.document))

    def test_extract_int_from_str_or_unicode(self):
        extractor = IntPropertyExtractor(ConstantPropertyExtractor('100'))
        self.assertEqual(100, extractor(self.ss_rows, self.document))

        extractor = IntPropertyExtractor(ConstantPropertyExtractor(u'50'))
        self.assertEqual(50, extractor(self.ss_rows, self.document))

        extractor = IntPropertyExtractor(ConstantPropertyExtractor('bad'))
        with self.assertRaises(PropertyExtractionError):
            extractor(self.ss_rows, self.document)

    def test_extract_int_from_float(self):
        extractor = IntPropertyExtractor(ConstantPropertyExtractor(1.0))
        self.assertEqual(1, extractor(self.ss_rows, self.document))

        extractor = IntPropertyExtractor(ConstantPropertyExtractor(1.5))
        with self.assertRaises(PropertyExtractionError):
            extractor(self.ss_rows, self.document)

    def test_extract_int_from_invalid_type(self):
        extractor = IntPropertyExtractor(ConstantPropertyExtractor('bad'))
        with self.assertRaises(PropertyExtractionError):
            extractor(self.ss_rows, self.document)

    def test_spreadsheet_property_extractor(self):
        path = SpreadsheetPath('alpha', 1)
        extractor = SpreadsheetPropertyExtractor(path, 'default')
        self.assertEqual('b', extractor(self.ss_rows, self.document))

        path = SpreadsheetPath('bad', 1)
        extractor = SpreadsheetPropertyExtractor(path, 'default')
        self.assertEqual('default', extractor(self.ss_rows, self.document))
                         
class PropertyBinderTests(unittest.TestCase):
    def setUp(self):
        self.ss_rows = { 'alpha' : MockRow(('a', 'b', 'c', 'd')),
                         'beta' : MockRow(('qqq', 'zzz', 'aaa')) }
        self.document = \
            ExtractedDocument('Cows Are Cool', (),
                              'The authors investigate why cows are cool.',
                              'Cows are really cool.  Everyone knows this.')

        self.db = MockDatabase()

    def test_bind_boolean(self):
        binder = BooleanPropertyBinder('is_read',
                                       ConstantPropertyExtractor('true'))
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertTrue(builder.is_read)

    def test_bind_invalid_boolean(self):
        binder = BooleanPropertyBinder('is_read',
                                       ConstantPropertyExtractor('bad'))
        builder = ArticleBuilder(self.db)
        with self.assertRaises(PropertyExtractionError):
            binder(self.ss_rows, self.document, builder)

    def test_bind_constant_value(self):
        binder = ConstantPropertyBinder('priority', 2)
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertEqual(2, builder.priority)

    def test_bind_document_title(self):
        binder = DocumentPropertyBinder.TITLE
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertEqual(self.document.title, builder.title)

    def test_bind_document_abstract(self):
        binder = DocumentPropertyBinder.ABSTRACT
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertEqual(self.document.abstract, builder.abstract)

    def test_bind_database_category(self):
        base_extractor = ConstantPropertyExtractor('Architecture')
        binder = DatabasePropertyBinder('category', self.db.categories,
                                        base_extractor, Category)
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertEqual(self.db.categories[1], builder.category)

    def test_bind_int(self):
        base_extractor = ConstantPropertyExtractor(1)
        binder = IntPropertyBinder('priority', base_extractor)
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertEqual(1, builder.priority)

    def test_bind_from_spreadsheet(self):
        path = SpreadsheetPath('beta', 0)
        binder = SpreadsheetPropertyBinder(path, 'summary', 'default')
        builder = ArticleBuilder(self.db)
        binder(self.ss_rows, self.document, builder)
        self.assertEqual('qqq', builder.summary)
        
if __name__ == '__main__':
    logging.basicConfig(level = logging.CRITICAL)
    unittest.main()
