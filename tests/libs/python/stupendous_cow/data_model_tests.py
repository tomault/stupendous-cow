"""Unit tests for stupendous_cow.data_model"""
from stupendous_cow.data_model import *
import datetime
import unittest

class ArticleTypeTests(unittest.TestCase):
    def test_create_article_type(self):
        article_type = ArticleType('Oral', 1)
        self.assertEqual('Oral', article_type.name)
        self.assertEqual(1, article_type.id)
        self.assertEqual('Oral', str(article_type))
        self.assertEqual("ArticleType('Oral', 1)", repr(article_type))

        article_type = ArticleType('Poster')
        self.assertEqual('Poster', article_type.name)
        self.assertIsNone(article_type.id)

class CategoryTests(unittest.TestCase):
    def test_create_category(self):
        category = Category('RL', 1)
        self.assertEqual('RL', category.name)
        self.assertEqual(1, category.id)
        self.assertEqual('RL', str(category))
        self.assertEqual("Category('RL', 1)", repr(category))

        category = Category('Reading Comprehension')
        self.assertEqual('Reading Comprehension', category.name)
        self.assertIsNone(category.id)

class VenueTests(unittest.TestCase):
    def test_create_venue(self):
        venue = Venue('Neural Information Processing Systems', 'NIPS', 1)
        self.assertEqual('Neural Information Processing Systems', venue.name)
        self.assertEqual('NIPS', venue.abbreviation)
        self.assertEqual(1, venue.id)
        self.assertEqual('NIPS', str(venue))
        self.assertEqual(
            "Venue('Neural Information Processing Systems', 'NIPS', 1)",
            repr(venue))

        venue = Venue('Association for Computational Linguistics', 'ACL')
        self.assertEqual('Association for Computational Linguistics',
                         venue.name)
        self.assertEqual('ACL', venue.abbreviation)
        self.assertIsNone(venue.id)

class ArticleTests(unittest.TestCase):
    def test_create_article(self):
        article_type = ArticleType('Oral', 1)
        category = Category('RL', 1)
        venue = Venue('Neural Information Processing Systems', 'NIPS', 1)
        creation_date = datetime.datetime(2018, 9, 23)
        update_date = datetime.datetime(2018, 9, 28)
        indexed_at = datetime.datetime(2018, 9, 24)
        
        article = Article('Cows are cool', 'Moo', 'Cows are really cool.',
                          2018, 9, 'CowsAreCool',
                          '/home/tomault/conferences/NIPS/2018/CowsAreCool.pdf',
                          article_type, category, venue, 'All about cows.',
                          True, creation_date, update_date, indexed_at, 10)

        self.assertEqual(10, article.id)
        self.assertEqual('Cows are cool', article.title)
        self.assertEqual('Moo', article.abstract)
        self.assertEqual('Cows are really cool.', article.content)
        self.assertEqual(2018, article.year)
        self.assertEqual(9, article.priority)
        self.assertEqual('CowsAreCool', article.downloaded_as)
        self.assertEqual('/home/tomault/conferences/NIPS/2018/CowsAreCool.pdf',
                         article.pdf_file)
        self.assertEqual(article_type, article.article_type)
        self.assertEqual(category, article.category)
        self.assertEqual(venue, article.venue)
        self.assertEqual('All about cows.', article.summary)
        self.assertTrue(article.is_read)
        self.assertEqual(creation_date, article.created_at)
        self.assertEqual(update_date, article.last_updated_at)
        self.assertEqual(indexed_at, article.last_indexed_at)

        self.assertEqual("Article('Cows are cool', NIPS 2018)", str(article))
        self.assertEqual("Article('Cows are cool', 'NIPS', 2018, id = 10)",
                         repr(article))
                          

if __name__ == '__main__':
    unittest.main()

        
        
