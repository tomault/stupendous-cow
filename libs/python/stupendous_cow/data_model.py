"""Classes that make up the stupendous_cow data model and their associated
helper functions."""

import collections

_EnumeratedConstantTuple = collections.namedtuple('_EnumeratedConstantTuple',
                                                  ('id', 'name'))
class _EnumeratedConstant(_EnumeratedConstantTuple):
    def __new__(_cls, name, id = None):
        return _EnumeratedConstantTuple.__new__(_cls, id, name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, repr(self.name),
                               repr(self.id))
        
class ArticleType(_EnumeratedConstant):
    pass

class Category(_EnumeratedConstant):
    pass

_Venue = collections.namedtuple('_Venue', ('id', 'name', 'abbreviation'))

class Venue(_Venue):
    def __new__(_cls, name, abbreviation, id = None):
        return _Venue.__new__(_cls, id, name, abbreviation)

    def __str__(self):
        return self.abbreviation

    def __repr__(self):
        return 'Venue(%s, %s, %s)' % (repr(self.name), repr(self.abbreviation),
                                      repr(self.id))

class Article:
    __slots__ = ('id', 'title', 'abstract', 'content', 'year', 'priority',
                 'downloaded_as', 'pdf_file', 'summary', 'is_read',
                 'created_at', 'last_updated_at', 'last_indexed_at',
                 'article_type_id', 'category_id', 'venue_id')

    def __init__(self, title, abstract, content, year, priority, downloaded_as,
                 pdf_file, article_type, category, venue, summary = '',
                 is_read = False, created_at = None, last_updated_at = None,
                 last_indexed_at = None, id = None):
        self.id = id
        self.title = title
        self.abstract = abstract
        self.content = content
        self.year = year
        self.priority = priority
        self.downloaded_as = downloaded_as
        self.pdf_file = pdf_file
        self.article_type = article_type
        self.category = category
        self.venue = venue
        self.summary = summary
        self.is_read = is_read
        self.created_at = created_at
        self.last_updated_at = last_updated_at
        self.last_indexed_at = last_indexed_at

    def update(self, new_article):
        self.title = new_article.title
        self.abstract = new_article.abstract
        self.content = new_article.content
        self.year = new_article.year
        self.priority = new_article.priority
        self.downloaded_as = new_article.downloaded_as
        self.pdf_file = new_article.pdf_file
        self.article_type = new_article.article_type
        self.category = new_article.category
        self.venue = new_article.venue
        self.summary = new_article.summary
        self.is_read = new_article.is_read
        
    def __str__(self):
        return 'Article(%s, %s %s)' % (repr(self.title),
                                       self.venue.abbreviation,
                                       self.year)

    def __repr__(self):
        # TODO: Add some more details...
        return 'Article(%s, %s, %s, id = %s)' % (repr(self.title),
                                                 repr(self.venue.abbreviation),
                                                 repr(self.year),
                                                 repr(self.id))
