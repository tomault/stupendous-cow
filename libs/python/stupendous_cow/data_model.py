"""Classes that make up the stupendous_cow data model and their associated
helper functions."""

class _EnumeratedConstant:
    __slots__ = ('id', 'name')
    
    def __init__(self, name, id = None):
        # int or None; Unique id for article type.  None if not yet assigned
        self.id = id

        # str; Article type name (also unique)
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, repr(self.name),
                               repr(self.id))
        
class ArticleType(_EnumeratedConstant):
    pass

class Category(_EnumeratedConstant):
    pass

class Venue(_EnumeratedConstant):
    __slots__ = ('abbreviation', )
    
    def __init__(self, name, abbreviation, id = None):
        _EnumeratedConstant.__init__(self, name, id)

        # str; Abbreviation for venue's name (e.g. ICML, NIPS, etc.)
        self.abbreviation = abbreviation

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

    def __str__(self):
        return 'Article(%s, %s %s)' % (self.title, self.venue.abbreviation,
                                       self.year)

    def __repr__(self):
        # TODO: Add some more details...
        return 'Article(%s, %s %s)' % (self.title, self.venue.abbreviation,
                                       self.year)
