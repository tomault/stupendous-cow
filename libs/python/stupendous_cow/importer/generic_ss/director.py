from stupendous_cow.importers.generic_ss.configuration import Configuration
from stupendous_cow.data_model import Article, ArticleType, Category, Venue
from stupendous_cow.importers.spreadsheets import SpreadsheetPath
from stupendous_cow.importers.extractors import DOCUMENT_EXTRACTOR_FACTORIES
from stupendous_cow.importers.abstracts import ABSTRACT_READER_FACTORIES

class ArticleBuilder:
    def __init__(self):
        pass

class DocumentPropertyExtractor:
    def __init__(self, source_field, article_property):
        self._get_value = lambda o: getattr(o, source_field)
        self._set_value = getattr(ArticleBuilder, 'set_' + article_property)

    def __call__(self, ss_rows, document, builder):
        self._set_value(builder, self._get_value(document))

DocumentPropertyExtractor.TITLE = DocumentPropertyExtractor('title', 'title')
DocumentPropertyExtractor.ABSTRACT = \
    DocumentPropertyExtractor('abstract', 'abstract')

class SpreadsheetPropertyExtractor:
    def __init__(self, ss_path, article_property, default_value):
        self._set_value = getattr(ArticleBuilder, 'set_' + article_property)
        self._ss_path = ss_path
        self._default_value = default_value

    def __call__(self, ss_rows, document, builder):
        self._set_value(builder, self._get_value(ss_rows))

    def _get_value(self, ss_rows):
        try:
            row = ss_rows[self._ss_path.sheet]
        except KeyError:
            return self._default_value
        return row[self._ss_path.column]

class ConstantPropertyExtractor:
    def __init__(self, article_property, value):
        self._set_value(ArticleBuilder, 'set_' + article_property)
        self.value = value

    def __call__(self, ss_rows, document, builder):
        self._set_value(builder, self.value)

class DocumentGroupProcessor:
    def __init__(self, configuration, db, venue, year, abstracts):
        def set_title_extractor():
            ts = configuration.title_source
            if isinstance(ts, SpreadsheetPath):
                self.sheet_names.add(ts.sheet)
                self._set_title = SpreadsheetPropertyExtractor(ts, 'title', '')
            elif ts == 'extracted':
                self._set_title = DocumentPropertyExtractor.TITLE
            else:
                raise ValueError('Invalid title source')
                
        def set_abstract_extractor():
            ab_src = configuration.abstract_source
            if not ab_src:
                self._set_abstract = ConstantPropertyExtractor('abstract', '')
            elif isinstance(ab_src, SpreadsheetPath):
                self.sheet_names.add(ab_src.sheet)
                self._set_abstract = \
                    SpreadsheetPropertyExtractor(ab_src, 'abstract', '')
            elif ab_src == 'file':
                self._set_abstract = self._set_abstract_from_map
            elif ab_src == 'extracted':
                self._set_abstract = DocumentPropertyExtractor.ABSTRACT
            else:
                raise ValueError('Invalid abstract source')

        def ss_constant_or_optional_extractor(source, property_name,
                                              default_value):
            if not source:
                return ConstantPropertyExtractor(property_name, default_value)
            elif isinstance(source, SpreadsheetPath):
                self.sheet_names.add(source.sheet)
                return SpreadsheetPropertyExtractor(source, property_name,
                                                    default_value)
            else:
                return ConstantPropertyExtractor(property_name, source)
            
        def set_priority_extractor():
            self._set_priority = \
                ss_constant_or_optional_extractor(configuration.priority_source,
                                                  'priority', 0)
        
        def set_article_type_extractor():
            self._set_article_type = \
                ss_constant_or_optional_extractor(\
                    configuration.article_type_source, 'article_type', None)
            
        def set_category_extractor():
            self._set_category = \
                ss_constant_or_optional_extractor(configuration.category_source,
                                                  'category', None)

        def set_summary_extractor():
            content_source = configuration.summary_content_source
            if not content_source:
                self._set_summary = ConstantPropertyExtractor('summary', '')
            elif isinstance(content_source, SpreadsheetPath):
                self.sheet_names.add(content_source.sheet)
                self._set_summary = \
                    SpreadsheetPropertyExtractor(content_source, 'summary', '')
            else:
                raise ValueError('Invalid summary content source')
        
        def set_is_read_extractor():
            self._set_is_read = \
                ss_constant_or_optional_extractor(configuration.is_read_source,
                                                  'is_read', False)

        def set_document_extractor():
            doc_ext = configuration.article_extractor
            if not doc_ext:
                self.document_extractor = None
            else:
                try:
                    fac = DOCUMENT_EXTRACTOR_FACTORIES[doc_ext]
                except KeyError:
                    msg = 'Unknown document extractor "%s"'
                    raise ValueError(msg % doc_ext)

                self.document_extractor = fac()

        self.group_name = configuration.config_name
        self.content_dirs = configuration.content_dirs
        self.downloaded_as_path = configuration.downloaded_as_source
        self.sheet_names = set()

        self.db = db
        self.venue = venue
        self.year = year
        self.abstracts = abstracts

        set_document_extractor()
        set_title_extractor()
        set_abstract_extractor()
        set_priority_extractor()
        set_article_type_extractor()
        set_category_extractor()
        set_summary_extractor()
        set_is_read_extractor()

    def process(self, workbook):
        pass

    def _set_abstract_from_map(self, ss_rows, document, builder):
        key = self._normalize_title(builder.title)
        try:
            builder.set_abstract(self.abstracts[key])
        except KeyError:
            msg = 'Could not find abstract for document [%s]'
            logging.warn(msg % builder.title)       
    
class Director:
    def __init__(self, configuration, db):
        venue = db.venues.with_abbreviation(configuration.venue)
        if not venue:
            raise ValueError('Unknown venue "%s"' % configuration.venue)
        year = configuration.year

        # TODO: Save processors, then create the group processor as
        #       needed in Director.process() and load abstracts beforehand
        self._processors = [ DocumentGroupProcessor(c, db, venue, year) \
                                 for c in configuration.document_groups ]
