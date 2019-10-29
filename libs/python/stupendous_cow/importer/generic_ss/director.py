from stupendous_cow.data_model import Article, ArticleType, Category, Venue
from stupendous_cow.extractors import ExtractedDocument
from stupendous_cow.util import normalize_title
from stupendous_cow.builders import ArticleBuilder, BooleanPropertyBinder, \
    ConstantPropertyBinder, ConstantPropertyExtractor, \
    DatabasePropertyBinder, DocumentPropertyBinder, IntPropertyBinder, \
    PropertyBinder, PropertyExtractionError, SpreadsheetPropertyBinder, \
    SpreadsheetPropertyExtractor
from stupendous_cow.importers.generic_ss.configuration import Configuration
from stupendous_cow.importers.abstracts import ABSTRACT_READER_FACTORIES
from stupendous_cow.importers.extractors import DOCUMENT_EXTRACTOR_FACTORIES
from stupendous_cow.importers.spreadsheets import SpreadsheetPath
import logging
import os.path

class DocumentGroupProcessor:
    _empty_extracted_document = ExtractedDocument('', (), '', '')
    
    def __init__(self, configuration, db, venue, year, abstracts):
        def set_title_extractor():
            ts = configuration.title_source
            if isinstance(ts, SpreadsheetPath):
                self.sheet_names.add(ts.sheet)
                self._set_title = SpreadsheetPropertyBinder(ts, 'title', '')
            elif ts == 'extracted':
                self._set_title = DocumentPropertyBinder.TITLE
            else:
                raise ValueError('Invalid title source')
                
        def set_abstract_extractor():
            ab_src = configuration.abstract_source
            if not ab_src:
                self._set_abstract = ConstantPropertyBinder('abstract', '')
            elif isinstance(ab_src, SpreadsheetPath):
                self.sheet_names.add(ab_src.sheet)
                self._set_abstract = \
                    SpreadsheetPropertyBinder(ab_src, 'abstract', '')
            elif ab_src == 'file':
                self._set_abstract = self._set_abstract_from_map
            elif ab_src == 'extracted':
                self._set_abstract = DocumentPropertyBinder.ABSTRACT
            else:
                raise ValueError('Invalid abstract source')

        def ss_constant_or_optional_extractor(source, property_name,
                                              default_value):
            if not source:
                return ConstantPropertyExtractor(default_value)
            elif isinstance(source, SpreadsheetPath):
                self.sheet_names.add(source.sheet)
                return SpreadsheetPropertyExtractor(source, default_value)
            else:
                return ConstantPropertyExtractor(source)

        def ss_constant_or_optional_binder(source, property_name,
                                           default_value):
            extractor = ss_constant_or_optional_extractor(source, default_value)
            return PropertyBinder(property_name, extractor)

        def set_priority_extractor():
            base_extractor =
                ss_constant_or_optional_extractor(\
                    configuration.priority_source, 0)
            return IntPropertyBinder('priority', base_extractor)
        
        def set_article_type_extractor():
            base_extractor = \
                ss_constant_or_optional_extractor(\
                    configuration.article_type_source, None)
            self._set_article_type = \
                DatabasePropertyBinder('article_type', db.article_types,
                                       base_extractor, ArticleType)
            
        def set_category_extractor():
            base_extractor = \
                ss_constant_or_optional_extractor(\
                    configuration.category_source, None)
            self._set_category = \
                DatabasePropertyBinder('category', db.categories,
                                       base_extractor, Category)

        def set_summary_extractor():
            content_source = configuration.summary_content_source
            if not content_source:
                self._set_summary = ConstantPropertyBinder('summary', '')
            elif isinstance(content_source, SpreadsheetPath):
                self.sheet_names.add(content_source.sheet)
                self._set_summary = \
                    SpreadsheetPropertyBinder(content_source, 'summary', '')
            else:
                raise ValueError('Invalid summary content source')
        
        def set_is_read_extractor():
            base_extractor = \
                ss_constant_or_optional_extractor(configuration.is_read_source,
                                                  False)
            self._set_is_read = BooleanPropertyBinder('is_read', base_extractor)

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
        self._get_downloaded_as = \
            SpreadsheetPropertyExtractor(self.downloaded_as_path, None)

    def process(self, workbook, db):
        row_iterators = dict((n, iter(workbook[n])) for n in self._sheet_names)
        self._next_row(row_iterators)  # Skip headers

        row_index = 2
        num_articles = 0
        num_failed = 0
        while row_iterators:
            rows = self._next_row(row_iterators)
            logging.debug('Process row %s from sheets %s' % (row_index,
                                                             ', '.join(rows)))
            
            downloaded_as = self._get_downloaded_as(rows, None)
            if not downloaded_as:
                logging.debug('Row %s has no downloaded_as property' % row_index)
                document = self._empty_extracted_document
            else:
                pdf_path = self._find_article_pdf(downloaded_as)
                if pdf_path:
                    document = self._fetch_document(pdf_path)
                else:
                    msg = 'Could not find PDF file for article downloaded ' + \
                          'as %s.pdf'
                    logging.error(msg % downloaded_as)
                    document = self._empty_extracted_document

            logging.debug('Build article')
            builder = ArticleBuilder(db)
            builder.set_ss_info(self.downloaded_as_path.sheet, row_index)
            builder.set_year(self.year)
            builder.set_downloaded_as(downloaded_as)
            builder.set_pdf_file(pdf_path)
            builder.set_venue(self.venue)

            try:
                self._set_title(row_index, rows, document, builder)
                self._set_abstract(row_index, rows, document, builder)
                self._set_priority(row_index, rows, document, builder)
                self._set_article_type(row_index, rows, document, builder)
                self._set_category(row_index, rows, document, builder)
                self._set_summary(row_index, rows, document, builder)
                self._set_is_read(row_index, rows, document, builder)
                article = builder.build()
            except PropertyExtractionError as e:
                msg = 'Could not construct article for %s, row %d (%s)'
                logging.error(msg % (self.downloaded_as_path.sheet,
                                     row_index, e.reason))
                article = None

            if article:
                self._save_article(db, article)
                num_articles += 1
            else:
                num_failed += 1
            row_index += 1

        return (num_articles, num_failed)

    def _next_row(self, row_iterators):
        rows = { }
        for (name, i) in iterators.iteritems():
            try:
                rows[name] = next(i)
            except StopIteration:
                del iterators[name]
        return rows

    def _find_article_pdf(self, downloaded_as):
        if not downloaded_as.endswith('.pdf'):
            downloaded_as += '.pdf'
        for content_dir in self.content_dirs:
            path = os.path.join(content_dir, downloaded_as)
            if os.path.isfile(path):
                return path
        return None

    def _fetch_document(self, path):
        if not self.document_extractor:
            logging.debug('Document not loaded because no extractor is ' + \
                          'configured')
            return _empty_extracted_document
        try:
            logging.debug('Load document from %s' % path)
            return self.document_extractor.extract(path)
        except PdfExtractionError as e:
            logging.error(e.details)
            return _empty_extracted_document
    
    def _set_abstract_from_map(self, ss_rows, document, builder):
        key = normalize_title(builder.title)
        try:
            builder.set_abstract(self.abstracts[key])
        except KeyError:
            msg = 'Could not find abstract for document [%s]'
            logging.warn(msg % builder.title)

    def _save_article(self, db, article):
        nt = normalize_title(article.title)
        logging.debug('Save article with normalized title [%s]' % nt)
        retrieved = db.articles.retrieve(normalized_title = nt,
                                         year = article.year,
                                         venue = article.venue)
        if not retrieved:
            logging.debug('Write new article to database')
            db.articles.add(article)
        elif len(retrieved) == 1:
            logging.debug('Update existing article')
            retrieved[0].update(article)
            db.articles.update(retrieved[0])
        else:
            msg = 'Retrieved %d articles for %s %s with normalized title ' + \
                  '[%s].  This should not have happened.  The article was ' + \
                  'not updated.  Please investigate.'
            logging.error(msg % (article.venue.abbreviation, article.year, nt))

class Director:
    def __init__(self, configuration, db):
        venue = db.venues.with_abbreviation(configuration.venue)
        if not venue:
            raise ValueError('Unknown venue "%s"' % configuration.venue)

        self.venue = venue
        self.year = configuration.year
        self.groups = configuration.groups

    def process(self, workbook, db):
        total_imported = 0
        total_failed = 0
        for configuration in self.groups:
            config_name = configuration.config_name
            logging.info('Importing document group %s' % config_name)
            if configuration.abstract_source == 'file':
                abstract_map = \
                    self._load_abstracts(configuration.abstracts_file_reader
                                         configuration.abstracts_file_name)
            else:
                abstract_map = None
            processor = DocumentGroupProcessor(configuration, db, self.venue,
                                               self.year, abstract_map)
            (num_imported, num_failed) = processor.process(workbook)
            logging.info('Loaded %d articles (%d failed) from %s' % (num_imported, num_failed, config_name))

            total_imported += num_imported
            total_failed += num_failed

        logging.info('Imported %d articles from %d groups with %d failures' % (total_imported, len(self.groups), total_failed))
        return (total_imported, total_failed)

    def _load_abstracts(self, reader_name, file_name):
        reader = ABSTRACT_READER_FACTORIES[reader_name](file_name)
        abstracts = dict((normalize_title(title), body) \
                             for (title, body) in reader)
        logging.info('Loaded abstracts from %s using %s' % (file_name,
                                                            reader_name))
        return abstracts
