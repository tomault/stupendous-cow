from stupendous_cow.data_model import Article
import logging

class ArticleBuilder:
    def __init__(self, db):
        self.title = None
        self.abstract = ''
        self.content = ''
        self.year = None
        self.priority = None
        self.downloaded_as = None
        self.pdf_file = None
        self.article_type = db.article_types.with_name('')
        self.category = db.categories.with_name('')
        self.venue = None
        self.summary = ''
        self.is_read = False

        self.ss_sheet_name = None
        self.ss_row_index = None

    def set_ss_info(self, sheet_name, row_index):
        self.ss_sheet_name = sheet_name
        self.ss_row_index = row_index
        return self
    
    def set_title(self, value):
        self.title = value
        return self

    def set_abstract(self, value):
        self.abstract = value
        return self

    def set_content(self, value):
        self.content = value
        return self

    def set_year(self, value):
        self.year = value
        return self
        
    def set_priority(self, value):
        self.priority = value
        return self

    def set_downloaded_as(self, value):
        self.downloaded_as = value
        return self

    def set_pdf_file(self, value):
        self.pdf_file = value
        return self

    def set_article_type(self, value):
        self.article_type = value
        return self

    def set_category(self, value):
        self.category = value
        return self

    def set_venue(self, value):
        self.venue = value
        return self

    def set_summary(self, value):
        self.summary = value
        return self

    def set_is_read(self, value):
        self.is_read = value
        return self

    def build(self):
        missing = [ ]
        for required_field in ('title', 'year', 'venue'):
            if not getattr(self, required_field):
                missing.append(required_field)
        if missing:
            msg = 'Could not create article for %s, row %d becuase ' + \
                  'these required fields are missing: %s'
            logging.error(msg % (self.ss_sheet_name, self.ss_row_index,
                                 ', '.join(missing)))
            return None
        return Article(self.title, self.abstract, self.content, self.year,
                       self.priority, self.downloaded_as, self.pdf_file,
                       self.article_type, self.category, self.venue,
                       self.summary, self.is_read)

class PropertyExtractionError(Exception):
    @property
    def reason(self):
        return self.args[0]

class BooleanPropertyExtractor:
    def __init__(self, base_extractor):
        self._get_base_value = base_extractor

    def __call__(self, ss_rows, document):
        value = self._get_base_value(ss_rows, document)
        if isinstance(value, int) or isinstance(value, long) or \
           isinstance(value, float):
            return bool(value)
        elif isinstance(value, str) or isinstance(value, unicode):
            value = value.strip().lower()
            if value in ('y', 'yes', 'true', 't'):
                return True
            elif value in ('', 'n', 'no', 'false', 'f'):
                return False
            else:
                msg = 'Cannot convert value "%s" to a boolean'
                arg = (value[0:17] + '...') if len(value) > 20 else value
                raise PropertyExtractionError(msg % arg)
        else:
            raise PropertyExtractionError(msg % value.__class__.__name__)
            
class ConstantPropertyExtractor:
    def __init__(self, value):
        self._value = value

    def __call__(self, ss_rows, document):
        return self._value

class DocumentPropertyExtractor:
    def __init__(self, source_field):
        self._get_value = lambda o: getattr(o, source_field)

    def __call__(self, ss_rows, document):
        return self._get_value(document)

DocumentPropertyExtractor.TITLE = DocumentPropertyExtractor('title')
DocumentPropertyExtractor.ABSTRACT = DocumentPropertyExtractor('abstract')

class DatabasePropertyExtractor:
    def __init__(self, table, base_extractor, item_factory):
        self.table = table
        self._get_value_name = base_extractor
        self._create_item = item_factory

    def __call__(self, ss_rows, document):
        name = self._get_value_name(ss_rows, document)
        value = self.table.with_name(name)
        if not value:
            value = self.table.add(self._create_item(name))
        return value

class IntPropertyExtractor:
    def __init__(self, base_extractor):
        self._get_base_value = base_extractor

    def __call__(self, ss_rows, document):
        value = self._get_base_value(ss_rows, document)
        if isinstance(value, int) or isinstance(value, long):
            return value
        elif isinstance(value, str) or isinstance(value, unicode):
            try:
                return int(value)
            except ValueError:
                msg = 'Cannot convert "%s" to an integer'
                arg = (value[0:17] + '...') if len(value) > 20 else value
                raise PropertyExtractionError(msg % arg)
        elif isinstance(value, float):
            if int(value) == value:
                return int(value)
            else:
                msg = 'Cannot convert floating-point number to integer - ' + \
                      'it would lose precision'
                raise PropertyExtractionError(msg)
        else:
            msg = 'Cannot convert value of type %s to an integer'
            raise PropertyExtractionError(msg % value.__class__.__name__)

class SpreadsheetPropertyExtractor:
    def __init__(self, ss_path, default_value):
        self._ss_path = ss_path
        self._default_value = default_value

    def __call__(self, ss_rows, document):
        try:
            row = ss_rows[self._ss_path.sheet]
        except KeyError:
            return self._default_value
        return row[self._ss_path.column]
                
class PropertyBinder:
    def __init__(self, article_property, extractor):
        self._property_name = article_property
        self._set_value = getattr(ArticleBuilder, 'set_' + article_property)
        self._get_value = extractor

    def __call__(self, ss_rows, document, builder):
        try:
            self._set_value(builder, self._get_value(ss_rows, document))
        except PropertyExtractionError as e:
            msg = 'Failed to extract value for %s: %s'
            raise PropertyExtractionError(msg % (self._property_name, e.reason))

class BooleanPropertyBinder(PropertyBinder):
    def __init__(self, article_property, base_extractor):
        PropertyBinder.__init__(self, article_property,
                                BooleanPropertyExtractor(base_extractor))

class ConstantPropertyBinder(PropertyBinder):
    def __init__(self, article_property, value):
        PropertyBinder.__init__(self, article_property,
                                ConstantPropertyExtractor(value))

class DocumentPropertyBinder(PropertyBinder):
    def __init__(self, article_property, source_field):
        PropertyBinder.__init__(self, article_property,
                                DocumentPropertyExtractor(source_field))

DocumentPropertyBinder.TITLE = DocumentPropertyBinder('title', 'title')
DocumentPropertyBinder.ABSTRACT = DocumentPropertyBinder('abstract', 'abstract')

class DatabasePropertyBinder(PropertyBinder):
    def __init__(self, article_property, table, base_extractor, item_factory):
        PropertyBinder.__init__(self, article_property,
                                DatabasePropertyExtractor(table,
                                                          base_extractor,
                                                          item_factory))

class IntPropertyBinder(PropertyBinder):
    def __init__(self, article_property, base_extractor):
        PropertyBinder.__init__(self, article_property,
                                IntPropertyExtractor(base_extractor))

class SpreadsheetPropertyBinder(PropertyBinder):
    def __init__(self, ss_path, article_property, default_value):
        PropertyBinder.__init__(self, article_property,
                                SpreadsheetPropertyExtractor(ss_path,
                                                             default_value))
