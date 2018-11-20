"""Configuration file parser for the stupendous_cow generic importer."""

from stupendous_cow.importer.spreadsheets import SpreadsheetPath
import codecs
import PyYAML
import re

class Configuration:
    def __init__(self, venue, year, document_groups):
        self.venue = venue
        self.year = year
        self.document_groups = document_groups
        
class DocumentGroupConfiguration:
    def __init__(self, config_name, title_source, abstract_source, content_dirs,
                 priority_source, downloaded_as_source, article_type_source,
                 category_source, summary_title_source, summary_content_source,
                 is_read_source, abstracts_file_reader, abstracts_file_name,
                 article_extractor):
        self.config_name = config_name
        self.title_source = title_source
        self.abstract_source = abstract_source
        self.content_dirs = content_dirs
        self.priority_source = priority_source
        self.downloaded_as_source = downloaded_as_source
        self.article_type_source = article_type_source
        self.category_source = category_source
        self.summary_title_source = summary_title_source
        self.summary_content_source = summary_content_source
        self.is_read_source = is_read_source
        self.abstracts_file_reader = abstracts_file_reader
        self.abstracts_file_name = abstracts_file_name
        self.article_extractor = article_extractor

class ConfigurationFileParser:
    _abstracts_file_spec_rex = re.compile('^([A-Za-z0-9_]+)\\("([^"]+)"\\)$')
    
    def load(self, filename):
        self._filename = filename
        with codecs.open(filename, 'r', 'utf-8') as input:
            parameters = PyYAML.load(input)
        return self._parse_configuration(parameters)

    def load_from_string(self, text, source_name = ''):
        self._filename = source_name
        parameters = PyYAML.load(text)
        return self._parse_configuration(parameters)

    def _parse_configuration(self, parameters)
        venue = self._parse_required_entry(parameters, 'Venue',
                                           ss_path_allowed = False)
        year = self._parse_required_int(parameters, 'Year',
                                        ss_path_allowed = False)
        if year < 1500:
            self._error('Year must be >= 1500')

        document_groups = [ ]
        groups_seen = set()
        for name in parameters:
            if name.startswith('DocumentGroup_'):
                index = self._parse_document_group_index(name)
                if index in groups_seen:
                    self._error('Duplicate document group %d' % index)
                dg = self._parse_document_group(name, index, parameters[name])
                document_groups.append(dg)
                groups_seen.add(index)
            elif not name in ('Venue', 'Year'):
                self._error('Unknown configuration file parameter "%s"' % name)

        return Configuration(venue, year, document_groups)

    def _parse_document_group(self, name, index, parameters):
        parents = (name, )
        title = self._parse_required_entry(parameters, 'Title',
                                           constants_allowed = ('extracted', ),
                                           parents = parents)
        abstract = self._parse_optional_entry(parameters, 'Abstract',
                                              constants_allowed = \
                                                  ('file', 'extracted'),
                                              parents = parents)
        content_dirs = self._parse_required_entry(parameters, 'ContentDir',
                                                  ss_path_allowed = False,
                                                  list_allowed = True,
                                                  parents = parents)
        priority = self._parse_optional_int(parameters, 'Priority',
                                            parents = parents)
        downloaded_as = self._parse_required_entry(parameters, 'DownloadedAs',
                                                   constants_allowed = False,
                                                   parents = parents)
        article_type = self._parse_optional_entry(parameters, 'ArticleType',
                                                  parents = parents)
        category = self._parse_optional_entry(parameters, 'Category',
                                              parents = parents)
        summary_title = self._parse_optional_entry(parameters, 'SummaryTitle',
                                                   constants_allowed = \
                                                       ('extracted', )
                                                   parents = parents)
        summary_text = self._parse_optional_entry(parameters, 'SummaryText',
                                                  constants_allowed = False,
                                                  parents = parents)
        is_read = self._parse_optional_entry(parameters, 'IsRead',
                                             parents = parents)
        (abstracts_file_reader, abstracts_file_name) = \
            self._parse_abstracts_file_spec(parameters, parents)
        extractor = self._parse_optional_entry(parameters, 'extractor',
                                               ss_path_allowed = False,
                                               parents = parents)

        if not summary_title:
            if summary_text:
                msg = 'Required parameter SummaryTitle missing in %s'
                self._error(msg % parents[0])
        else:
            if not summary_text:
                msg = 'Required parameter SummaryText missing in %s'
                self._error(msg % parents[0])
            if summary_title == 'extracted':
                if title != 'extracted':
                    msg = 'In %s, the Title is "extracted," so the ' + \
                          'SummaryTitle must be "extracted" as well.'
                    self._error(msg % parents[0])
            if not isinstance(title, SpreadsheetPath):
                msg = 'In %s, the Title is taken from a spreadsheet, ' + \
                      'so the SummaryTitle must also come from a spreadsheet'
                self._error(msg % parents[0])

        return DocumentGroupConfiguration(name, title, abstract, content_dirs,
                                          priority, downloaded_as, article_type,
                                          category, summary_title,
                                          summary_text, is_read,
                                          abstracts_file_reader,
                                          abstracts_file_name, extractor)

    def _parse_document_group_index(self, name):
        try:
            value = int(name[14:])
        except ValueError:
            self._error('Invalid document group index')

        if value < 1:
            self._error('Invalid document group index')
        return value
    
    def _parse_required_entry(parameters, name, ss_path_allowed = True,
                              constants_allowed = True, list_allowed = False,
                              parents = ()):
        try:
            value = parameters[name].strip()
        except KeyError:
            full_name = self._full_name(name, parents)
            self._error('Required parameter "%s" is missing' % full_name)

        if not value:
            full_name = self._full_name(name, parents)
            
            self._error('Required parameter "%s" is missing' % full_name)
        return self._parse_value(name, value, ss_path_allowed,
                                 constants_allowed, list_allowed, parents)
            
    def _parse_optional_entry(parameters, name, ss_path_allowed = True,
                              constants_allowed = True, list_allowed = False,
                              parents = ()):
        try:
            value = parameters[name].strip()
        except KeyError:
            return None

        if not value:
            return None
        
        return self._parse_value(name, value, ss_path_allowed,
                                 constants_allowed, list_allowed, parents)

    def _parse_required_int(parameters, name, parents = ()):
        value = self._parse_required_value(parameters, name,
                                           ss_path_allowed = False,
                                           constants_allowed = True,
                                           parents = parents)
        try:
            return int(value)
        except KeyError:
            full_name = self._full_name(name, parents)
            self._error('Value for "%s" must be an integer' % full_name)

    def _parse_optional_int(parameters, name, parents = ()):
        value = self._parse_optional_value(parameters, name,
                                           ss_path_allowed = False,
                                           constants_allowed = True,
                                           parents = parents)
        if value == None:
            return value
        
        try:
            return int(value)
        except KeyError:
            full_name = self._full_name(name, parents)
            self._error('Value for "%s" must be an integer' % full_name)

    def _parse_abstracts_file_spec(self, parameters, parents):
        try:
            value = parameters['AbstractsFileReader'].strip()
        except KeyError:
            return (None, None)

        m = self._abstracts_file_spec_rex.match(value)
        if not m:
            msg = 'In %s, the value for AbstractsFileSpec is invalid'
            self._error(msg % parents[-1])
        return (m.group(1), m.group(2))

    def _parse_value(self, name, value, ss_path_allowed = True,
                     constants_allowed = True, list_allowed = False,
                     parents = ()):
        if isinstance(value, list):
            if not list_allowed:
                msg = 'A list is not a legal value for %s'
                self._error(msg % self._full_name(name, parents))
            return value
        elif not (isinstance(value, str) or isinstance(value, unicode)):
            full_name = self._full_name(name, parents)
            msg = 'A %s is not a legal value for %s'
            self._error(msg % (value.__class__.name__, full_name))
        elif value.startswith('@'):
            if not ss_path_allowed:
                msg = 'A spreadsheet path is not a legal value for %s'
                self._error(msg % self._full_name(name, parents))
            
            m = self._ss_path_rex.match(value):
            if not m:
                full_name = self._full_name(name, parents)
                msg = '%s has invalid spreadsheet path [%s]'
                self._error(msg % (full_name, value))
            return SpreadsheetPath(m.group(1), m.group(2))
        elif not constants_allowed:
            msg = 'A constant is not a legal value for %s'
            self._error(msg % self._full_name(name, parents))
        else:
            return value

    def _full_name(self, name, parents):
        if parents:
            return '%s.%s' % ('.'.join(parents), name)
        return name
    
    def _error(self, details):
        if self._filename):
            raise IOError('Error reading %s: %s' % (self._filename, details))
        else:
            raise IOError('Error reading configuration: %s' % details)

                        
            
    
