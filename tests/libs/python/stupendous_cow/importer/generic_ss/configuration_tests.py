from stupendous_cow.importer.generic_ss.configuration import *
from stupendous_cow.importer.spreadsheets import SpreadsheetPath
import yaml
import cStringIO
import unittest

class ConfigurationParserTests(unittest.TestCase):
    def setUp(self):
        self.parser = ConfigurationFileParser()
        
    def test_parse_config(self):
        dg_1 = self._create_document_group()
        dg_2 = self._create_document_group(\
            title = 'extracted', abstract = 'extracted',
            content_dirs = [ '/home/cows/moo', '/home/peguins/wark' ],
            priority = 4,
            downloaded_as = SpreadsheetPath('NewPapers', 'DownloadedAs'),
            article_type = 'Long Paper', category = 'Deep Learning',
            summary_title = 'extracted',
            summary_text = SpreadsheetPath('NewSummaries', 'SUMMARY_TITLE'),
            is_read = 'False',
            abstracts_file_reader = 'nips',
            abstracts_file_name = 'nips2018_part2.txt',
            article_extractor = 'special_pdf')
        true_config = self._create_config_map(document_groups = [ dg_1, dg_2 ])

        config = self.parser.load(stream = self._create_stream(true_config))
        self._verify_config(true_config, config)
    
    def _create_config_map(self, venue = 'NIPS', year = 2018,
                           document_groups = ()):
        config_map = { 'Venue' : venue, 'Year' : year }
        for (n, dg) in enumerate(document_groups):
            config_map['DocumentGroup_%d' % (n + 1)] = dg
        return config_map

    def _create_document_group(self, title = SpreadsheetPath('Papers', 'TITLE'),
                               abstract = 'file',
                               content_dirs = [ '/home/tomault/conferences' ],
                               priority = SpreadsheetPath('Papers', 'PRIORITY'),
                               downloaded_as = SpreadsheetPath('Papers',
                                                               'DOWNLOADED_AS'),
                               article_type = SpreadsheetPath('Papers', 'TYPE'),
                               category = SpreadsheetPath('Papers', 'AREA'),
                               summary_title = SpreadsheetPath('Summaries',
                                                               'TITLE'),
                               summary_text = SpreadsheetPath('Summaries',
                                                              'SUMMARY'),
                               is_read = SpreadsheetPath('Papers', 'IS_READ'),
                               abstracts_file_reader = 'nips',
                               abstracts_file_name = 'nips2018_details.txt',
                               article_extractor = 'default_pdf'):
        return { 'Title' : title, 'Abstract' : abstract,
                 'ContentDir' : content_dirs, 'Priority' : priority,
                 'DownloadedAs' : downloaded_as, 'ArticleType' : article_type,
                 'Category' : category, 'SummaryTitle' : summary_title,
                 'SummaryText' : summary_text, 'IsRead' : is_read,
                 'AbstractsFileReader' : abstracts_file_reader,
                 'AbstractsFileName' : abstracts_file_name,
                 'Extractor' : article_extractor }
    
    def _create_stream(self, config):
        def s(value):
            if isinstance(value, SpreadsheetPath):
                return '@%s[%s]' % (value.sheet, value.column)
            return value
        tmp = { 'Venue' : config['Venue'], 'Year' : config['Year'] }
        for dg_name in (k for k in config if k.startswith('DocumentGroup_')):
            dg = config[dg_name]
            abstract_reader = '%s("%s")' % (dg['AbstractsFileReader'],
                                            dg['AbstractsFileName'])
            serialized = { 'Title' : s(dg['Title']),
                           'Abstract' : s(dg['Abstract']),
                           'ContentDir' : dg['ContentDir'],
                           'Priority' : s(dg['Priority']),
                           'DownloadedAs' : s(dg['DownloadedAs']),
                           'ArticleType' : s(dg['ArticleType']),
                           'Category' : s(dg['Category']),
                           'SummaryTitle' : s(dg['SummaryTitle']),
                           'SummaryText' : s(dg['SummaryText']),
                           'IsRead' : s(dg['IsRead']),
                           'AbstractsFileReader' : abstract_reader,
                           'Extractor' : dg['Extractor'] }
            tmp[dg_name] = serialized

        return cStringIO.StringIO(yaml.dump(tmp, default_flow_style = False))

    def _verify_config(self, true_config, config):
        self.assertEqual(true_config['Venue'], config.venue)
        self.assertEqual(true_config['Year'], config.year)
        true_dgs = sorted([ x for x in true_config \
                              if x.startswith('DocumentGroup_') ])
        for (true_dg_name, dg) in zip(true_dgs, config.document_groups):
            true_dg = true_config[true_dg_name]
            self._verify_document_group(true_dg_name, true_dg, dg)

    def _verify_document_group(self, true_name, true_dg, dg):
        self.assertEqual(true_name, dg.config_name)
        self.assertEqual(true_dg['Title'], dg.title_source)
        self.assertEqual(true_dg['Abstract'], dg.abstract_source)
        self.assertEqual(true_dg['ContentDir'], dg.content_dirs)
        self.assertEqual(true_dg['Priority'], dg.priority_source)
        self.assertEqual(true_dg['DownloadedAs'], dg.downloaded_as_source)
        self.assertEqual(true_dg['ArticleType'], dg.article_type_source)
        self.assertEqual(true_dg['Category'], dg.category_source)
        self.assertEqual(true_dg['SummaryTitle'], dg.summary_title_source)
        self.assertEqual(true_dg['SummaryText'], dg.summary_content_source)
        self.assertEqual(true_dg['IsRead'], dg.is_read_source)
        self.assertEqual(true_dg['AbstractsFileReader'],
                         dg.abstracts_file_reader)
        self.assertEqual(true_dg['AbstractsFileName'], dg.abstracts_file_name)
        self.assertEqual(true_dg['Extractor'], dg.article_extractor)

if __name__ == '__main__':
    unittest.main()
