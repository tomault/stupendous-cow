from stupendous_cow.importer.extractors import *
from stupendous_cow.testing import get_resource_dir, set_resource_dir
import os.path
import unittest

class DefaultPdfExtractorTests(unittest.TestCase):
    def test_read_abstracts(self):
        true_body = 'This is line one.\nThis is line two.\n\n'
        true_doc = ExtractedDocument('', (), '', true_body)

        extractor = DefaultPdfExtractor()
        filename = os.path.join(get_resource_dir(), 'test_pdf.pdf')
        doc = extractor.extract(filename)

        self.assertEqual(true_doc.title, doc.title)
        self.assertEqual(true_doc.authors, doc.authors)
        self.assertEqual(true_doc.abstract, doc.abstract)
        self.assertEqual(true_doc.body, doc.body)        

if __name__ == '__main__':
    set_resource_dir('importer')
    unittest.main()
