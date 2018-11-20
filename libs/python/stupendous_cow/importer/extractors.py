"""Classes to extract article content from PDF files."""

import subprocess

class PdfExtractionError(Exception):
    def __init__(self, details):
        Exception.__init__(self, details)
        self.details = details

class ExtractedDocument:
    def __init__(self, title, authors, abstract, body):
        self.title = title
        self.authors = ()
        self.abstract = abstract
        self.body = body

class DefaultPdfExtractor:
    configuration_name = 'default'

    def extract(filename):
        content = execute_pdftotext(filename)
        return ExtractedDocument(title = '', authors = (), abstract = '',
                                 body = content)

def execute_pdftotext(filename):
    args = [ 'pdftotext', '-nopgbrk', filename, '-' ]
    pdftotext = subprocess.Popen(args, stdout = subprocess.PIPE,
                                 stderr = subprocess.PIPE)
    (output, errors) = subprocess.communicate()
    if pdftotext.returncode:
        msg = 'Extraction from %s failed (code %d): %s'
        raise PdfExtractionError(msg % (filename, pdftotext.returncode, errors))
    return output
