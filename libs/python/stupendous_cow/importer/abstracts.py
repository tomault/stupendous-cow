"""Classes and functions for working with abstract files or obtaining
abstracts from sources other than the PDF file."""

import codecs
import logging
import re

class Abstract:
    def __init__(self, start, title, authors, body):
        self.start = start  # int; Line containing abstract's title
        self.title = title
        self.authors = authors
        self.body = body

class NipsAbstractFileReader:
    configuration_name = 'nips'
    _title_rex = re.compile('^#\\d+\\s+(?:\[[^\\]]*\\])?\\s*')

    def __init__(self, filename):
        self.filename = filename
        self.line = 0

    def __iter__(self):
        with codecs.open(self.filename, 'r', 'utf-8') as input:
            (title, start) = self._find_first_title(input)
            while title:
                (next_title, next_start, body) = self._read_body(input)
                yield Abstract(start, title, (), body)
                title = next_title
                start = next_start

    def _find_first_title(self, input):
        text = self._next_line(input)

        # Skip blank lines at top
        while not text.strip():
            text = self._next_line()

        if not text:
            return (None, self.line)  # File is empty

        return (self._extract_title(text), self.line)

    def _read_body(self, input):
        text = self._next_line(input)
        body = [ ]
        while text and text[0].isspace():
            body.append(text)
            text = self._next_line(input)

        body = ''.join(body[1:]).strip()
        if not text:
            return (None, None, body)
        return (self._extract_title(text), self.line, body)

    def _extract_title(self, text):
        m = self._title_rex.match(text)
        if not m:
            self._error('Abstract title is missing')
            
        title = text[m.end():].strip()
        if not title:
            self._error('Abstract title is missing')
        return title

    def _next_line(self, input):
        text = input.readline()
        if text:
            self.line += 1
        return text

    def _error(self, details):
        raise IOError('Error on line %d of %s: %s' % (self.line, self.filename,
                                                      details))

ABSTRACT_READER_FACTORIES = {
    'nips' : NipsAbstractFileReader
}
