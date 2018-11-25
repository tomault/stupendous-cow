import re

_COMPRESS_WHITESPACE_REX = re.compile('\\s+')
_STRIP_PUNCT_REX = re.compile('[^A-Za-z0-9 \\t\\n]')

def compress_whitespace(t):
    return _COMPRESS_WHITESPACE_REX.sub(' ', t)

def strip_punct(t):
    return _STRIP_PUNCT_REX.sub('', t)
    
def normalize_title(title):
    return compress_whitespace(strip_punct(title.strip())).lower()
