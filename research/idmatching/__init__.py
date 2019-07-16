import unicodedata


def strip_accents(s):
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return unicodedata.normalize('NFC', s)
