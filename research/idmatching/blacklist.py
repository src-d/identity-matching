from pathlib import Path
import gzip
from typing import List

from idmatching import strip_accents

blacklists_dir = Path(__file__).parents[2] / "blacklists"


def _read_blacklist(name: str) -> List[str]:
    with gzip.open(str(blacklists_dir / ("%s.csv.gz" % name)), "rt") as f:
        content = strip_accents(f.read())
        return [" ".join(x.strip().lower().split()) for x in content.splitlines()]


IGNORED_DOMAIN = _read_blacklist("domains")
IGNORED_EMAILS = _read_blacklist("emails")
IGNORED_NAMES = _read_blacklist("names")
IGNORED_TLD = _read_blacklist("top_level_domains")
POPULAR_NAMES = _read_blacklist("popular_names")
POPULAR_EMAILS = _read_blacklist("popular_emails")
