import sys
import time

import scrapy
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from tqdm import tqdm


print("Reading the existing records...")
skip = set()
try:
    with open("names.old.csv") as fin:
        fin.readline()
        for line in fin:
            skip.add(line.split(",", 1)[0])
    print(len(skip), "records")
except FileNotFoundError:
    pass


class BlogSpider(scrapy.Spider):
    name = "GitHub Users"
    start_urls = ["https://github.com/" + u.strip() for u in tqdm(open("logins.txt"))
                  if u.strip() not in skip]
    print("Will fetch", len(start_urls), "pages")

    custom_settings = {
        "RETRY_TIMES": 10000,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 429, 408],
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 3,
        "AUTOTHROTTLE_DEBUG": True,
    }

    def parse(self, response):
        try:
            yield {"id": response.request.url.rsplit("/", 1)[1],
                   "name": response.css(".vcard-fullname::text").get()}
        except Exception as e:
            yield {"id": response.request.url.rsplit("/", 1)[1],
                   "name": "!error: %s: %s" % (type(e).__name__, e)}
