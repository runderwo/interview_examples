#!/usr/bin/python3
#
# Extremely stupid parallelized web spider intended to demonstrated threaded concurrency in Python.
# Use at your own risk.

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import requests
import sys


# Doesn't catch single quote attributes, or relative links
ABS_URL_RE = r'href="(http[s]:\/\/[a-zA-Z0-9\.]+\/[^"]*)"'
BREADTH = 5


class Spider():
    def __init__(self, depth=10, breadth=BREADTH):
        self.depth = depth
        self.breadth = breadth

    def scrape(self, url):
        if self.depth < 0:
            return None

        try:
            response = requests.get(url, timeout=15)
        except requests.HTTPError as e:
            print(e)
            return None

        if response.status_code >= 200 and response.status_code < 300:
            self.text = response.text
            return self

        return None


def get_spider(url, depth=10):
    return Spider(depth).scrape(url)


def main():
    url = "http://www.wikipedia.org/"
    if len(sys.argv) > 1:
        url = sys.argv[1]

    visited = {}
    with ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(get_spider, url): url}
        more_results = True
        next_results = {}
        while more_results:
            more_results = False
            for future in as_completed(future_to_url.keys()):
                try:
                    url = future_to_url[future]
                    result = future.result()
                    if result:
                        visited[url] = True
                        print(f"Scraped {url} successfully")
                        m = re.findall(ABS_URL_RE, result.text)
                        for i in range(min(len(m), result.breadth)):
                            if not m[i] in visited:
                                more_results = True
                                print(f"Queuing {m[i]} at depth {result.depth-1}")
                                next_results[executor.submit(get_spider, m[i], result.depth-1)] = m[i]
                except Exception as e:
                    print(f"Error getting future result: {e}")

            if more_results:
                future_to_url = next_results
                next_results = {}

    return 0


if __name__ == "__main__":
    main()
