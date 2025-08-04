import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import time
from pymongo import MongoClient
from dotenv import load_dotenv
import os
class SiteCrawler:
    def __init__(self, base_url, max_pages=10, concurrency=5):
        self.base_url = self.normalize_url(base_url)
        self.domain = urlparse(self.base_url).netloc
        self.merchant_path = urlparse(self.base_url).path.rstrip('/')
        self.visited_urls = set()
        self.url_queue = [self.base_url]
        self.pages = []
        self.max_pages = max_pages
        self.successful = 0
        self.failed = 0
        self.total_time = 0
        self.concurrency = concurrency

    def normalize_url(self, url):
        if not url.startswith("http"):
            return "https://" + url
        return url

    def is_internal_url(self, url):
        try:
            parsed = urlparse(url)
            return (
                parsed.netloc == self.domain and
                parsed.path.startswith(self.merchant_path)
            )
        except:
            return False

    def extract_links(self, soup):
        internal = []
        external = []
        key_pages = []
        key_patterns = [
            'about', 'contact', 'privacy', 'terms', 'refund', 'shipping',
            'careers', 'faq', 'support', 'return', 'help', 'policy'
        ]

        for a in soup.find_all('a', href=True):
            href = a['href']
            link_text = a.get_text(strip=True).lower()
            resolved = urljoin(self.base_url, href)
            if self.is_internal_url(resolved):
                internal.append(resolved)
                if any(p in resolved.lower() or p in link_text for p in key_patterns):
                    key_pages.append(resolved)
            else:
                external.append(resolved)

        return internal, external, key_pages

    def detect_page_type(self, url, soup, content):
        u = url.lower()
        c = content.lower()
        t = (soup.title.string if soup.title else "").lower()

        if any(x in u for x in ['/about', 'about-us']): return 'About'
        if any(x in u for x in ['/contact', 'contact-us']): return 'Contact'
        if any(x in u for x in ['/product', '/shop', '/store']): return 'Product'
        if any(x in u for x in ['/service', '/services']): return 'Service'
        if any(x in u for x in ['/terms', '/tos']): return 'Terms'
        if any(x in u for x in ['/privacy', '/policy']): return 'Privacy'

        if 'about us' in c or 'our story' in c or 'our mission' in c or 'about' in t: return 'About'
        if 'contact us' in c or 'get in touch' in c or 'contact' in t: return 'Contact'
        if 'terms of service' in c or 'terms and conditions' in c or 'terms' in t: return 'Terms'
        if 'privacy policy' in c or 'privacy' in t: return 'Privacy'

        if any(x in c for x in ['add to cart', 'buy now', 'product', 'price', 'shop']): return 'Product'
        if any(x in c for x in ['our services', 'consulting', 'solutions']): return 'Service'

        return 'General'

    def count_products(self, soup):
        selectors = [
            '.product', '.product-item', '.product-card', '.shop-item',
            '.store-item', '[data-product]', '.woocommerce-loop-product__title',
            '.product-title', '.item-title'
        ]
        max_count = 0
        for sel in selectors:
            elements = soup.select(sel)
            if len(elements) > max_count:
                max_count = len(elements)

        if max_count == 0:
            text = soup.get_text()
            pattern = re.compile(r'(?=\S*[A-Za-z])(?=\S*\d)[A-Za-z\d\-_]{6,}')
            matches = pattern.findall(text)
            unique_matches = set(matches)
            return len(unique_matches)

        return max_count

    def analyze_metadata(self, url, content):
        u = url.lower()
        c = content.lower()
        return {
            'hasAboutUs': 'about' in u or 'about us' in c,
            'hasTerms': 'terms' in u or 'terms of service' in c,
            'hasPrivacy': 'privacy' in u or 'privacy policy' in c,
            'hasContact': 'contact' in u or 'contact us' in c,
            'hasServices': 'services' in u or 'our services' in c,
            'hasProducts': any(x in c for x in ['product', 'shop', 'store']),
        }

    async def crawl_page(self, session, url):
        try:
            async with session.get(url, timeout=10) as res:
                if res.status != 200:
                    self.failed += 1
                    return None
                html = await res.text()
                soup = BeautifulSoup(html, 'html.parser')
                for tag in soup(['script', 'style', 'noscript']):
                    tag.decompose()
                content = soup.get_text(separator=' ', strip=True)
                title = soup.title.string.strip() if soup.title else 'Untitled'
                page_type = self.detect_page_type(url, soup, content)
                product_count = self.count_products(soup)
                internal, external, key_pages = self.extract_links(soup)
                metadata = self.analyze_metadata(url, content)

                for link in key_pages:
                    if link not in self.visited_urls and link not in self.url_queue:
                        self.url_queue.insert(0, link)
                for link in internal:
                    if link not in self.visited_urls and link not in self.url_queue:
                        self.url_queue.append(link)

                self.successful += 1

                return {
                    "url": url,
                    "title": title,
                    "pageType": page_type,
                    "status": res.status,
                    "contentLength": len(content),
                    "hasProducts": product_count > 0,
                    "productCount": product_count,
                    "links": {
                        "internal": len(internal),
                        "external": len(external)
                    },
                    "metadata": metadata
                }

        except Exception as e:
            print(f"Failed: {url} ({e})")
            self.failed += 1
            return None

    def generate_report(self):
        pages_by_type = defaultdict(int)
        total_skus = 0
        internal_links = 0
        external_links = 0

        for p in self.pages:
            pages_by_type[p['pageType']] += 1
            total_skus += p['productCount']
            internal_links += p['links']['internal']
            external_links += p['links']['external']

        summary = {
            'aboutUsPages': sum(1 for p in self.pages if p['metadata']['hasAboutUs']),
            'termsPages': sum(1 for p in self.pages if p['metadata']['hasTerms']),
            'privacyPages': sum(1 for p in self.pages if p['metadata']['hasPrivacy']),
            'contactPages': sum(1 for p in self.pages if p['metadata']['hasContact']),
            'productPages': sum(1 for p in self.pages if p['metadata']['hasProducts']),
            'servicePages': sum(1 for p in self.pages if p['metadata']['hasServices']),
            'totalInternalLinks': internal_links,
            'totalExternalLinks': external_links,
        }

        return {
            'baseUrl': self.base_url,
            'totalPages': len(self.pages),
            'totalSKUs': total_skus,
            'pagesByType': dict(pages_by_type),
            'pages': self.pages,
            'summary': summary,
            'crawlStats': {
                'successful': self.successful,
                'failed': self.failed,
                'totalTime': round(self.total_time, 2)
            }
        }

    def crawl(self):
        return asyncio.run(self.async_crawl())

    async def async_crawl(self):
        start = time.time()
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Referer": "https://google.com"}) as session:
            while self.url_queue and len(self.pages) < self.max_pages:
                batch = []
                while self.url_queue and len(batch) < self.concurrency and len(self.pages) + len(batch) < self.max_pages:
                    url = self.url_queue.pop(0)
                    if url not in self.visited_urls and self.is_internal_url(url):
                        self.visited_urls.add(url)
                        batch.append(url)

                tasks = [self.crawl_page(session, url) for url in batch]
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        self.pages.append(result)
        self.total_time = time.time() - start
        return self.generate_report()



if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(description="Website crawler for detailed site analysis")
    parser.add_argument("url", nargs="?", help="The base URL to start crawling from (e.g., https://example.com)")
    parser.add_argument("--max", type=int, help="Maximum number of pages to crawl (default: 10)")
    args = parser.parse_args()

    if not args.url:
        args.url = input("🔗 Enter the URL to crawl (e.g., https://example.com): ").strip()
        if not args.url:
            print("❌ No URL provided. Exiting.")
            sys.exit(1)

    if args.max is None:
        try:
            args.max = int(input("📄 Enter the maximum number of pages to crawl (default = 10): ").strip() or 10)
        except ValueError:
            print("❌ Invalid number entered. Using default = 10.")
            args.max = 10

    crawler = SiteCrawler(base_url=args.url, max_pages=args.max)
    crawl_result = crawler.crawl()
    if crawl_result["totalPages"] == 0:
       print("❌ Unable to crawl any pages. Please check the URL or site restrictions.")
       sys.exit(1)
    # with open("result.txt", "w", encoding="utf-8") as f:
    #     for page in crawl_result["pages"]:
    #         f.write(f"\n{'='*100}\nURL: {page['url']}\nTitle: {page['title']}\n")
    #         f.write(f"Page Type: {page['pageType']}, Status: {page['status']}, SKU Count: {page['productCount']}\n")
    #         f.write(f"Metadata: {json.dumps(page['metadata'], indent=2)}\n")
    #         f.write(f"{'-'*10}\n")
    #     f.write("\n\nSUMMARY\n")
    #     f.write(json.dumps(crawl_result["summary"], indent=2))
    #     f.write("\n\nCRAWL STATS\n")
    #     f.write(json.dumps(crawl_result["crawlStats"], indent=2))

    # print("\n✅ Crawl complete. Results saved to result.txt.")
    # ==== SAVE TO MONGODB ====
    load_dotenv()
mongo_uri = os.getenv("MONGO_URI")  # No fallback to localhost
client = MongoClient(mongo_uri)
db = client["website_crawler"]
collection = db["crawl_results"]

# Find the last used result number
last_doc = collection.find_one(sort=[("result_id", -1)])
last_id = last_doc.get("result_id", 0) if last_doc else 0
next_id = last_id + 1

# Wrap your crawl result inside a key like "result_1", "result_2", etc.
wrapped_result = {
    "result_id": next_id,
    f"result_{next_id}": {
        "baseUrl": crawl_result["baseUrl"],
        "pages": [
            {
                "url": p["url"],
                "title": p["title"],
                "pageType": p["pageType"],
                "status": p["status"],
                "productCount": p["productCount"],
                "metadata": p["metadata"]
            }
            for p in crawl_result["pages"]
        ],
        "summary": crawl_result["summary"],
        "crawlStats": crawl_result["crawlStats"]
    }
}

collection.insert_one(wrapped_result)
print(f"\n✅ Crawl complete. Stored as result_{next_id} in MongoDB.")