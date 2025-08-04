import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import sys
def scrape_website(url: str):
    headers_list = [
        {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
        {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
        {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'},
        {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'},
        {'User-Agent': 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)'}
    ]

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    response = None
    for headers in headers_list:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                break
        except requests.RequestException:
            continue

    if not response or response.status_code != 200:
        return {"url": url, "error": "Failed to retrieve page"}

    soup = BeautifulSoup(response.text, 'html.parser')

    for tag in soup(['script', 'style', 'noscript', 'iframe']):
        tag.decompose()
    for cls in ['popup', 'modal', 'overlay', 'cookie-banner', 'cookie-consent', 'ad', 'ads', 'advertisement']:
        for tag in soup.select(f'.{cls}, #{cls}'):
            tag.decompose()

    title = (soup.title.string if soup.title else '').strip()
    description = ''
    if soup.find("meta", attrs={"name": "description"}):
        description = soup.find("meta", attrs={"name": "description"}).get("content", '')

    nav_text = ' '.join([el.get_text(strip=True) for el in soup.select('nav, .nav, .menu, .navbar, header')])
    about_text = ' '.join([el.get_text(strip=True) for el in soup.select('[href*="about"], .about, #about')])
    services_text = ' '.join([el.get_text(strip=True) for el in soup.select('[href*="service"], .services, #services')])
    products_text = ' '.join([el.get_text(strip=True) for el in soup.select('[href*="product"], .products, #products')])

    content = ''
    if nav_text:
        content += f"Navigation: {nav_text}\n"
    if about_text:
        content += f"About: {about_text}\n"
    if services_text:
        content += f"Services: {services_text}\n"
    if products_text:
        content += f"Products: {products_text}\n"

    main_content = ''
    for selector in ['main', '.main-content', '#main-content', '.content', '#content', 'article']:
        el = soup.select_one(selector)
        if el and len(el.get_text(strip=True)) > 100:
            main_content = el.get_text(separator=' ', strip=True)
            break

    if not main_content:
        text = soup.get_text(separator=' ', strip=True)
        lines = [line for line in text.splitlines() if len(line) > 10 and all(bad not in line.lower() for bad in ['cookie', 'google', 'facebook'])]
        main_content = ' '.join(lines[:50])

    content += main_content.strip()
    content = ' '.join(content.split())[:5000]

    metadata = analyze_metadata(soup, content, url)

    return {
        "title": title or 'Untitled',
        "description": description or '',
        "content": content,
        "url": response.url,
        "metadata": metadata
    }

def analyze_metadata(soup, content, url):
    return {
        "pageType": detect_page_type(soup, content),
        "hasProducts": bool(soup.select('[class*="product"], .price, .shop')),
        "hasServices": bool(re.search(r'service|consultation|support', soup.get_text(), re.IGNORECASE)),
        "contactInfo": bool(re.search(r'@|\+\d|\(\d{3}\)', soup.get_text())),
        "socialLinks": list({a['href'] for a in soup.select('a[href]') if any(x in a['href'] for x in ['facebook', 'linkedin', 'twitter'])}),
        "images": len(soup.find_all('img')),
        "links": len(soup.find_all('a')),
        "keywords": extract_keywords(soup, content),
        "contentSections": identify_sections(soup),
        "pageHeadings": extract_headings(soup)
    }

def detect_page_type(soup, content):
    content_lower = content.lower()
    title = (soup.title.string or '').lower()
    meta_description = soup.find('meta', attrs={'name': 'description'})
    meta_description = meta_description['content'].lower() if meta_description else ''
    all_text = f"{content_lower} {title} {meta_description}"

    scores = {
        'E-commerce': sum(kw in all_text for kw in ['shop', 'buy', 'cart', 'checkout', 'price']) + len(soup.select('.product, .add-to-cart')),
        'Blog/News': sum(kw in all_text for kw in ['blog', 'news', 'article', 'post']) + len(soup.select('article')),
        'Portfolio': sum(kw in all_text for kw in ['portfolio', 'gallery', 'project']) + len(soup.select('.gallery, .portfolio')),
        'Services': sum(kw in all_text for kw in ['services', 'consulting']) + len(soup.select('.services')),
        'Corporate': sum(kw in all_text for kw in ['about us', 'company', 'contact']) + len(soup.select('.about'))
    }
    best = max(scores.items(), key=lambda x: x[1])
    return best[0] if best[1] > 0 else 'Landing Page'

def extract_keywords(soup, content):
    if meta := soup.find('meta', attrs={'name': 'keywords'}):
        return [kw.strip() for kw in meta['content'].split(',')][:10]
    words = re.findall(r'\b\w{5,}\b', content.lower())
    freq = Counter(words)
    return [word for word, _ in freq.most_common(8)]

def identify_sections(soup):
    sections = []
    for label, selector in [
        ('Header', 'header, .header'),
        ('Navigation', 'nav, .nav, .menu'),
        ('Main Content', 'main, .main, .content'),
        ('Sidebar', '.sidebar, aside'),
        ('Footer', 'footer, .footer'),
        ('Hero/Banner', '.hero, .banner'),
        ('Testimonials', '.testimonial'),
        ('Gallery', '.gallery, .portfolio'),
        ('Forms', 'form')
    ]:
        if soup.select(selector):
            sections.append(label)
    return sections

def extract_headings(soup):
    tags = ['h1', 'h2', 'h3', 'nav a', '.nav a', '.menu a', '.section-title', '.title', '.heading']
    texts = set()
    for selector in tags:
        for el in soup.select(selector):
            text = el.get_text(strip=True)
            if text and 0 < len(text) < 100:
                texts.add(text)
    return list(texts)[:20]

def extract_urls_from_mongodb(crawl_id: int):
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["website_crawler"]
    collection = db["crawl_results"]

    crawl_data = collection.find_one({"result_id": crawl_id})
    if not crawl_data or "result_{}".format(crawl_id) not in crawl_data:
        raise ValueError(f"No crawl result found for result_id = {crawl_id}")

    return [page["url"] for page in crawl_data[f"result_{crawl_id}"]["pages"] if "url" in page]

def scrape_all_concurrently(urls, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(scrape_website, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                print(f"✅ Scraped: {url}")
                results.append(result)
            except Exception as e:
                print(f"❌ Error scraping {url}: {e}")
                results.append({"url": url, "error": str(e)})
    return results

if __name__ == "__main__":
    # Accept crawl_id from command-line if passed
    if len(sys.argv) > 1:
        try:
            crawl_id = int(sys.argv[1])
        except ValueError:
            print("❌ Invalid crawl ID. Must be an integer.")
            sys.exit(1)
    else:
        crawl_id = int(input("🔢 Enter the Crawl ID to scrape from MongoDB: "))

    urls = extract_urls_from_mongodb(crawl_id)
    print(f"🔎 Found {len(urls)} URLs to scrape from crawl ID = {crawl_id}...")

    all_results = scrape_all_concurrently(urls, max_workers=10)

    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["website_crawler"]
    collection = db["scrape_results"]

    scrape_document = {
        "_id": crawl_id,
        "crawl_id": crawl_id,
        "compliance_sections": all_results
    }

    collection.replace_one({"_id": crawl_id}, scrape_document, upsert=True)
    print(f"🎉 All scraping completed. Results saved in MongoDB with ID = {crawl_id}")

# if __name__ == "__main__":
#     urls = extract_urls_from_result_txt()
#     print(f"🔎 Found {len(urls)} URLs to scrape...")
#     all_results = scrape_all_concurrently(urls, max_workers=10)

#     with open("entil.txt", "w", encoding="utf-8") as f:
#         json.dump({"compliance_sections": all_results}, f, indent=2, ensure_ascii=False)

#     print("🎉 All scraping completed. Results saved to entil.txt")

# if __name__ == "__main__":
#     crawl_id = int(input("🔢 Enter the Crawl ID to scrape from MongoDB: "))
#     urls = extract_urls_from_mongodb(crawl_id)
#     print(f"🔎 Found {len(urls)} URLs to scrape from crawl ID = {crawl_id}...")

#     all_results = scrape_all_concurrently(urls, max_workers=10)

#     load_dotenv()
#     mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
#     client = MongoClient(mongo_uri)
#     db = client["website_crawler"]
#     collection = db["scrape_results"]

#     last = collection.find_one(sort=[("_id", -1)])
#     next_id = 1 if not last else last["_id"] + 1

#     scrape_document = {
#         "_id": next_id,
#         "crawl_id": crawl_id,
#         "compliance_sections": all_results
#     }

#     collection.insert_one(scrape_document)
#     print(f"🎉 All scraping completed. Results saved in MongoDB with ID = {next_id}")
