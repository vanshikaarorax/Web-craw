from light import (
    run_crawler,
    run_scraper,
    get_latest_crawl_id,
    fetch_crawl_text,
    fetch_scrape_json,
    extract_total_skus,
    count_total_pages,
    summarize_with_openai_or_claude,
    run_risk_analysis
)
import json

def analyze_website(url, max_pages=20):
    print(f"🔍 Crawling: {url}")
    run_crawler(url, max_pages)

    crawl_id = get_latest_crawl_id()
    if not crawl_id:
        return {"error": "❌ No crawl ID found after crawling."}

    print("🧹 Scraping pages from crawl ID:", crawl_id)
    run_scraper(crawl_id)

    print("📄 Fetching crawl and scrape data from MongoDB...")
    crawl_text = fetch_crawl_text(crawl_id)
    scrape_json = fetch_scrape_json(crawl_id)

    if not crawl_text or not scrape_json:
        return {"error": "❌ Failed to retrieve crawl or scrape data from MongoDB."}

    total_skus = extract_total_skus(crawl_text)
    total_pages = count_total_pages(crawl_text)

    print("🧠 Summarizing with OpenAI/Claude...")
    summary = summarize_with_openai_or_claude(
        crawl_text,
        scrape_json,
        total_skus,
        total_pages
    )

    print("🏷️ Running risk classification...")
    classification = run_risk_analysis(crawl_text)

    return {
        "analysis": summary,
        "classification": classification
    }
