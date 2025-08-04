import os
import json
import subprocess
from dotenv import load_dotenv
import sys
import re
import time
import hashlib
from pymongo import MongoClient
from openai import OpenAI
import anthropic

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "website_crawler"

# ---------------------- Basic Cache for URL Repeats ----------------------
url_cache = {}
def is_repeated_url(url):
    key = hashlib.sha256(url.encode()).hexdigest()
    now = time.time()
    window = 3600  # 1 hour
    threshold = 6
    if key not in url_cache:
        url_cache[key] = []
    url_cache[key] = [t for t in url_cache[key] if now - t < window]
    url_cache[key].append(now)
    return len(url_cache[key]) > threshold

# ---------------------- MongoDB Fetchers ----------------------
def get_latest_crawl_id():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["crawl_results"]
    last = collection.find_one(sort=[("result_id", -1)])
    return last["result_id"] if last else None

def fetch_crawl_text(crawl_id):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    data = db["crawl_results"].find_one({"result_id": crawl_id})
    if not data or f"result_{crawl_id}" not in data:
        return ""

    pages = data[f"result_{crawl_id}"]["pages"]
    lines = []
    for page in pages:
        lines.append(f"URL: {page['url']}")
        lines.append(f"Title: {page['title']}")
        lines.append(f"Page Type: {page['pageType']}, Status: {page['status']}, SKU Count: {page['productCount']}")
        lines.append(f"Metadata: {json.dumps(page['metadata'])}")
        lines.append("-" * 20)
    return "\n".join(lines)

def fetch_scrape_json(crawl_id):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    data = db["scrape_results"].find_one({"_id": crawl_id})
    return data.get("compliance_sections", []) if data else []

# ---------------------- Helpers ----------------------
def run_crawler(url, max_pages):
    print("🚀 Running crawl.py...")
    subprocess.run([sys.executable, "crawl.py", url, "--max", str(max_pages)], check=True)

def run_scraper(crawl_id):
    print(f"🔍 Running scrape.py with crawl_id={crawl_id}")
    subprocess.run([sys.executable, "scrape.py", str(crawl_id)], check=True)


def extract_total_skus(crawl_text):
    sku_matches = re.findall(r'SKU Count:\s*(\d+)', crawl_text)
    total = sum(int(count) for count in sku_matches)
    print(f"🧮 Found {len(sku_matches)} SKU entries. Total SKUs: {total}")
    return total

def count_total_pages(crawl_text):
    return len(re.findall(r'URL:', crawl_text))

def normalize(text):
    return re.sub(r"\s+", " ", text.strip())

def flatten_content(data):
    if isinstance(data, dict):
        return " ".join(flatten_content(v) for v in data.values())
    elif isinstance(data, list):
        return " ".join(flatten_content(item) for item in data)
    elif isinstance(data, str):
        return data
    return ""

def extract_json(text):
    cleaned = re.sub(r"```(?:json)?|```", "", text).strip()
    try:
        start = cleaned.find('{')
        end = cleaned.rfind('}') + 1
        json_candidate = cleaned[start:end]
        return json.loads(json_candidate)
    except Exception as e:
        print(f"❌ JSON parsing failed: {e}")
        return None

# ---------------------- LLM Summary ----------------------
def summarize_with_openai_or_claude(crawl_text, scrape_json, total_skus, total_pages):
    prompt = f"""
You are an expert at analyzing eCommerce website data.

Below is content collected from a merchant website. Your task is to **estimate the number of unique products** listed across all pages, even if SKU codes are missing.

Be accurate — avoid double-counting. Consider a listing “unique” if it has a different title, description, image, or price. Products may appear in text blocks, cards, or repeated patterns.

Also return a short summary of the site in 2–4 lines and mention presence or absence of each of the following key pages: About Us, Privacy Policy, Terms & Conditions, Contact, Shipping, Refund.

Respond strictly in the following JSON format:

{{
  "websiteSummary": "Brief 1-2 sentence summary of the website purpose.",
  "estimatedProductCount": "Approximate number of distinct products being sold, based on the text and structure of the pages. You may ignore any SKU counts if they seem inaccurate. Give number output just",
  "totalPages": {total_pages},
  "totalSKUs": {total_skus},
  "webCompliance": {{
    "aboutUs": {{
      "summary": "2-4 line description or 'not found'",
      "url": "URL or 'not found'"
    }},
    "privacyPolicy": {{
      "summary": "2-4 line description or 'not found'",
      "url": "URL or 'not found'"
    }},
    "termsAndConditions": {{
      "summary": "2-4 line description or 'not found'",
      "url": "URL or 'not found'"
    }},
    "contactUs": {{
      "summary": "2-4 line description or 'not found'",
      "url": "URL or 'not found'"
    }},
    "shippingPolicy": {{
      "summary": "2-4 line description or 'not found'",
      "url": "URL or 'not found'"
    }},
    "refundPolicy": {{
      "summary": "2-4 line description or 'not found'",
      "url": "URL or 'not found'"
    }}
  }}
}}

## INSTRUCTIONS:
Analyze the crawl and scrape output and fill in the fields accurately.

## CRAWL DATA:
{crawl_text[:3500]}

## SCRAPE DATA:
{json.dumps(scrape_json)[:3500]}
"""
    try:
        print("🤖 Sending to OpenAI GPT-4...")
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        return extract_json(response.choices[0].message.content.strip())
    except Exception as e:
        print("⚠️ OpenAI failed:", e)
        try:
            print("🤖 Falling back to Claude...")
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            response = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=1024,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}]
            )
            return extract_json(response.content[0].text.strip())
        except Exception as ce:
            print("❌ Claude also failed:", ce)
            return {"error": "Summarization failed from all models."}

# ---------------------- Category Classification ----------------------
def run_risk_analysis(crawl_text, risk_matrix_path="risk_matrix.json"):
    with open(risk_matrix_path, "r", encoding="utf-8") as f:
        risk_matrix = json.load(f)

    website_text = flatten_content(crawl_text)[:6000]
    available_categories = "\n".join(
        f"{normalize(e['Category'])} - {normalize(e['Sub_Category'])} (MCC: {e['MCC_Code']})"
        for e in risk_matrix
    )

    prompt = f"""
Analyze the following website content and categorize it based on the available business categories provided.

Website Content:
{website_text}

Available Categories (format: Category - Subcategory (MCC: code)):
{available_categories}

Please respond with JSON in this exact format:
{{
  "category": "exact category name only (before the dash)",
  "subcategory": "exact subcategory name only (between dash and MCC)",
  "MCC_Code":"Give the MCC code for the subcategory",
  "Risk_Level":"Give the risk level of the subcategory",
  "Risk_Score":"Give risk score of the subcategory ",
  "confidence": number between 0 and 1,
  "reasoning": "detailed explanation of MCC assignment decision",
  "evidence": {{
    "keyIndicators": ["specific words/phrases that indicated this category"],
    "productTypes": ["specific products/services mentioned"],
    "businessModel": "description of how business operates",
    "targetMarket": "who the business serves",
    "primaryActivity": "main business activity identified"
  }},
  "decisionProcess": "step-by-step explanation of how you arrived at this MCC"
}}
"""
    try:
        print("🏷️ Classifying via OpenAI...")
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        return extract_json(response.choices[0].message.content.strip())
    except Exception as e:
        print("⚠️ OpenAI failed:", e)
        try:
            print("🏷️ Falling back to Claude...")
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            response = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=1024,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}]
            )
            return extract_json(response.content[0].text.strip())
        except Exception as ce:
            print("❌ Claude also failed:", ce)
            return {"error": "All model calls failed for classification."}

# ---------------------- Main Entry ----------------------
def analyze_site(url, max_pages=50):
    if not url:
        return {"success": False, "error": "URL is required.", "code": "INVALID_URL"}
    if not url.startswith("http://") and not url.startswith("https://"):
        return {"success": False, "error": "URL must start with http:// or https://", "code": "INVALID_URL"}
    if is_repeated_url(url):
        return {"success": False, "error": "Too many requests for this URL.", "code": "TOO_MANY_REQUESTS"}

    try:
        run_crawler(url, max_pages)
        crawl_id = get_latest_crawl_id()
        run_scraper(crawl_id)

        crawl_id = get_latest_crawl_id()
        if not crawl_id:
            return {"success": False, "error": "No crawl data found in MongoDB.", "code": "NO_CRAWL"}

        crawl_text = fetch_crawl_text(crawl_id)
        scrape_json = fetch_scrape_json(crawl_id)
        total_pages = count_total_pages(crawl_text)
        total_skus = extract_total_skus(crawl_text)

        if total_pages == 0:
            return {"success": False, "error": "No pages crawled. Site may be blocking bots.", "code": "CRAWL_EMPTY"}

        summary = summarize_with_openai_or_claude(crawl_text, scrape_json, total_skus, total_pages)
        classification = run_risk_analysis(crawl_text)

        return {
            "success": True,
            "analysis": summary,
            "classification": classification
        }

    except subprocess.CalledProcessError:
        return {"success": False, "error": "Crawler failed. Site might be blocking requests.", "code": "FETCH_FAILED"}
    except Exception as e:
        return {"success": False, "error": str(e), "code": "SERVER_ERROR"}

# ---------------------- CLI Entry ----------------------
def main():
    url = input("🌐 Enter website URL to crawl: ").strip()
    if not url:
        print("❌ URL required.")
        return

    try:
        max_pages = int(input("📄 Max number of pages to crawl (default 50): ").strip() or 50)
    except ValueError:
        max_pages = 10

    result = analyze_site(url, max_pages)
    if not result["success"]:
        print(f"❌ {result['error']} (Code: {result['code']})")
        return

    print("\n✅ Final Combined Result:\n")
    print(json.dumps({
        "analysis": result["analysis"],
        "classification": result["classification"]
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
