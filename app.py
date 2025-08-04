# app.py

from flask import Flask, request, jsonify
from flask_cors import CORS
import traceback

from light_runner import analyze_website  # updated to use MongoDB logic

app = Flask(__name__)
CORS(app)  # Enable CORS

@app.route('/')
def home():
    return "✅ API is up! Use POST /analyze with JSON: { url: string, max_pages: number (optional) }"

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.get_json()

    if not data or 'url' not in data:
        return jsonify({"error": "Missing 'url' in request body"}), 400

    url = data['url']
    max_pages = data.get('max_pages', 20)

    try:
        print(f"🚀 Starting analysis for: {url} with max_pages={max_pages}")
        result = analyze_website(url, max_pages)
        return jsonify(result)
    except Exception as e:
        trace = traceback.format_exc()
        print("❌ Exception in /analyze route:", e)
        print(trace)
        return jsonify({
            "error": str(e),
            "trace": trace
        }), 500

if __name__ == '__main__':
    print("🚀 Flask server is starting on http://127.0.0.1:5000")
    app.run(debug=True)
