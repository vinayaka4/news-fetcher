from __future__ import annotations

import json
import os
from pathlib import Path

from flask import Flask, render_template, request
from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target
from news_fetcher.relevance import deduplicate, is_upsc_relevant
from news_fetcher.api import api

app = Flask(__name__)
DATABASE = database_target()
app.config["NEWS_DATABASE"] = DATABASE
app.config["DATABASE_URL"] = os.getenv("DATABASE_URL")
app.config["UPLOAD_DIRECTORY"] = os.getenv("UPLOAD_DIRECTORY", str(Path("uploads").resolve()))
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024
app.register_blueprint(api)
CITED_SOURCE_FILTERS = {
    "The Hindu (via Perplexity)": "thehindu.com",
    "Deccan Herald (via Perplexity)": "deccanherald.com",
}


def get_connection():
    return connect(DATABASE)


@app.get("/")
def index():
    order = "asc" if request.args.get("order") == "asc" else "desc"
    filters = {name: request.args.get(name, "").strip()
               for name in ("ministry", "publisher", "q", "category", "upsc")}
    show_all = request.args.get("show_all") == "1"
    if not str(DATABASE).startswith(("postgres://", "postgresql://")) and not Path(DATABASE).exists():
        return render_template("index.html", articles=[], ministries=[], publishers=[],
            categories=[], upsc_values=[], order=order, show_all=show_all, **filters)

    with get_connection() as connection:
        conditions, values = [], []
        if filters["ministry"]:
            conditions.append("ministry = ?"); values.append(filters["ministry"])
        if filters["publisher"] in CITED_SOURCE_FILTERS or filters["publisher"] == "Perplexity Digest":
            conditions.append("1 = 0")
        elif filters["publisher"]:
            conditions.append("publisher = ?"); values.append(filters["publisher"])
        if filters["q"]:
            conditions.append("(title LIKE ? OR excerpt LIKE ?)")
            values.extend([f"%{filters['q']}%", f"%{filters['q']}%"])
        if filters["category"] or filters["upsc"]:
            conditions.append("1 = 0")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rss_rows = connection.execute(f"""SELECT publisher, title, article_url, published_at,
            excerpt, ministry, full_text FROM articles {where} LIMIT 500""", values).fetchall()

        conditions, values = [], []
        if filters["q"]:
            conditions.append("(headline LIKE ? OR summary_json LIKE ?)")
            values.extend([f"%{filters['q']}%", f"%{filters['q']}%"])
        if filters["category"]:
            conditions.append("category = ?"); values.append(filters["category"])
        if filters["upsc"]:
            conditions.append("upsc_relevance = ?"); values.append(filters["upsc"])
        if filters["publisher"] in CITED_SOURCE_FILTERS:
            conditions.append("source_urls_json LIKE ?")
            values.append(f"%{CITED_SOURCE_FILTERS[filters['publisher']]}%")
        elif filters["publisher"] == "Perplexity Digest":
            pass
        elif filters["publisher"] or filters["ministry"]:
            conditions.append("1 = 0")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        digest_rows = connection.execute(f"""SELECT headline, category, summary_json,
            upsc_relevance, source_urls_json, published_at, details_json FROM digest_stories {where} LIMIT 500""",
            values).fetchall()

        articles = [dict(row) | {"category": None, "upsc_relevance": None,
                    "sources": None, "summary_points": None}
                    for row in rss_rows]
        for row in digest_rows:
            sources = json.loads(row["source_urls_json"])
            details = json.loads(row["details_json"]) if row["details_json"] else None
            articles.append({"publisher": "Perplexity Digest", "title": row["headline"],
                "article_url": sources[0], "published_at": row["published_at"],
                "excerpt": None, "summary_points": json.loads(row["summary_json"]), "ministry": None,
                "category": row["category"], "upsc_relevance": row["upsc_relevance"],
                "sources": sources, "details": details})
        if not show_all:
            articles = [item for item in articles if is_upsc_relevant(item)]
        articles = deduplicate(articles)
        articles.sort(key=lambda item: item["published_at"] or "", reverse=order == "desc")
        ministries = [row[0] for row in connection.execute(
            "SELECT DISTINCT ministry FROM articles WHERE ministry IS NOT NULL AND ministry != '' ORDER BY ministry")]
        publishers = [row[0] for row in connection.execute(
            "SELECT DISTINCT publisher FROM articles ORDER BY publisher")]
        if connection.execute("SELECT 1 FROM digest_stories LIMIT 1").fetchone():
            publishers.append("Perplexity Digest")
        for label, domain in CITED_SOURCE_FILTERS.items():
            if connection.execute("SELECT 1 FROM digest_stories WHERE source_urls_json LIKE ? LIMIT 1",
                                  (f"%{domain}%",)).fetchone():
                publishers.append(label)
        categories = [row[0] for row in connection.execute(
            "SELECT DISTINCT category FROM digest_stories ORDER BY category")]
        upsc_values = [row[0] for row in connection.execute(
            "SELECT DISTINCT upsc_relevance FROM digest_stories ORDER BY upsc_relevance")]
    return render_template("index.html", articles=articles, ministries=ministries,
        publishers=publishers, categories=categories, upsc_values=upsc_values,
        order=order, show_all=show_all, **filters)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
