from __future__ import annotations

import argparse
import json

from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target
from news_fetcher.raw_store import build_envelope, store_raw_event


def main() -> int:
    parser = argparse.ArgumentParser(description="Create raw envelopes for legacy normalized records")
    parser.add_argument("--database", default=database_target())
    args = parser.parse_args()
    created = 0
    with connect(args.database) as connection:
        initialize(connection)
        for row in connection.execute("SELECT * FROM articles"):
            payload = dict(row)
            _, inserted = store_raw_event(connection, build_envelope(
                source_type="legacy_normalized", source_key=row["source_key"], publisher=row["publisher"],
                external_id=row["article_url"], published_at=row["published_at"], payload=payload,
                metadata={"migrated_from": "articles"}, fetched_at=row["fetched_at"],
            ))
            created += int(inserted)
        for row in connection.execute("SELECT * FROM digest_stories"):
            payload = dict(row)
            for key in ("summary_json", "source_urls_json", "details_json"):
                if payload.get(key):
                    payload[key.removesuffix("_json")] = json.loads(payload.pop(key))
            _, inserted = store_raw_event(connection, build_envelope(
                source_type="legacy_normalized", source_key="perplexity", publisher="Perplexity Digest",
                external_id=row["id"], published_at=f"{row['published_at']}T00:00:00+05:30",
                payload=payload, metadata={"migrated_from": "digest_stories"}, fetched_at=row["fetched_at"],
            ))
            created += int(inserted)
        connection.commit()
    print(f"Created {created} raw envelopes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
