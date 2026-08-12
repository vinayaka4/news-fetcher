"""Create or migrate the configured database schema before serving requests."""

from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target


def main() -> None:
    with connect(database_target()) as connection:
        initialize(connection)
    print("Database schema is ready", flush=True)


if __name__ == "__main__":
    main()
