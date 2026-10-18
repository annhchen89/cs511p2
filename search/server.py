#!/usr/bin/env python3
"""CS 511 Project 2, Part 2: the search REST API (runs on main).

    python3 server.py [--port 8000] [--database /data/search.db]

API (the tests rely on it exactly):

    GET /search?q=<query>   ->  200, application/json
        {"query": "<query>",
         "results": [{"id": 155765, "title": "Champaign, Illinois", "score": 1.2e-05}, ...]}
    GET /health             ->  200 once the server can answer queries

Search: split the query into terms with tokenize() (the Project 1 rules) and
keep the distinct terms. An article matches when its text contains every term.
Its score is

    score = PageRank(article) * sum over the terms t of tf(t, article) * ln(N / df(t))

(tf, df and N as in indexer.py). Return the 10 best matches, ordered by
descending score and then ascending id; a query without terms or without
matches returns an empty list.

Only SearchIndex (marked TODO) needs to be written; the HTTP handling is
given. Use any database you like (see serve.sh).
"""
import argparse
import heapq
import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import datetime
import math
import os
import sqlite3
import subprocess
import tempfile
import time
from contextlib import closing
from pathlib import Path
from functools import lru_cache

TOP_K = 10

# Tokenization rules of Project 1.
TOKEN_PATTERN = re.compile(r"[a-z]+(?:'[a-z]+)?")
MIN_TOKEN_LENGTH = 2
STOPWORDS = frozenset(
    "a an and are as at be by for from in is it of on or that the to was with".split()
)


def tokenize(text):
    """Project 1 tokenizer: lowercase, match TOKEN_PATTERN, drop short words and stopwords."""
    return [word for word in TOKEN_PATTERN.findall(text.lower())
            if len(word) >= MIN_TOKEN_LENGTH and word not in STOPWORDS]

def measure_index_build(metrics_path, command):
    """Run spark-submit and record its duration separately from database loading.

    Called by serve.sh before load_index(); importing this module does not
    start the HTTP server.
    """
    started = time.perf_counter()
    result = subprocess.run(command, check=False)
    elapsed = time.perf_counter() - started
    if result.returncode:
        return result.returncode
    metrics = {"index_build_seconds": elapsed,
               "built_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "build_command": command}
    Path(metrics_path).write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    print("INDEX BUILD SECONDS: %.6f" % elapsed, flush=True)
    return 0


_INDEX_BATCH_SIZE = 10000
_INDEX_SCHEMA = """
CREATE TABLE docs (
    id INTEGER PRIMARY KEY,
    rank REAL NOT NULL CHECK(rank >= 0),
    title TEXT NOT NULL
);
CREATE TABLE terms (
    term_id INTEGER PRIMARY KEY,
    term TEXT NOT NULL UNIQUE,
    df INTEGER NOT NULL CHECK(df > 0)
);
CREATE TABLE postings (
    term_id INTEGER NOT NULL,
    article_id INTEGER NOT NULL,
    weight REAL NOT NULL CHECK(weight >= 0),
    score REAL NOT NULL CHECK(score >= 0),
    PRIMARY KEY (term_id, article_id)
) WITHOUT ROWID;
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
"""


def _hdfs_lines(uri):
    # No shell expansion or local copy of the full index is necessary.
    process = subprocess.Popen(["hdfs", "dfs", "-cat", uri + "/part-*"],
                               stdout=subprocess.PIPE, text=True, encoding="utf-8")
    try:
        yield from process.stdout
        if process.wait() != 0:
            raise RuntimeError("Could not read HDFS index: " + uri)
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.terminate()
        process.wait()


def _index_batches(records):
    batch = []
    for record in records:
        batch.append(record)
        if len(batch) == _INDEX_BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def _index_docs(uri):
    for line in _hdfs_lines(uri + "/docs"):
        article_id, rank, title = line.rstrip("\n").split("\t", 2)
        rank = float(rank)
        if not math.isfinite(rank) or rank < 0:
            raise ValueError("Invalid rank for article " + article_id)
        yield int(article_id), rank, title


def _index_posting_rows(term_id, encoded):
    # A term occupies one HDFS line in the starter layout. Iterate matches
    # instead of allocating a second list of all its comma-separated entries.
    for match in re.finditer(r"[^,]+", encoded):
        article_id, weight = match.group().split(":")
        weight = float(weight)
        if not math.isfinite(weight) or weight < 0:
            raise ValueError("Invalid weight for article " + article_id)
        yield term_id, weight, weight, int(article_id)


def load_index(index_uri, database, articles_uri, ranks_uri, build_metrics=None,
               metrics_path="/data/search-metrics.json"):
    """Stream the HDFS index into SQLite on main, then publish the database.

    serve.sh calls this after spark-submit succeeds. If build_metrics is
    omitted, the output leaves index build time unmeasured.
    """
    started = time.perf_counter()
    destination = Path(database).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=destination.name + ".", suffix=".tmp",
                                            dir=destination.parent)
    os.close(descriptor)
    connection = None
    try:
        connection = sqlite3.connect(temporary)
        # This is a disposable staging database. A failed load never replaces
        # the committed database; durable settings are restored before publish.
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA cache_size = -65536")
        connection.execute("PRAGMA temp_store = FILE")
        connection.executescript(_INDEX_SCHEMA)
        connection.execute("BEGIN")
        document_count = 0
        for batch in _index_batches(_index_docs(index_uri)):
            connection.executemany("INSERT INTO docs VALUES (?, ?, ?)", batch)
            document_count += len(batch)

        term_count = posting_count = 0
        insert = ("INSERT INTO postings (term_id, article_id, weight, score) "
                  "SELECT ?, id, ?, rank * ? FROM docs WHERE id = ?")
        for line in _hdfs_lines(index_uri + "/postings"):
            term, encoded = line.rstrip("\n").split("\t", 1)
            term_count += 1
            frequency = 0
            for batch in _index_batches(_index_posting_rows(term_count, encoded)):
                cursor = connection.executemany(insert, batch)
                if cursor.rowcount != len(batch):
                    raise ValueError("Posting refers to an article missing from docs: " + term)
                frequency += len(batch)
            connection.execute("INSERT INTO terms VALUES (?, ?, ?)", (term_count, term, frequency))
            posting_count += frequency

        # Build the secondary index after bulk loading to avoid updating it
        # separately for each posting. The primary key serves AND lookups.
        connection.execute("CREATE INDEX postings_by_score "
                           "ON postings (term_id, score DESC, article_id ASC)")
        connection.execute("ANALYZE")
        connection.commit()
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        load_seconds = time.perf_counter() - started
        metrics = (json.loads(Path(build_metrics).read_text(encoding="utf-8"))
                   if build_metrics is not None else {})
        metrics.update({"database_load_seconds": load_seconds,
                        "articles_uri": articles_uri, "ranks_uri": ranks_uri,
                        "index_uri": index_uri, "database": str(destination),
                        "documents": document_count, "terms": term_count,
                        "postings": posting_count,
                        "loaded_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        metadata = {**metrics, "schema_version": "1", "ready": "1"}
        with connection:
            connection.executemany("INSERT INTO metadata VALUES (?, ?)",
                                   ((key, str(value)) for key, value in metadata.items()))
        connection.close()
        connection = None
        os.replace(temporary, destination)
        metrics["database_bytes"] = destination.stat().st_size
        Path(metrics_path).write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        print("DATABASE LOAD SECONDS: %.6f" % load_seconds, flush=True)
        print("DATABASE BYTES: %d" % metrics["database_bytes"], flush=True)
        print("LOADED: %d documents, %d terms, %d postings" %
              (document_count, term_count, posting_count), flush=True)
    finally:
        if connection is not None:
            connection.close()
        for suffix in ("", "-journal"):
            path = Path(temporary + suffix)
            if path.exists():
                path.unlink()

class SearchIndex:
    """Answers queries from the database that serve.sh loaded."""

    def __init__(self, database):
        """Connect to your database; load what should stay in memory.

        `database` is the --database option (useful if your database is a
        file); ignore it otherwise. Called once, when the server starts.
        """
        # TODO: implement.
        # raise NotImplementedError("SearchIndex.__init__")
        self.database_uri = Path(database).resolve().as_uri() + "?mode=ro&immutable=1"
        with closing(self._connect()) as connection:
            metadata = dict(connection.execute("SELECT key, value FROM metadata"))
            if metadata.get("ready") != "1" or metadata.get("schema_version") != "1":
                raise ValueError("Search database is incomplete or has an unsupported schema")
        self._cached_search = lru_cache(maxsize=256)(self._search)

    def search(self, terms):
        """Return the top TOP_K [(score, article_id, title)] for a set of terms.

        Called concurrently from several threads (one per request). An
        article matches when it contains every term; see the module docstring
        for the score. Return [] when there are no terms or no matches.
        """
        # TODO: implement. heapq.nsmallest(TOP_K, ..., key=lambda s: (-s[0], s[1]))
        # keeps the TOP_K best without sorting every match.
        # raise NotImplementedError("SearchIndex.search")
        return list(self._cached_search(tuple(sorted(set(terms)))))

    def _connect(self):
        connection = sqlite3.connect(self.database_uri, uri=True)
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA cache_size = -8192")
        connection.execute("PRAGMA mmap_size = 268435456")
        return connection

    def _search(self, terms):
        if not terms:
            return ()
        with closing(self._connect()) as connection:
            term_info = []
            for term in terms:
                row = connection.execute(
                    "SELECT term_id, df FROM terms WHERE term = ?", (term,)).fetchone()
                if row is None:
                    return ()
                term_info.append(row)
            term_info.sort(key=lambda row: (row[1], row[0]))
            term_ids = [row[0] for row in term_info]
            if len(term_ids) == 1:
                # The score index serves even very frequent single terms
                # without scanning and sorting their complete posting lists.
                return tuple(connection.execute(
                    "SELECT p.score, p.article_id, d.title "
                    "FROM postings AS p INDEXED BY postings_by_score "
                    "CROSS JOIN docs AS d "
                    "WHERE p.term_id = ? AND d.id = p.article_id "
                    "ORDER BY p.score DESC, p.article_id ASC LIMIT ?",
                    (term_ids[0], TOP_K)))
            if len(term_ids) > 16:
                return self._long_query(connection, term_ids)

            # CROSS JOIN fixes the outer scan to the rarest term. Other terms
            # use exact lookups on (term_id, article_id), preserving AND.
            aliases = ["p%d" % i for i in range(len(term_ids))]
            tables = " CROSS JOIN ".join("postings AS " + alias for alias in aliases)
            conditions = [alias + ".term_id = ?" for alias in aliases]
            conditions += [alias + ".article_id = p0.article_id" for alias in aliases[1:]]
            weights = " + ".join(alias + ".weight" for alias in aliases)
            sql = ("SELECT d.rank * (" + weights + ") AS score, d.id, d.title "
                   "FROM " + tables + " CROSS JOIN docs AS d "
                   "WHERE d.id = p0.article_id AND " + " AND ".join(conditions) +
                   " ORDER BY score DESC, d.id ASC LIMIT ?")
            return tuple(connection.execute(sql, (*term_ids, TOP_K)))

    def _long_query(self, connection, term_ids):
        # Avoid SQLite's joined-table limit for unusually long queries. Keep
        # only the rarest term's candidates, then intersect in bounded batches.
        candidates = dict(connection.execute(
            "SELECT article_id, weight FROM postings WHERE term_id = ?", (term_ids[0],)))
        for term_id in term_ids[1:]:
            ids = list(candidates)
            intersection = {}
            for offset in range(0, len(ids), 400):
                batch = ids[offset:offset + 400]
                sql = ("SELECT article_id, weight FROM postings WHERE term_id = ? "
                       "AND article_id IN (" + ",".join("?" for _ in batch) + ")")
                for article_id, weight in connection.execute(sql, (term_id, *batch)):
                    intersection[article_id] = candidates[article_id] + weight
            candidates = intersection
            if not candidates:
                return ()

        def scored():
            ids = list(candidates)
            for offset in range(0, len(ids), 400):
                batch = ids[offset:offset + 400]
                sql = "SELECT id, rank, title FROM docs WHERE id IN (" + ",".join("?" for _ in batch) + ")"
                for article_id, rank, title in connection.execute(sql, batch):
                    yield rank * candidates[article_id], article_id, title

        return tuple(heapq.nsmallest(TOP_K, scored(), key=lambda row: (-row[0], row[1])))


# --- Given code -----------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    index = None

    def send_json(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self.send_json(200, {"status": "ok"})
        if url.path != "/search":
            return self.send_json(404, {"error": "not found"})
        query = parse_qs(url.query).get("q", [""])[0]
        results = self.index.search(frozenset(tokenize(query)))
        self.send_json(200, {"query": query, "results": [
            {"id": article_id, "title": title, "score": score}
            for score, article_id, title in results]})

    def log_message(self, *args):
        pass  # keep the log quiet


def main(argv):
    parser = argparse.ArgumentParser(description="Search REST API")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", default="/data/search.db",
                        help="where serve.sh put the index (if your database uses a file)")
    args = parser.parse_args(argv[1:])
    Handler.index = SearchIndex(args.database)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print("serving on port %d" % args.port, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
