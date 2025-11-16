import sqlite3
import json
from tqdm.auto import tqdm
import jsonlines


class WorkDatabase:
    """
    SQLite-backed work database.
    """

    def __init__(self, filename: str):
        self.db = sqlite3.connect(filename)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS Data (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Input TEXT,
                Output TEXT
            )
        """)
        # No auto-commit. User controls commits.
        self._pbar = None

    # ----------------------------------------------------------------------
    # Context Manager Support
    # ----------------------------------------------------------------------

    def __enter__(self):
        """Return database object when entering context."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensure cleanup on exit.
        Does NOT commit automatically.
        """
        self.close()
        return False

    # ----------------------------------------------------------------------
    # Transaction Helpers
    # ----------------------------------------------------------------------

    def begin(self):
        """
        Begin a transaction explicitly.
        SQLite implicitly begins transactions on writes, but this enforces clarity.
        """
        self.db.execute("BEGIN")

    def commit(self):
        """Commit the current transaction."""
        self.db.commit()

    def rollback(self):
        """Roll back the current transaction."""
        self.db.rollback()

    # ----------------------------------------------------------------------
    # Core Methods (manual commit required)
    # ----------------------------------------------------------------------

    def close(self):
        """Close progress bar and database connection."""
        if self._pbar:
            self._pbar.close()
            self._pbar = None
        self.db.close()

    def add(self, x):
        """Insert an input row. No automatic commit."""
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO Data (Input) VALUES (?)", (json.dumps(x),))
        cursor.close()

    def remove_duplicates(self):
        cursor = self.db.cursor()
        cursor.execute("""
            DELETE FROM Data
            WHERE ID NOT IN (
                SELECT MIN(ID)
                FROM Data
                GROUP BY Input
            )
        """)
        self.db.commit()
        cursor.close()

    def update(self, record_id: int, y):
        """Update output of a row. No automatic commit."""
        cursor = self.db.cursor()
        cursor.execute("UPDATE Data SET Output = ? WHERE ID = ?", (json.dumps(y), record_id))
        cursor.close()

        if self._pbar:
            self._pbar.update(1)

    def total_size(self) -> int:
        """Return total record count."""
        cursor = self.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM Data")
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def completed_size(self) -> int:
        """Return count of rows with non-null output."""
        cursor = self.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM Data WHERE Output IS NOT NULL")
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def incomplete(self):
        """
        Yield incomplete rows one at a time with progress tracking.
        No database writes here.
        """
        total = self.total_size()
        done = self.completed_size()

        cursor = self.db.cursor()
        cursor.execute("SELECT ID, Input FROM Data WHERE Output IS NULL")

        self._pbar = tqdm(total=total, initial=done, desc="Processing records")

        try:
            while True:
                row = cursor.fetchone()
                if row is None:
                    break

                record_id, input_json = row
                yield record_id, json.loads(input_json)

        finally:
            cursor.close()
            self._pbar.close()
            self._pbar = None

    def write_jsonl(self, jsonl_file: str, batch_size: int = 1000):
        """
        Write completed rows to JSONL in batches.
        READ-ONLY — does not require commit.
        """
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT ID, Input, Output
            FROM Data
            WHERE Input IS NOT NULL AND Output IS NOT NULL
        """)

        with jsonlines.open(jsonl_file, mode='w') as writer:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                for record_id, input_json, output_json in rows:
                    writer.write({
                        "id": record_id,
                        "input": json.loads(input_json),
                        "output": json.loads(output_json)
                    })

        cursor.close()

    def write_jsonl_custom(self, f, jsonl_file: str, batch_size: int = 1000):
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT ID, Input, Output
            FROM Data
            WHERE Input IS NOT NULL AND Output IS NOT NULL
        """)

        with jsonlines.open(jsonl_file, mode='w') as writer:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                for record_id, input_json, output_json in rows:
                    writer.write(
                        f(record_id, input_json, output_json)
                    )

        cursor.close()

