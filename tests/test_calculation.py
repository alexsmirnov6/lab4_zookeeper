import io
import json
import sys
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from calculation import checkpoint_document, read_complete_lines, restore
from kazoo.exceptions import BadVersionError
from storage import commit_checked


class RangeSource:
    def __init__(self, body):
        self.body = body

    def get_object(self, *, Bucket, Key, Range):
        left, right = map(int, Range.removeprefix("bytes=").split("-"))
        return {"Body": io.BytesIO(self.body[left:right + 1])}


class ChunkReadingTests(unittest.TestCase):
    def read_chunks(self, rows, width, final_newline):
        body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows).encode("utf-8")
        if final_newline:
            body += b"\n"
        source = RangeSource(body)
        actual = []
        for start in range(0, len(body), width):
            payload = read_complete_lines(source, "bucket", "input", start,
                                          min(start + width - 1, len(body) - 1), len(body))
            actual.extend(json.loads(line) for line in payload.splitlines() if line.strip())
        self.assertEqual(rows, actual)

    def test_adjacent_ranges_do_not_duplicate_records_or_split_utf8(self):
        rows = [{"vacancy_id": i, "skills": ["линукс", "docker"]} for i in range(5)]
        for width in (1, 7, 31, 67):
            for newline in (False, True):
                with self.subTest(width=width, final_newline=newline):
                    self.read_chunks(rows, width, newline)

    def test_records_larger_than_a_read_extension(self):
        rows = [{"vacancy_id": 1, "skills": ["x" * 150000, "python"]},
                {"vacancy_id": 2, "skills": ["docker", "linux"]}]
        self.read_chunks(rows, 65536, False)


class CheckpointTests(unittest.TestCase):
    def test_restore_preserves_reduced_counts_and_completed_chunks(self):
        expected = Counter({("docker", "linux"): 7, ("bash", "python"): 2})
        saved = checkpoint_document(expected, 9, {0, 2}, "input-sha", 1024)
        loaded = restore(json.loads(json.dumps(saved)), "input-sha", 1024, 4)
        self.assertEqual(loaded, (expected, 9, {0, 2}))

    def test_foreign_input_partition_and_duplicate_chunks_are_rejected(self):
        saved = checkpoint_document(Counter(), 0, {0}, "input-sha", 1024)
        for checksum, width in (("other-sha", 1024), ("input-sha", 2048)):
            with self.assertRaises(ValueError):
                restore(saved, checksum, width, 4)
        for completed in ([0, 0], [-1], [4]):
            with self.assertRaises(ValueError):
                restore(dict(saved, completed=completed), "input-sha", 1024, 4)

    def test_transaction_error_values_are_raised(self):
        class FailedTransaction:
            def commit(self):
                return [BadVersionError()]

        with self.assertRaises(BadVersionError):
            commit_checked(FailedTransaction())


if __name__ == "__main__":
    unittest.main()
