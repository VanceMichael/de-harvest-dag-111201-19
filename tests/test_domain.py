import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        self.assertEqual(topological(['a', 'b'], {'b': ['a']}), ['a', 'b'])
        with self.assertRaises(ValueError):
            topological(['a', 'b'], {'a': ['b'], 'b': ['a']})
        with self.assertRaises(ValueError):
            topological(['a'], {'a': ['missing']})

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
