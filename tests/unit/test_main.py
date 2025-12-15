#!/usr/bin/env python3
# import unittest
# from click.testing import CliRunner
# from muse_tool import __main__ as MOD

# class ThisTestCase(unittest.TestCase):
#     def setUp(self) -> None:
#         self.runner = CliRunner()

#     def test_pass(self):
#         result = self.runner.invoke(MOD.main)
#         self.assertEqual(result.exit_code, 0)

import subprocess
import sys
import unittest


class ThisTestCase(unittest.TestCase):
    def test_help_exits_zero(self):
        proc = subprocess.run(
            [sys.executable, "-m", "muse_tool", "--help"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("usage:", proc.stdout.lower())


if __name__ == "__main__":
    unittest.main()


# __END__
