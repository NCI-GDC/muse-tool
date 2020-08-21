#!/usr/bin/env python3

import pathlib
import subprocess
import unittest
from types import SimpleNamespace
from unittest import mock

from muse_tool import multi_muse as MOD


class ThisTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.mocks = SimpleNamespace(subprocess=mock.MagicMock(spec_set=subprocess),)

    def setup_popen(self, stdout=None, stderr=None):
        stdout = stdout or b''
        stderr = stderr or b''
        popen_instance = mock.MagicMock(subprocess.Popen)
        self.mocks.subprocess.Popen.return_value = popen_instance
        popen_instance.communicate.return_value = (stdout, stderr)
        return popen_instance


class Test_subprocess_commands_pipe(ThisTestCase):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_Popen_called_as_expected(self):
        mock_popen = self.setup_popen()
        cmd = MOD.CMD_STR
        MOD.subprocess_commands_pipe(cmd, di=self.mocks)
        self.mocks.subprocess.Popen.assert_called_once_with(
            MOD.shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        mock_popen.communicate.assert_called_once_with(timeout=3600)


# __END__
