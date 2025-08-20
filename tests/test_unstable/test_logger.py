import logging
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cognite.extractorutils.unstable.core.logger import RobustFileHandler


class TestRobustFileHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.log_dir = Path(self.test_dir, "logs", "nested", "dir")
        self.log_file = Path(self.log_dir, "test.log")

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_create_dirs(self) -> None:
        """Test that directories are created automatically"""
        self.assertFalse(os.path.exists(self.log_dir))

        handler = RobustFileHandler(
            filename=self.log_file,
            when="midnight",
            utc=True,
            backupCount=3,
            create_dirs=True,
        )

        self.assertTrue(os.path.exists(self.log_dir))

        handler.close()

    def test_no_create_dirs(self) -> None:
        """Test that create_dirs=False doesn't create directories"""
        with self.assertRaises((OSError, FileNotFoundError)):
            RobustFileHandler(
                filename=self.log_file,
                when="midnight",
                utc=True,
                backupCount=3,
                create_dirs=False,
            )

    @patch("pathlib.Path.mkdir")
    def test_permission_error(self, mock_mkdir: unittest.mock.MagicMock) -> None:
        """Test handling of permission errors"""
        mock_mkdir.side_effect = PermissionError("Permission denied")

        with self.assertRaises(PermissionError):
            RobustFileHandler(
                filename=self.log_file,
                when="midnight",
                utc=True,
                backupCount=3,
                create_dirs=True,
            )

    def test_rotation(self) -> None:
        """Test log rotation (simplified test)"""
        handler = RobustFileHandler(
            filename=self.log_file,
            when="S",
            utc=True,
            backupCount=3,
            create_dirs=True,
        )

        logger = logging.getLogger("test_rotation")
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        logger.info("Test message 1")

        self.assertTrue(os.path.exists(self.log_file))

        time.sleep(2)

        logger.info("Test message 2")

        self.assertTrue(os.path.exists(self.log_file))

        rotated_files = [f for f in os.listdir(self.log_dir) if f.startswith(os.path.basename(self.log_file))]
        self.assertEqual(len(rotated_files), 2)

        handler.close()
