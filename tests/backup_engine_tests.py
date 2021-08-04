import json
import os
import shutil
import traceback
import socket

import pytest
import copy

import backup
from tests.drive_mock import DriveMock
from tests.logger_mock import LoggerMock
from anytree import RenderTree

class DummyFileGenerator:
    os.chdir('..')
    TEST_DATA_FOLDER_PATH = 'test_data'

    def __init__(self):
        try:
            shutil.rmtree(DummyFileGenerator.TEST_DATA_FOLDER_PATH)
        except:
            pass

    def __enter__(self):
        os.makedirs(DummyFileGenerator.TEST_DATA_FOLDER_PATH + '/data_folder1/data_folder2')
        self._create_file(DummyFileGenerator.TEST_DATA_FOLDER_PATH + '/data_file1.txt')
        self._create_file(DummyFileGenerator.TEST_DATA_FOLDER_PATH + '/data_file1.png')
        self._create_file(DummyFileGenerator.TEST_DATA_FOLDER_PATH + '/data_folder1/data_file2.docx')
        self._create_file(DummyFileGenerator.TEST_DATA_FOLDER_PATH + '/data_folder1/data_file2.html')

        return self

    def __exit__(self, exc_type, exc_value, tb):
        shutil.rmtree(DummyFileGenerator.TEST_DATA_FOLDER_PATH)
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return False

    def _create_file(self, path, content=None):
        if not content:
            content = os.path.abspath(path)
        os.umask(0)
        with open(os.open(path, os.O_CREAT | os.O_WRONLY, 0o777), 'w') as fh:
            fh.write(content)

    def get_non_existent_folder(self, absolute_path=True):
        path = f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/non_existent_folder'
        return os.path.abspath(path) if absolute_path else path

    def get_non_existent_file(self, absolute_path=True):
        path = f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/non_existent_file.txt'
        return os.path.abspath(path) if absolute_path else path

    def get_base_folder_path(self, absolute_path=True):
        path = DummyFileGenerator.TEST_DATA_FOLDER_PATH
        return os.path.abspath(path) if absolute_path else path

    def get_sub_folder_path(self, absolute_path=True):
        path = f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1'
        return os.path.abspath(path) if absolute_path else path

    def get_file_path_from_base_folder(self, absolute_path=True):
        path = f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_file1.txt'
        return os.path.abspath(path) if absolute_path else path

    def get_file_path_from_sub_folder(self, absolute_path=True):
        path = f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1/data_file2.docx'
        return os.path.abspath(path) if absolute_path else path

    def get_folder_path_from_sub_folder(self, absolute_path=True):
        path = f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1/data_folder2'
        return os.path.abspath(path) if absolute_path else path

    def add_folder_to_base_folder(self, folder_name):
        os.makedirs(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/{folder_name}', exist_ok=True)

    def add_folder_to_sub_folder(self, folder_name):
        os.makedirs(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1/{folder_name}', exist_ok=True)

    def add_file_to_base_folder(self, file_name):
        self._create_file(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/{file_name}')

    def add_file_to_sub_folder(self, file_name):
        self._create_file(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1/{file_name}')

    def remove_folder_from_base_folder(self):
        shutil.rmtree(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1')

    def remove_folder_from_sub_folder(self):
        shutil.rmtree(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1/data_folder2')

    def remove_file_from_base_folder(self):
        os.remove(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_file1.txt')

    def remove_file_from_sub_folder(self):
        os.remove(f'{DummyFileGenerator.TEST_DATA_FOLDER_PATH}/data_folder1/data_file2.docx')


class TestBackupEngine:

    @pytest.fixture(autouse=True)
    def disable_internet_connection(self, monkeypatch):
        def guard(*args, **kwargs):
            raise ConnectionError("Internet connection disabled for testing")

        socket.socket = guard

    @pytest.fixture(autouse=True)
    def logger_mock(self, monkeypatch):
        logger_mock = LoggerMock(monkeypatch)
        yield logger_mock
        assert not logger_mock.is_error_occurred()

    @pytest.fixture(autouse=True)
    def drive_mock(self, monkeypatch):
        with DummyFileGenerator():
            return DriveMock(monkeypatch, [DummyFileGenerator.TEST_DATA_FOLDER_PATH])

    def test_backup_paths_sanity(self, drive_mock):
        with DummyFileGenerator() as tree:
            # TESTCASE no change sync
            original_tree = copy.deepcopy(drive_mock.files_tree)
            with backup.BackupEngine([tree.get_base_folder_path()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree, compare_drive_id=True), \
                    f'already synced folder has changed after backup.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            # TESTCASE sync from scratch
            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            with backup.BackupEngine([tree.get_base_folder_path()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                    f'sync folder to empty drive upload unexpected files.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

                # TESTCASE no re-upload after re-calling backup_paths when no need
                original_tree = copy.deepcopy(drive_mock.files_tree)
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree, compare_drive_id=True), \
                    f'already synced folder has changed after second call to backup.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            # TESTCASE no re-upload after re-calling backup_paths and __enter__/__exit__ when no need
            with backup.BackupEngine([tree.get_base_folder_path()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree, compare_drive_id=True), \
                    f'already synced folder has changed after second call to backup in new with block.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            # TESTCASE single file sync
            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            with backup.BackupEngine([tree.get_file_path_from_base_folder()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                original_tree = drive_mock.get_simulated_files_tree([tree.get_file_path_from_base_folder()])
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                    f'---------.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            # TESTCASE sync containing folder after inner file was already synced
            with backup.BackupEngine([tree.get_base_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                original_tree = drive_mock.get_simulated_files_tree([tree.get_base_folder_path()])
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                    f'---------.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

    def test_backup_paths_twice(self, drive_mock):
        with DummyFileGenerator() as tree:
            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            # TESTCASE 2 calls to backup_paths with same folder
            be = backup.BackupEngine([tree.get_base_folder_path()], [],
                                DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            with be:
                be.backup_paths()
            tree.remove_file_from_base_folder()
            with be:
                be.backup_paths()
            original_tree = drive_mock.get_simulated_files_tree([tree.get_base_folder_path()])
            assert drive_mock.compare_drive_trees(
                original_tree, drive_mock.files_tree), \
                f'---------.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

    def test_backup_paths_error(self, logger_mock):
        with DummyFileGenerator() as tree:
            be = backup.BackupEngine([tree.get_base_folder_path()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            # TESTCASE exception when calling backup_path without with block
            with pytest.raises(RuntimeError):
                be.backup_paths()

    # def test_get_creds(self):
    #     with DummyFileGenerator() as tree:
    #         be = backup.BackupEngine([tree.get_file_path_from_base_folder()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
    #         creds = be._get_creds()
    #         drive_mock.Credentials.from_authorized_user_file = lambda a, b: 'uriel'
    #         be = backup.BackupEngine([tree.get_file_path_from_base_folder()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)

    def test_init_sanity(self):
        with DummyFileGenerator() as tree:
            be = backup.BackupEngine([tree.get_file_path_from_base_folder()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_base_folder_path()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_base_folder_path(), tree.get_non_existent_file()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_base_folder_path()],
                                     [tree.get_non_existent_folder(), tree.get_non_existent_file()],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_base_folder_path()], [tree.get_sub_folder_path()],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_base_folder_path(), tree.get_base_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_file_path_from_base_folder(), tree.get_file_path_from_base_folder()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            be = backup.BackupEngine([tree.get_sub_folder_path(), tree.get_file_path_from_base_folder()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            with be:
                pass

    def test_init_error(self):
        with DummyFileGenerator() as tree:
            with pytest.raises(ValueError):
                be = backup.BackupEngine([], [], '')
            with pytest.raises(ValueError):
                be = backup.BackupEngine([], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            with pytest.raises(ValueError):
                be = backup.BackupEngine([tree.get_base_folder_path()], [tree.get_sub_folder_path()], '')
            with pytest.raises(ValueError):
                be = backup.BackupEngine([tree.get_non_existent_file(), tree.get_non_existent_folder()], [],
                                         DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            with pytest.raises(ValueError):
                be = backup.BackupEngine(
                    [tree.get_non_existent_file(absolute_path=True), tree.get_non_existent_folder(absolute_path=True)],
                    [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            with pytest.raises(ValueError):
                be = backup.BackupEngine([tree.get_sub_folder_path()], [tree.get_base_folder_path()], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)

    def test_file_watcher_sync(self, drive_mock):
        with DummyFileGenerator() as tree:
            with backup.BackupEngine([tree.get_base_folder_path()], [], DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                tree.remove_file_from_base_folder()
                expected_tree = drive_mock.get_simulated_files_tree([DummyFileGenerator.TEST_DATA_FOLDER_PATH])
                be.monitor_filesystem(1)
                assert drive_mock.compare_drive_trees(expected_tree, drive_mock.files_tree), \
                    f'out of sync after file removed.\nExpected:\n{RenderTree(expected_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

    def test_is_path_excluded_sanity(self):
        with DummyFileGenerator() as tree:
            be = backup.BackupEngine([tree.get_base_folder_path()], [tree.get_sub_folder_path()],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME)
            assert not be._is_path_excluded(tree.get_base_folder_path(absolute_path=True))
            assert not be._is_path_excluded(tree.get_base_folder_path(absolute_path=False))
            assert not be._is_path_excluded(tree.get_file_path_from_base_folder(absolute_path=True))
            assert not be._is_path_excluded(tree.get_file_path_from_base_folder(absolute_path=False))
            assert be._is_path_excluded(tree.get_folder_path_from_sub_folder(absolute_path=True))
            assert be._is_path_excluded(tree.get_folder_path_from_sub_folder(absolute_path=False))
            assert not be._is_path_excluded(tree.get_non_existent_file(absolute_path=True))
            assert not be._is_path_excluded(tree.get_non_existent_file(absolute_path=False))
            assert not be._is_path_excluded(tree.get_non_existent_folder(absolute_path=True))
            assert not be._is_path_excluded(tree.get_non_existent_folder(absolute_path=False))
            assert be._is_path_excluded(tree.get_sub_folder_path(absolute_path=True))
            assert be._is_path_excluded(tree.get_sub_folder_path(absolute_path=False))
            assert be._is_path_excluded(tree.get_file_path_from_sub_folder(absolute_path=True))
            assert be._is_path_excluded(tree.get_file_path_from_sub_folder(absolute_path=False))

    def test_overlapped_paths(self, drive_mock):
        with DummyFileGenerator() as tree:
            original_tree = copy.deepcopy(drive_mock.files_tree)
            with backup.BackupEngine([tree.get_base_folder_path(), tree.get_sub_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree, compare_drive_id=True), \
                    f'already synced folder has changed after backup when supplied 2 overlapped paths for backup (order #1).\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            with backup.BackupEngine([tree.get_sub_folder_path(), tree.get_base_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree, compare_drive_id=True), \
                    f'already synced folder has changed after backup when supplied 2 overlapped paths for backup (order #2).\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            with backup.BackupEngine([tree.get_base_folder_path(), tree.get_sub_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                    f'empty folder was synced unexpectedly when supplied 2 overlapped paths for sync.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            with backup.BackupEngine([tree.get_sub_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
            with backup.BackupEngine([tree.get_base_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
            assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                f'empty folder was synced unexpectedly when supplied 2 overlapped paths (sub folder first) for sync.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            with backup.BackupEngine([tree.get_base_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
            with backup.BackupEngine([tree.get_sub_folder_path()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
            assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                f'empty folder was synced unexpectedly when supplied 2 overlapped paths (parent folder first) for sync.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

            drive_mock.files_tree = drive_mock.get_simulated_files_tree()
            with backup.BackupEngine([tree.get_sub_folder_path(), tree.get_file_path_from_base_folder()], [],
                                     DriveMock.TEST_DATA_FOLDER_DRIVE_NAME) as be:
                be.backup_paths()
                original_tree = drive_mock.get_simulated_files_tree([tree.get_sub_folder_path(), tree.get_file_path_from_base_folder()])
                assert drive_mock.compare_drive_trees(original_tree, drive_mock.files_tree), \
                    f'---------.\nBefore sync:\n{RenderTree(original_tree)}\nAfter sync:\n{RenderTree(drive_mock.files_tree)}'

    def test_is_path_excluded_error(self):
        pass
