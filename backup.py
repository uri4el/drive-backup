from __future__ import print_function

import hashlib
import math
import mimetypes
import os
import os.path
import queue
import random
import socket
import stat
import threading
import time
import traceback
from datetime import datetime

from anytree import Node, Resolver, ResolverError, PreOrderIter
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

import configurations

socket.setdefaulttimeout(600)  # set timeout to 10 minutes

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']
Node.separator = os.sep


class Logger:
    _print_lock = threading.Lock()

    @staticmethod
    def debug(msg):
        if configurations.LOG_LEVEL.upper() != 'DEBUG':
            return
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        with Logger._print_lock:
            print(f'[{dt_string}] [DEBUG] [{threading.current_thread().name}] - {msg}')

    @staticmethod
    def info(msg):
        if configurations.LOG_LEVEL.upper() != 'DEBUG' and configurations.LOG_LEVEL.upper() != 'INFO':
            return
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        with Logger._print_lock:
            print(f'[{dt_string}] [INFO] [{threading.current_thread().name}] - {msg}')

    @staticmethod
    def error(msg):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        with Logger._print_lock:
            print(f'[{dt_string}] [ERROR] [{threading.current_thread().name}] - {msg}')

    @staticmethod
    def print_progress(progress):
        Logger.info("[%-20s] %d%%" % ('=' * round(progress * 20), round(100 * progress)))


class FileChangeEventHandler(FileSystemEventHandler):
    def __init__(self, on_created_event_handler, on_file_deleted_event_handler, on_file_modified_event_handler, on_file_moved_event_handler):
        self._on_created_event_handler = on_created_event_handler
        self._on_deleted_event_handler = on_file_deleted_event_handler
        self._on_modified_event_handler = on_file_modified_event_handler
        self._on_moved_event_handler = on_file_moved_event_handler

    def on_created(self, event):
        for i in range(configurations.UPLOAD_RETRIES):
            try:
                self._on_created_event_handler(event.src_path, not event.is_directory)
                break
            except Exception as e:
                if i == configurations.UPLOAD_RETRIES - 1:
                    Logger.error(f'Failed to backup file {event.src_path} with exception: {e}')
                else:
                    Logger.debug(f'file upload failed (try #{i}) {event.src_path}')

    def on_deleted(self, event):
        for i in range(configurations.UPLOAD_RETRIES):
            try:
                self._on_deleted_event_handler(event.src_path, not event.is_directory)
                break
            except Exception as e:
                if i == configurations.UPLOAD_RETRIES - 1:
                    Logger.error(f'Failed to delete file {event.src_path} with exception: {e}')
                else:
                    Logger.debug(f'file delete failed (try #{i}) {event.src_path}')


    def on_modified(self, event):
        for i in range(configurations.UPLOAD_RETRIES):
            try:
                self._on_modified_event_handler(event.src_path, not event.is_directory)
                break
            except Exception as e:
                if i == configurations.UPLOAD_RETRIES - 1:
                    Logger.error(f'Failed to update modified file {event.src_path} with exception: {e}')
                else:
                    Logger.debug(f'file update failed (try #{i}) {event.src_path}')

    def on_moved(self, event):
        for i in range(configurations.UPLOAD_RETRIES):
            try:
                self._on_moved_event_handler(event.src_path, event.dest_path, not event.is_directory)
                break
            except Exception as e:
                if i == configurations.UPLOAD_RETRIES - 1:
                    Logger.error(f'Failed to move file from {event.src_path} to {event.dest_path} with exception: {e}')
                else:
                    Logger.debug(f'file move from {event.src_path} to {event.dest_path} failed (try #{i})')


class SyncWorker(threading.Thread):

    SyncWorkerCounter = 0

    def __init__(self, work_locker, work_active, work_queue, work_to_do):
        super(SyncWorker, self).__init__()
        self._work_locker = work_locker
        self._work_active = work_active
        self._work_queue = work_queue
        self._work_to_do = work_to_do

        SyncWorker.SyncWorkerCounter += (SyncWorker.SyncWorkerCounter + 1) % configurations.WORKERS
        self.name = f'SyncWorker-{SyncWorker.SyncWorkerCounter}'

    def run(self):
        while self._work_active:
            item = self._work_queue.get()
            if item is None:
                break

            try:
                self._work_locker.acquire_one()
                self._work_to_do(*item)
            except Exception as e:
                Logger.error(f'Failed to sync file {item} with exception: {e}')
            finally:
                self._work_locker.release_one()

            self._work_queue.task_done()


class SyncWorkerLocker:
    def __init__(self):
        self._semaphore = threading.Semaphore(configurations.WORKERS)

    def __enter__(self):
        self.acquire_all()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.release_all()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return False

    def acquire_one(self):
        self._semaphore.acquire()

    def release_one(self):
        self._semaphore.release()

    def acquire_all(self):
        for _ in range(configurations.WORKERS):
            self._semaphore.acquire()

    def release_all(self):
        for _ in range(configurations.WORKERS):
            self._semaphore.release()


class BackupEngine:
    def __init__(self, paths_to_backup: list, excluded_paths: list, backup_destination: str):
        self._file_changed_observer = None
        self._work_active = False
        self._upload_queue = None
        self._files_tree = None
        self._total_files_size = 0
        self._uploaded_files_size = 0
        self._upload_progress = 0
        self._destination_folder_id = None
        # TODO: verify logic for support excluding single file
        self._excluded_paths = [os.path.abspath(f) for f in excluded_paths if os.path.exists(f)]
        # TODO: remove nested paths
        self._paths_to_backup = [os.path.abspath(p) for p in paths_to_backup if self._is_valid_path(p)]
        self._backup_destination = backup_destination
        self._workers = []
        self._workers_locker = None

        if not self._backup_destination:
            raise ValueError(f'Empty backup destination supplied: {backup_destination}')
        if len(self._paths_to_backup) == 0:
            raise ValueError(f'Cannot find valid non-excluded path to backup here: {paths_to_backup} (excluded paths: {excluded_paths})')

    def __enter__(self):
        self._workers = []
        self._upload_queue = queue.Queue()
        self._work_active = True
        self._total_files_size = 0
        self._uploaded_files_size = 0
        self._upload_progress = 0
        self._create_workers()

        self._scan_all_paths()

        event_handler = FileChangeEventHandler(self._on_file_created, self._on_file_deleted, self._on_file_modified, self._on_file_moved)
        self._file_changed_observer = Observer()
        # TODO: check what happen if path was deleted and you schedlue event handler for it
        for path in self._paths_to_backup:
            self._file_changed_observer.schedule(event_handler, path, recursive=True)
        self._file_changed_observer.start()

        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._file_changed_observer.stop()
        self._file_changed_observer.join()
        self._file_changed_observer = None

        self._work_active = False
        for i in range(configurations.WORKERS):
            self._upload_queue.put(None)

        for thread in self._workers:
            thread.join()

        with self._upload_queue.mutex:
            self._total_files_size = 0
            self._uploaded_files_size = 0
            self._upload_progress = 0

        self._files_tree = None
        self._upload_queue = None
        self._workers = []
        self._workers_locker = None
        self._destination_folder_id = None

        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            return False
        return True

    def _is_path_excluded(self, path):
        path = os.path.abspath(path)
        for excluded_path in self._excluded_paths:
            if path == excluded_path:
                return True
            if os.path.normcase(path).startswith(os.path.normcase(excluded_path) + os.sep):
                return True
        return False

    def _on_file_created(self, path, is_file):
        Logger.debug(f'{"file" if is_file else "folder"} created {path}')

        if not self._is_valid_path(path):
            Logger.debug(f'{"file" if is_file else "folder"} skipped since it invalid {path}')
            return

        with self._workers_locker:
            node_resolver = Resolver()
            parent_node = self._files_tree
            folders_in_path = path.strip(os.sep).split(os.sep)
            for i in range(len(folders_in_path)):
                try:
                    parent_node = node_resolver.get(parent_node.root, os.sep + os.sep.join(folders_in_path[:i + 1]))
                except:
                    parent_node = Node(folders_in_path[i], parent=parent_node, size=0, drive_id=None, is_file=False)
            parent_node.is_file = is_file

            if parent_node.is_file:
                self._upload_queue.put([parent_node])
            else:
                parent_node.drive_id = self._get_or_create_folder(parent_node.name,
                                                                           parent_node.parent.drive_id)

    def _on_file_deleted(self, path, is_file):
        Logger.debug(f'{"file" if is_file else "folder"} deleted {path}')
        if self._is_path_excluded(path):
            return
        service = build('drive', 'v3', credentials=self._get_creds())
        with self._workers_locker:
            node_resolver = Resolver()
            parent_node = self._files_tree
            # TODO: move it to separate method
            folders_in_path = path.strip(os.sep).split(os.sep)
            for i in range(len(folders_in_path)):
                try:
                    parent_node = node_resolver.get(parent_node.root, os.sep + os.sep.join(folders_in_path[:i + 1]))
                except:
                    parent_node = None
                    break

            if parent_node:
                name = parent_node.name
                parent_node_drive_id = parent_node.parent.drive_id
                parent_node.parent.children = [n for n in parent_node.parent.children if n != parent_node]
                if parent_node_drive_id is not None:
                    results = service.files().list(
                        pageSize=1, fields="files",
                        q=f'name = "{name}" and trashed = false and parents in "{parent_node_drive_id}"').execute()
                    items = results.get('files', [])
                    if len(items) > 0:
                        item = items[0]
                        service.files().delete(fileId=item['id']).execute()
                        Logger.debug(
                            f'file {path} deleted from drive.')

    def _on_file_modified(self, path, is_file):
        Logger.debug(f'{"file" if is_file else "folder"} modified {path}')

        if not self._is_valid_path(path):
            Logger.debug(f'{"file" if is_file else "folder"} skipped since it invalid {path}')
            self._on_file_deleted(path, is_file)
            return

        if is_file:
            with self._workers_locker:
                node_resolver = Resolver()
                parent_node = self._files_tree
                folders_in_path = path.strip(os.sep).split(os.sep)
                for i in range(len(folders_in_path)):
                    try:
                        parent_node = node_resolver.get(parent_node.root, os.sep + os.sep.join(folders_in_path[:i + 1]))
                    except:
                        parent_node = Node(folders_in_path[i], parent=parent_node, size=0, drive_id=None, is_file=False)
                parent_node.is_file = True
                self._upload_queue.put([parent_node])

    def _on_file_moved(self, src_path, dest_path, is_file):
        Logger.debug(f'{"file" if is_file else "folder"} moved from {src_path} to {dest_path}')
        self._on_file_deleted(src_path, is_file)
        self._on_file_created(dest_path, is_file)


    def _create_workers(self):
        self._workers_locker = SyncWorkerLocker()
        for i in range(configurations.WORKERS):
            thread = SyncWorker(self._workers_locker, self._work_active, self._upload_queue, self._upload_file)
            self._workers.append(thread)
            thread.start()

    def _get_creds(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # TODO: make sure it works after 7 days https://stackoverflow.com/questions/66058279/token-has-been-expired-or-revoked-google-oauth2-refresh-token-gets-expired-i
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=8080, prompt='consent')
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        return creds

    def _md5(self, file_name):
        hash_md5 = hashlib.md5()
        with open(file_name, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _convert_size(self, size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def _is_valid_path(self, path):
        path = os.path.abspath(path)
        try:
            return os.path.exists(path) \
                   and not bool(os.stat(path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN) \
                   and not os.path.basename(os.path.abspath(path)).startswith('.') \
                   and not os.path.islink(path) \
                   and os.access(path, os.R_OK) \
                   and (path.count(os.sep) > 0 or os.path.isfile(path)) \
                   and not self._is_path_excluded(path)
        except:
            return False

    def _scan_path(self, path):
        #TODO: test singel file backup, single file excluded path, test nested backup folders

        if not self._is_valid_path(path):
            Logger.error(f'path does not exist {path}')
            return

        node_resolver = Resolver()
        total_size = 0
        parent_node = self._files_tree
        current_path = ''
        for folder_name_in_path in path.strip(os.sep).split(os.sep):
            current_path += os.sep + folder_name_in_path
            try:
                parent_node = node_resolver.get(parent_node.root, current_path)
            except:
                parent_node = Node(folder_name_in_path, parent=parent_node, size=0, drive_id=None, is_file=False)

        if os.path.isfile(path):
            parent_node.is_file = True
            parent_node.file_size = os.path.getsize(path)

        for root, folders, files in os.walk(path, topdown=True):
            root = root.rstrip(os.sep)
            folders[:] = [d for d in folders if self._is_valid_path(root + os.sep + d)]
            random.shuffle(folders)
            # TODO: search for os.sep and strip
            try:
                parent_node = node_resolver.get(parent_node.root, os.sep + root.lstrip(os.sep))
            except ResolverError:
                parent_node = node_resolver.get(parent_node.root,
                                                os.sep + os.path.abspath(root + os.sep + '..').lstrip(os.sep))
                parent_node = Node(os.path.basename(root), parent=parent_node, size=0, drive_id=None, is_file=False)

            for name in files:
                # TODO: only do it for new created nodes.
                file_path = root + os.sep + name
                if self._is_valid_path(file_path):
                    file_size = os.path.getsize(file_path)
                    parent_node.size += file_size
                    Node(name, parent=parent_node, size=file_size, drive_id=None, is_file=True)
            total_size += parent_node.size
        return parent_node.root, total_size

    def _get_node_path(self, node):
        return ('' if os.sep == '\\' else os.sep) + \
               os.sep.join([p.name for p in node.ancestors]) + \
               os.sep + node.name

    def _upload_file(self, path_node):
        file_checksum = None
        file_path = self._get_node_path(path_node)
        service = build('drive', 'v3', credentials=self._get_creds())

        if path_node.parent.drive_id is not None:
            results = service.files().list(
                pageSize=1, fields="files",
                q=f'name = "{path_node.name}" and trashed = false and parents in "{path_node.parent.drive_id}"').execute()
            items = results.get('files', [])
            if len(items) > 0:
                item = items[0]
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    service.files().delete(fileId=item['id']).execute()
                    Logger.debug(f'file {file_path} found on drive as folder. folder removed, will upload the file now.')
                else:
                    path_node.drive_id = item['id']
                    file_checksum = item['md5Checksum']

        if path_node.parent.drive_id is not None and self._is_valid_path(file_path):
            for i in range(configurations.UPLOAD_RETRIES):
                try:
                    media = MediaFileUpload(
                        file_path,
                        mimetype=mimetypes.MimeTypes().guess_type(path_node.name)[0],
                        resumable=path_node.size > 1 * 1024 * 1024)

                    if path_node.drive_id is None:
                        file_metadata = {'name': path_node.name, 'parents': [path_node.parent.drive_id]}
                        results = service.files().create(body=file_metadata,
                                                         media_body=media,
                                                         fields='id').execute()
                        path_node.drive_id = results.get('id')
                        Logger.debug(f'file uploaded {file_path}')
                    elif file_checksum != self._md5(file_path):
                        service.files().update(
                            fileId=path_node.drive_id,
                            body={},
                            media_body=media).execute()
                        Logger.debug(f'file updated {file_path}')
                    else:
                        Logger.debug(f'file skipped. file already exist {file_path}')

                    break
                except Exception as e:
                    if i == configurations.UPLOAD_RETRIES - 1:
                        Logger.error(f'Failed to backup file {file_path} with exception: {e}')
                    else:
                        Logger.debug(f'file upload failed (try #{i}) {file_path}')
        elif path_node.drive_id is not None:
            service.files().delete(fileId=path_node.drive_id).execute()
            Logger.debug(f'file deleted {file_path}')
        else:
            Logger.debug(f'file skipped (failed to sync parent or invalid file) {file_path}')

        with self._upload_queue.mutex:
            self._uploaded_files_size += path_node.size
            if (self._uploaded_files_size / (self._total_files_size or 1) - self._upload_progress) * 100 >= configurations.PROGRESS_INTERNALS:
                self._upload_progress = self._uploaded_files_size / (self._total_files_size or 1)
                Logger.print_progress(self._upload_progress)

    def _validate_usage(self):
        if self._upload_queue is None:
            raise RuntimeError('Must be called inside with statement')

    def _get_or_create_folder(self, folder_name, parent_folder_id):
        service = build('drive', 'v3', credentials=self._get_creds())

        results = service.files().list(
            pageSize=1, fields="nextPageToken, files",
            q=f'name = "{folder_name}" and trashed = false and parents in "{parent_folder_id}"').execute()
        items = results.get('files', [])
        folder_id = None
        if not items or len(items) == 0:
            folder_metadata = {
                'name': folder_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder_id = service.files().create(body=folder_metadata,
                                                     fields='id').execute().get('id')
        else:
            folder_id = items[0]['id']

        return folder_id

    def _scan_all_paths(self):
        random.shuffle(self._paths_to_backup)
        # TODO make sure there isn't overlap, remove last slash, case insensitive in paths for windows only, single file as backup folder

        self._destination_folder_id = self._get_or_create_folder(self._backup_destination, 'root')
        Logger.info(f'backup folder id on Drive: {self._backup_destination}')

        Logger.info(f'Scan files in paths {self._paths_to_backup}...')
        for path in self._paths_to_backup:
            self._files_tree, total_files_size = self._scan_path(path)
            self._total_files_size += total_files_size

    def backup_paths(self):
        '''Uploads paths and all it's content to Drive
        '''
        self._validate_usage()
        service = build('drive', 'v3', credentials=self._get_creds())
        node_resolver = Resolver()

        self._uploaded_files_size = 0
        self._upload_progress = 0
        Logger.info(f'Start backup paths {self._paths_to_backup} ({self._convert_size(self._total_files_size)})...')
        Logger.print_progress(0)

        if self._files_tree is None:
            Logger.info('Nothing to sync')
        else:
            for path_node in PreOrderIter(self._files_tree):
                if path_node.is_file:
                    self._upload_queue.put([path_node])
                else:
                    try:
                        if path_node.drive_id is None:
                            path_node.drive_id = self._get_or_create_folder(path_node.name,
                                                                            self._destination_folder_id if path_node.is_root else path_node.parent.drive_id)
                        next_page_token = None
                        while True:
                            results = service.files().list(
                                pageSize=configurations.DRIVE_SCAN_PAGE_SIZE, fields="nextPageToken, files",
                                pageToken=next_page_token,
                                q=f'trashed = false and parents in "{path_node.drive_id}"').execute()
                            items = results.get('files', [])

                            for item in items:
                                try:
                                    node_resolver.get(path_node, item['name'])
                                except ResolverError:
                                    service.files().delete(fileId=item['id']).execute()
                                    Logger.debug(f'path deleted {self._get_node_path(path_node) + os.sep + item["name"]}')

                            next_page_token = results.get('nextPageToken', None)
                            if next_page_token is None:
                                break
                    except Exception as e:
                        Logger.error(f'Failed to sync folder {path_node.name} with exception: {e}')

            # Wait all workers to empty the queue
            self._upload_queue.join()

        Logger.info('End Sync')

    def monitor_filesystem(self, block_time=None):
        if block_time is None:
            input("Monitor filesystem to sync any file change...\nPress Enter to stop backup.")
        else:
            time.sleep(block_time)


if __name__ == '__main__':
    with BackupEngine(configurations.PATHS_TO_BACKUP, configurations.EXCLUDED_PATHS, configurations.BACKUP_DESTINATION) as backup_engine:
        while True:
            backup_engine.backup_paths()
            backup_engine.monitor_filesystem(configurations.FULL_SYNC_EVERY)
