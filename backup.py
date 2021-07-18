from __future__ import print_function

import hashlib
import logging
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

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler
from anytree import Node, Resolver, ResolverError, PreOrderIter

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
    def __init__(self):
        pass

    def on_created(self, event):
        Logger.debug(f'{"folder" if event.is_directory else "file"} created {event.src_path}')

    def on_deleted(self, event):
        Logger.debug(f'{"folder" if event.is_directory else "file"} deleted {event.src_path}')

    def on_modified(self, event):
        Logger.debug(f'{"folder" if event.is_directory else "file"} modified {event.src_path}')

    def on_moved(self, event):
        Logger.debug(f'{"folder" if event.is_directory else "file"} moved from {event.src_path} to {event.dest_path}')

class SyncWorker(threading.Thread):
    def __init__(self, name, context):
        super(SyncWorker, self).__init__()
        self.name = name
        self.mutex = threading.Lock()
        self.context = context

    def run(self):
        while self.context._workers_active:
            item = self.context._upload_queue.get()
            if item is None:
                break
            try:
                with self.mutex:
                    self.context._upload_file(*item)
            except Exception as e:
                Logger.error(f'Failed to sync file {item} with exception: {e}')

            self.context._upload_queue.task_done()


class SyncWorkerLocker:
    def __init__(self, sync_workers):
        self.sync_workers = sync_workers

    def __enter__(self):
        for worker in self.sync_workers:
            worker.mutex.acquire()

    def __exit__(self, exc_type, exc_value, tb):
        for worker in self.sync_workers:
            worker.mutex.release()

        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return True


class BackupEngine:
    def __init__(self):
        self._file_changed_observer = None
        self._workers_active = False
        self._upload_queue = None
        self._files_to_upload = None
        self._total_files_size = 0
        self._uploaded_files_size = 0
        self._upload_progress = 0
        pass

    def __enter__(self):
        self._threads = []
        self._upload_queue = queue.Queue()
        self._workers_active = True
        self._files_to_upload = None
        self._total_files_size = 0
        self._uploaded_files_size = 0
        self._upload_progress = 0
        self._create_workers()
        self._service = build('drive', 'v3', credentials=self._get_creds())

        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._service = None
        self._workers_active = False
        for i in range(configurations.WORKERS):
            self._upload_queue.put(None)

        for thread in self._threads:
            thread.join()

        with self._upload_queue.mutex:
            self._total_files_size = 0
            self._uploaded_files_size = 0
            self._upload_progress = 0

        self._files_to_upload = None
        self._upload_queue = None
        self._threads = []

        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return True

    def _create_workers(self):
        for i in range(configurations.WORKERS):
            thread = SyncWorker(name=i, context=self)
            self._threads.append(thread)
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

    def _is_valid_file(self, file_path):
        try:
            return not bool(
                os.stat(file_path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN) and not os.path.basename(
                os.path.abspath(file_path)).startswith('.') and not os.path.islink(
                file_path) and os.access(file_path, os.R_OK)
        except:
            return False

    def _scan_folder(self, folder_path, excluded_paths, parent_node):
        node_resolver = Resolver()
        total_size = 0

        current_path = ''
        for folder_name in folder_path.strip(os.sep).split(os.sep):
            current_path += folder_name
            try:
                parent_node = node_resolver.get(parent_node.root, os.sep + current_path)
            except:
                parent_node = Node(folder_name, parent=parent_node, size=0, drive_id=None, is_file=False)

        for root, folders, files in os.walk(folder_path, topdown=True):
            root = root.rstrip(os.sep)
            folders[:] = [d for d in folders if
                          ((root + os.sep + d).upper() not in excluded_paths and self._is_valid_file(
                              root + os.sep + d))]
            random.shuffle(folders)

            try:
                parent_node = node_resolver.get(parent_node.root, os.sep + root.lstrip(os.sep))
            except ResolverError:
                parent_node = node_resolver.get(parent_node.root,
                                                os.sep + os.path.abspath(root + os.sep + '..').lstrip(os.sep))
                parent_node = Node(os.path.basename(root), parent=parent_node, size=0, drive_id=None, is_file=False)

            for name in files:
                file_path = root + os.sep + name
                # skip if it is symbolic link
                if self._is_valid_file(file_path):
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

        if path_node.parent.drive_id is not None and self._is_valid_file(file_path):
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
            raise Exception('Must be called inside with statement')

    def _get_or_create_folder(self, folder_name, parent_folder_id):
        results = self._service.files().list(
            pageSize=10, fields="nextPageToken, files",
            q=f'name = "{folder_name}" and trashed = false and parents in "{parent_folder_id}"').execute()
        items = results.get('files', [])
        folder_id = None
        if not items or len(items) == 0:
            folder_metadata = {
                'name': folder_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder_id = self._service.files().create(body=folder_metadata,
                                                     fields='id').execute().get('id')
        else:
            folder_id = items[0]['id']

        return folder_id

    def backup_folders(self, local_folders, infinite_sync):
        '''Uploads folder and all it's content (if it doesnt exists)
        in root folder.
        Args:
            :param local_folders: Folders to backup to Drive.
            :param infinite_sync: Whether to stop once all files were back-ed up or keep listening for file changes and
                                  keep backup forever
        Returns:
            Dictionary, where keys are folder's names
            and values are id's of these folders.

        '''
        self._validate_usage()
        node_resolver = Resolver()
        parents_id = {}
        random.shuffle(local_folders)
        local_folders = [os.path.abspath(local_folder) for local_folder in local_folders if
                         os.path.isdir(local_folder) and local_folder.count(os.sep) > 0]
        # TODO make sure there isn't overlap, remove last slash

        excluded_paths = [os.path.abspath(excluded_path).upper() for excluded_path in configurations.EXCLUDED_PATHS]

        destination_folder_id = self._get_or_create_folder(configurations.BACKUP_DESTINATION, 'root')
        Logger.info(f'backup folder id on Drive: {configurations.BACKUP_DESTINATION}')

        if infinite_sync:
            event_handler = FileChangeEventHandler()
            self._file_changed_observer = Observer()
            for local_folder in local_folders:
                self._file_changed_observer.schedule(event_handler, local_folder, recursive=True)
            self._file_changed_observer.start()

        Logger.info(f'Scan files in folders {local_folders}...')
        for local_folder in local_folders:
            self._files_to_upload, total_files_size = self._scan_folder(local_folder, excluded_paths, self._files_to_upload)
            self._total_files_size += total_files_size

        self._uploaded_files_size = 0
        self._upload_progress = 0
        Logger.info(f'Start backup folder {local_folders} ({self._convert_size(self._total_files_size)})...')
        Logger.print_progress(0)

        if self._files_to_upload is None:
            Logger.info('Nothing to sync')
        else:
            for path_node in PreOrderIter(self._files_to_upload):
                if path_node.is_file:
                    self._upload_queue.put([path_node])
                else:
                    try:
                        if path_node.drive_id is None:
                            path_node.drive_id = self._get_or_create_folder(path_node.name,
                                                                            destination_folder_id if path_node.is_root else path_node.parent.drive_id)
                        next_page_token = None
                        while True:
                            results = self._service.files().list(
                                pageSize=configurations.DRIVE_SCAN_PAGE_SIZE, fields="nextPageToken, files",
                                pageToken=next_page_token,
                                q=f'trashed = false and parents in "{path_node.drive_id}"').execute()
                            items = results.get('files', [])

                            for item in items:
                                try:
                                    node_resolver.get(path_node, item['name'])
                                except ResolverError:
                                    self._service.files().delete(fileId=item['id']).execute()
                                    Logger.debug(f'folder deleted {self._get_node_path(path_node) + os.sep + item["name"]}')

                            next_page_token = results.get('nextPageToken', None)
                            if next_page_token is None:
                                break
                    except Exception as e:
                        Logger.error(f'Failed to sync folder {path_node.name} with exception: {e}')

            # Wait all workers to empty the queue
            self._upload_queue.join()

        if infinite_sync:
            time.sleep(60)
            self._file_changed_observer.stop()
            self._file_changed_observer.join()
            self._file_changed_observer = None

        Logger.info('End Sync')

        return parents_id

if __name__ == '__main__':
    with BackupEngine() as backup_engine:
        backup_engine.backup_folders(configurations.FOLDERS_TO_BACKUP, infinite_sync=True)
