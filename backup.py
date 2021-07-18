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
from watchdog.events import LoggingEventHandler
from anytree import Node, Resolver, ResolverError, PreOrderIter

import configurations

socket.setdefaulttimeout(600)  # set timeout to 10 minutes

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']
Node.separator = os.sep


class BackupEngine:
    def __init__(self):
        self._upload_queue = None
        self._files_to_upload = None
        self._folder_ids = {}
        self._total_files_size = 1
        self._uploaded_files_size = 0
        self._upload_progress = 0
        self._print_lock = threading.Lock()
        pass

    def __enter__(self):
        self._threads = []
        self._folder_ids = {}
        self._upload_queue = queue.Queue()
        self._workers_active = True
        self._files_to_upload = None
        self._total_files_size = 1
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
            self._total_files_size = 1
            self._uploaded_files_size = 0
            self._upload_progress = 0

        self._upload_queue = None
        self._threads = []

        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return True

    @staticmethod
    def _sync_worker(_self):
        while _self._workers_active:
            item = _self._upload_queue.get()
            if item is None:
                break
            try:
                _self._upload_file(*item)
            except Exception as e:
                _self._error(f'Failed to backup file {item} with exception: {e}')

            _self._upload_queue.task_done()

    def _create_workers(self):
        for i in range(configurations.WORKERS):
            thread = threading.Thread(name=i, target=BackupEngine._sync_worker, args=[self])
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

    def _scan_folder(self, folder_path, excluded_paths):
        total_size = 0
        parent_node = None
        for folder_name in folder_path.strip(os.sep).split(os.sep):
            parent_node = Node(folder_name, parent=parent_node, size=0, drive_id=None, is_file=False)
        node_resolver = Resolver()
        for root, folders, files in os.walk(folder_path, topdown=True):
            root = root.rstrip(os.sep)
            folders[:] = [d for d in folders if
                          ((root + os.sep + d).upper() not in excluded_paths and self._is_valid_file(
                              root + os.sep + d))]
            random.shuffle(folders)

            try:
                parent_node = node_resolver.get(parent_node.root, os.sep + root.lstrip(os.sep))
            except ResolverError:
                parent_node = node_resolver.get(parent_node.root, os.sep + os.path.abspath(root + os.sep + '..').lstrip(os.sep))
                parent_node = Node(os.path.basename(root), parent=parent_node, size=0, drive_id=None, is_file=False)

            for name in files:
                file_path = root + os.sep + name
                # skip if it is symbolic link
                if self._is_valid_file(file_path):
                    file_size = os.path.getsize(file_path)
                    parent_node.size += file_size
                    Node(name, parent=parent_node, size=file_size, drive_id=None, is_file=True)
            total_size += parent_node.size
        return parent_node.root, total_size or 1

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
                    self._debug(f'file {file_path} found on drive as folder. folder removed, will upload the file now.')
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
                        self._debug(f'file uploaded {file_path}')
                    elif file_checksum != self._md5(file_path):
                        service.files().update(
                            fileId=path_node.drive_id,
                            body={},
                            media_body=media).execute()
                        self._debug(f'file updated {file_path}')
                    else:
                        self._debug(f'file skipped. file already exist {file_path}')

                    break
                except Exception as e:
                    if i == configurations.UPLOAD_RETRIES - 1:
                        self._error(f'Failed to backup file {file_path} with exception: {e}')
                    else:
                        self._debug(f'file upload failed (try #{i}) {file_path}')
        elif path_node.drive_id is not None:
            service.files().delete(fileId=path_node.drive_id).execute()
            self._debug(f'file deleted {file_path}')
        else:
            self._debug(f'file skipped (failed to sync parent or invalid file) {file_path}')

        with self._upload_queue.mutex:
            self._uploaded_files_size += path_node.size
            if (
                    self._uploaded_files_size / self._total_files_size - self._upload_progress) * 100 >= configurations.PROGRESS_INTERNALS:
                self._upload_progress = self._uploaded_files_size / self._total_files_size
                self._print_progress(self._upload_progress)

    def _print_progress(self, progress):
        self._log("[%-20s] %d%%" % ('=' * round(progress * 20), round(100 * progress)))

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

    def backup_folder(self, local_folder):
        '''Uploads folder and all it's content (if it doesnt exists)
        in root folder.

        Args:
            local_folder: Folder to backup to Drive.
            destination_folder_id: id of parent dir
        Returns:
            Dictionary, where keys are folder's names
            and values are id's of these folders.
        '''
        self._validate_usage()
        node_resolver = Resolver()
        parents_id = {}
        local_folder = os.path.abspath(local_folder)
        if not os.path.isdir(local_folder):
            self._error(f'Invalid path supplied. local_folder = {local_folder}. Path must be existing folder.')
            return

        excluded_paths = [os.path.abspath(excluded_path).upper() for excluded_path in configurations.EXCLUDED_PATHS]

        destination_folder_id = self._get_or_create_folder(configurations.BACKUP_DESTINATION, 'root')
        self._log(f'backup folder id on Drive: {configurations.BACKUP_DESTINATION}')

        if local_folder.count(os.sep) > 1:
            local_folder = local_folder.rstrip(os.sep)

        if local_folder.endswith(os.sep):
            self._log(f'Path to full drive was supplied. local_folder = {local_folder}')
        elif local_folder.count(os.sep) == 0:
            self._error(f'Invalid path supplied. local_folder = {local_folder}. Will skip this one.')
            return

        self._log(f'Scan files in folder {local_folder}...')
        self._files_to_upload, self._total_files_size = self._scan_folder(local_folder, excluded_paths)
        self._uploaded_files_size = 0
        self._upload_progress = 0
        self._log(f'Start backup folder {local_folder} ({self._convert_size(self._total_files_size)})...')
        self._print_progress(0)

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
                            pageToken=next_page_token, q=f'mimeType = "application/vnd.google-apps.folder" and trashed = false and parents in "{path_node.drive_id}"').execute()
                        items = results.get('files', [])

                        for item in items:
                            try:
                                node_resolver.get(path_node, item['name'])
                            except ResolverError:
                                self._service.files().delete(fileId=item['id']).execute()
                                self._debug(f'folder deleted {self._get_node_path(path_node) + os.sep + item["name"]}')

                        next_page_token = results.get('nextPageToken', None)
                        if next_page_token is None:
                            break
                except Exception as e:
                    self._error(f'Failed to sync folder {path_node.name} with exception: {e}')

        # Wait all workers to empty the queue
        self._upload_queue.join()

        self._log('End Sync')

        return parents_id

    def _debug(self, str):
        if configurations.LOG_LEVEL.upper() != 'DEBUG':
            return
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        with self._print_lock:
            print(f'[{dt_string}] [DEBUG] [{threading.current_thread().name}] - {str}')

    def _log(self, str):
        if configurations.LOG_LEVEL.upper() != 'DEBUG' and configurations.LOG_LEVEL.upper() != 'INFO':
            return
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        with self._print_lock:
            print(f'[{dt_string}] [INFO] [{threading.current_thread().name}] - {str}')

    def _error(self, str):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        with self._print_lock:
            print(f'[{dt_string}] [ERROR] [{threading.current_thread().name}] - {str}')


if __name__ == '__main__':

    # logging.basicConfig(level=logging.INFO,
    #                     format='%(asctime)s - %(message)s',
    #                     datefmt='%Y-%m-%d %H:%M:%S')
    # event_handler = LoggingEventHandler()
    # observer = Observer()
    # observer.schedule(event_handler, configurations.FOLDERS_TO_BACKUP[0], recursive=True)
    # observer.start()
    # time.sleep(10)
    # observer.stop()
    # observer.join()
    # exit()

    with BackupEngine() as backup_engine:
        random.shuffle(configurations.FOLDERS_TO_BACKUP)
        for path in configurations.FOLDERS_TO_BACKUP:
            backup_engine.backup_folder(path)
