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
import traceback
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import configurations

socket.setdefaulttimeout(600)  # set timeout to 10 minutes

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']


class BackupEngine:
    def __init__(self):
        self._files = None
        self._print_lock = threading.Lock()
        pass

    def __enter__(self):
        self._threads = []
        self._files = queue.Queue()
        self._workers_active = True
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
            self._files.put(None)

        for thread in self._threads:
            thread.join()

        with self._files.mutex:
            self._total_files_size = 1
            self._uploaded_files_size = 0
            self._upload_progress = 0

        self._files = None
        self._threads = []

        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return True

    @staticmethod
    def _upload_worker(_self):
        while _self._workers_active:
            item = _self._files.get()
            if item is None:
                break
            try:
                _self._upload_file(*item)
            except Exception as e:
                _self._error(f'Failed to backup file {item} with exception: {e}')

            _self._files.task_done()

    def _create_workers(self):
        for i in range(configurations.WORKERS):
            thread = threading.Thread(name=i, target=BackupEngine._upload_worker, args=[self])
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
            return not bool(os.stat(file_path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN) and not os.path.basename(os.path.abspath(file_path)).startswith('.') and not os.path.islink(
                file_path) and os.access(file_path, os.R_OK)
        except:
            return False

    def _get_folder_size(self, folder_path, excluded_paths):
        total_size = 0
        for root, dirs, files in os.walk(folder_path, topdown=True):
            root = root.rstrip(os.sep)
            dirs[:] = [d for d in dirs if
                       ((root + os.sep + d).upper() not in excluded_paths and self._is_valid_file(root + os.sep + d))]

            for name in files:
                file_path = root + os.sep + name
                # skip if it is symbolic link
                if self._is_valid_file(file_path):
                    total_size += os.path.getsize(file_path)

        return total_size

    def _upload_file(self, root, name, folder_id, file_on_drive):
        file_path = root + os.sep + name
        if not self._is_valid_file(file_path):
            return

        file_size = os.path.getsize(root + os.sep + name)
        for i in range(configurations.UPLOAD_RETRIES):
            try:
                service = build('drive', 'v3', credentials=self._get_creds())

                media = MediaFileUpload(
                    file_path,
                    mimetype=mimetypes.MimeTypes().guess_type(name)[0],
                    resumable=file_size > 1 * 1024 * 1024)

                if file_on_drive is None:
                    file_metadata = {'name': name, 'parents': [folder_id]}
                    service.files().create(body=file_metadata,
                                                     media_body=media,
                                                     fields='id').execute()
                    self._debug(f'file created {file_path}')
                elif file_on_drive['md5Checksum'] != self._md5(file_path):
                    updated_file = service.files().update(
                        fileId=file_on_drive['id'],
                        body={},
                        media_body=media).execute()
                    self._debug(f'file updated {file_path}')
                else:
                    self._debug(f'file already exist {file_path}')

                break
            except Exception as e:
                if i == configurations.UPLOAD_RETRIES-1:
                    self._error(f'Failed to backup file {file_path} with exception: {e}')
                else:
                    self._debug(f'file upload failed ({i}) {file_path}')


        with self._files.mutex:
            self._uploaded_files_size += file_size
            if (self._uploaded_files_size / self._total_files_size - self._upload_progress) * 100 >= configurations.PROGRESS_INTERNALS:
                self._upload_progress = self._uploaded_files_size / self._total_files_size
                self._print_progress(self._upload_progress)

    def _print_progress(self, progress):
        self._log("[%-20s] %d%%" % ('=' * round(progress * 20), round(100 * progress)))

    def _validate_usage(self):
        if self._files is None:
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
        parents_id = {}
        local_folder = os.path.abspath(local_folder)
        excluded_paths = [os.path.abspath(excluded_path).upper() for excluded_path in configurations.EXCLUDED_PATHS]

        destination_folder_id = self._get_or_create_folder(configurations.BACKUP_DESTINATION, 'root')
        self._log(f'backup folder id on Drive: {configurations.BACKUP_DESTINATION}')

        self._log(f'Scan files in folder {local_folder}...')
        self._total_files_size = self._get_folder_size(local_folder, excluded_paths) + 1
        self._uploaded_files_size = 0
        self._upload_progress = 0
        self._log(f'Start backup folder {local_folder} ({self._convert_size(self._total_files_size)})...')
        self._print_progress(0)

        for root, folders, files in os.walk(local_folder, topdown=True):
            root = root.rstrip(os.sep)
            split_path = root.split(os.sep)

            last_dir_name = None
            pre_last_dir_name = None

            folders[:] = [d for d in folders if
                       ((root + os.sep + d).upper() not in excluded_paths and self._is_valid_file(root + os.sep + d))]

            random.shuffle(folders)

            if len(split_path) >= 2:
                last_dir_name = split_path[-1]
                pre_last_dir_name = os.sep.join(split_path[:-1])
            elif len(split_path) >= 1 and local_folder[-1] == os.sep:
                self._log(f'Path to complete drive was supplied. root = {root}, local_folder = {local_folder}')
                last_dir_name = split_path[-1]
            else:
                self._error(f'Invalid path supplied. root = {root}, local_folder = {local_folder}')
                raise Exception()

            if pre_last_dir_name is None or pre_last_dir_name not in parents_id.keys():
                pre_last_dir_id = destination_folder_id
            else:
                pre_last_dir_id = parents_id[pre_last_dir_name]

            try:
                folder_id = self._get_or_create_folder(last_dir_name, pre_last_dir_id)

                # Get all files on drive at this path
                folders_in_drive_folder = {}
                files_in_drive_folder = {}
                results = self._service.files().list(
                    pageSize=100, fields="nextPageToken, files",
                    q=f'trashed = false and parents in "{folder_id}"').execute()
                items = results.get('files', [])
                while items and len(items) > 0:
                    for item in items:
                        if item['mimeType'] == 'application/vnd.google-apps.folder':
                            folders_in_drive_folder[item['name']] = {'id': item['id']}
                        else:
                            files_in_drive_folder[item['name']] = {'id': item['id'], 'md5Checksum': item['md5Checksum']}
                    next_page_token = results.get('nextPageToken', None)
                    if next_page_token is None:
                        break
                    results = self._service.files().list(
                        pageSize=100, fields="nextPageToken, files",
                        pageToken=next_page_token, q=f'trashed = false and parents in "{folder_id}"').execute()
                    items = results.get('files', [])

                # For each local file, upload or update it on drive
                for name in files:
                    self._files.put([root, name, folder_id, files_in_drive_folder.pop(name, None)])

                # For each remote file which isn't exist locally, remove it from drive
                for name, file in files_in_drive_folder.items():
                    self._service.files().delete(fileId=file['id']).execute()
                    self._debug(f'file deleted {root + os.sep + name}')

                # For each local folder, allow upload
                for folder in folders:
                    folders_in_drive_folder.pop(folder, None)

                # For each remote folder which isn't exist locally, remove it from drive
                for name, folder in folders_in_drive_folder.items():
                    self._service.files().delete(fileId=folder['id']).execute()
                    self._debug(f'folder deleted {root + os.sep + name}')

                last_dir_name = root
                parents_id[last_dir_name] = folder_id
            except Exception as e:
                self._error(f'Failed to backup folder {root} with exception: {e}')

        # Wait all workers to empty the queue
        self._files.join()

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
    with BackupEngine() as backup_engine:
        random.shuffle(configurations.FOLDERS_TO_BACKUP)
        for path in configurations.FOLDERS_TO_BACKUP:
            backup_engine.backup_folder(path)

