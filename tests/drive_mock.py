# from googleapiclient.http import MediaFileUpload
# from google.oauth2.credentials import Credentials
#
# from googleapiclient.discovery import build
# from google.auth.transport.requests import Request
# from google_auth_oauthlib.flow import InstalledAppFlow
import os
import hashlib
import re
import json
from anytree import Node, PreOrderIter, RenderTree, cachedsearch, Resolver
import uuid, random
from googleapiclient.http import MediaFileUpload
import copy


class DriveMock:
    TEST_DATA_FOLDER_DRIVE_NAME = 'TEST'

    def __init__(self, monkeypatch, folder_path_to_simulate=None):
        self._rd = random.Random()
        self.files_tree = self.get_simulated_files_tree(folder_path_to_simulate)
        self._overridden_attributes = {
            'Credentials': DriveMock.CredentialsMock(),
            'build': self._build
        }

        self._init_mock(monkeypatch)

    def compare_drive_trees(self, drive_tree_1, drive_tree_2, compare_drive_id=False, compare_file_content=True):

        if not drive_tree_1 and not drive_tree_2:
            return True

        if (drive_tree_1 and not drive_tree_2) or (not drive_tree_1 and drive_tree_2):
            return False

        if drive_tree_1.name != drive_tree_2.name or drive_tree_1.is_file != drive_tree_2.is_file \
                or (compare_drive_id and drive_tree_1.drive_id != drive_tree_2.drive_id):
            return False
        if compare_file_content and drive_tree_1.is_file:
            if drive_tree_1.size != drive_tree_2.size or drive_tree_1.content != drive_tree_2.content:
                return False

        if len(drive_tree_1.children) != len(drive_tree_2.children):
            return False

        if len(drive_tree_1.children) == 0:
            return True
        else:
            for child_1 in drive_tree_1.children:
                match_found = any([self.compare_drive_trees(child_1, child_2, compare_drive_id, compare_file_content) for child_2 in drive_tree_2.children])
                return match_found

    def _uuid(self):
        return str(uuid.UUID(int=self._rd.getrandbits(128)))

    def get_simulated_files_tree(self, folder_paths_to_simulate=None):
        self._rd.seed(0)
        parent_node = Node('root', size=0, drive_id='root', is_file=False)
        parent_node = Node(DriveMock.TEST_DATA_FOLDER_DRIVE_NAME, parent=parent_node, size=0, drive_id=self._uuid(),
                           is_file=False)

        if not folder_paths_to_simulate or len(folder_paths_to_simulate) == 0:
            return parent_node.root
        if len(folder_paths_to_simulate) > 1 and os.path.commonpath(folder_paths_to_simulate) in folder_paths_to_simulate:
            raise Exception('Overlapped paths are not supported!')

        for folder_path_to_simulate in folder_paths_to_simulate:
            node_resolver = Resolver()
            folder_path_to_simulate = os.path.abspath(folder_path_to_simulate)
            current_path = os.sep + 'root' + os.sep + DriveMock.TEST_DATA_FOLDER_DRIVE_NAME
            parent_node = node_resolver.get(parent_node.root, current_path)
            for folder_name_in_path in folder_path_to_simulate.strip(os.sep).split(os.sep):
                current_path += os.sep + folder_name_in_path
                try:
                    parent_node = node_resolver.get(parent_node.root, current_path)
                except:
                    parent_node = Node(folder_name_in_path, parent=parent_node, size=0, drive_id=self._uuid(), is_file=False)

            if os.path.isfile(folder_path_to_simulate):
                parent_node.is_file = True
                with open(os.path.abspath(folder_path_to_simulate), "r") as f:
                    parent_node.content = f.read()
                    continue

            ids = {folder_path_to_simulate: parent_node.drive_id}
            for root, folders, files in os.walk(folder_path_to_simulate, topdown=True):
                folder_id = ids.get(root)

                parent_node_tmp = cachedsearch.find_by_attr(parent_node.root, name='drive_id', value=folder_id)
                if parent_node_tmp:
                    parent_node = parent_node_tmp
                else:
                    try:
                        parent_node = node_resolver.get(parent_node.root, os.sep + 'root' + os.sep + DriveMock.TEST_DATA_FOLDER_DRIVE_NAME + os.sep + root)
                    except:
                        parent_folder_id = ids.get(os.path.abspath(root + os.sep + '..'), self._uuid())
                        parent_node = cachedsearch.find_by_attr(parent_node.root, name='drive_id',
                                                                value=parent_folder_id)
                        parent_node = Node(os.path.basename(root), parent=parent_node, size=0,
                                       drive_id=self._uuid(),
                                       is_file=False)
                ids[root] = parent_node.drive_id
                for name in files:
                    with open(root + os.sep + name, "r") as f:
                        Node(name, parent=parent_node, size=0, drive_id=self._uuid(), is_file=True, content=f.read())

        return parent_node.root

    def _init_mock(self, monkeypatch):
        for mock_name, mock in self._overridden_attributes.items():
            monkeypatch.setattr(f'backup.{mock_name}', mock)

    def _build(self, serviceName, version, credentials):
        return DriveMock.ServiceMock(self.files_tree, self._uuid)

    class CredentialsMock:
        def __init__(self):
            self.from_authorized_user_file = self._from_authorized_user_file

        def _from_authorized_user_file(self, token_file, scopes):
            return DriveMock.CredentialsMock.AuthorizationMock()

        class AuthorizationMock:
            def __init__(self):
                self.valid = True
                self.expired = False
                self.refresh_token = False
                self.refresh = self._refresh
                self.to_json = self._to_json

            def _refresh(self, request):
                return None

            def _to_json(self):
                return None

    class ServiceMock:

        def __init__(self, files_tree, uuid_method):
            self._files_tree = files_tree
            self._uuid = uuid_method

            self.files = self._files

        def _files(self):
            return DriveMock.ServiceMock.FilesMock(self._files_tree, self._uuid)

        class FilesMock:

            def __init__(self, files_tree, uuid_method):
                self._files_tree = files_tree
                self._uuid = uuid_method

                self.list = self._list
                self.delete = self._delete
                self.create = self._create
                self.update = self._update

            def _md5(self, content):
                hash_md5 = hashlib.md5()
                hash_md5.update(content.encode('utf-8'))
                return hash_md5.hexdigest()

            def _list(self, pageSize=None, fields=None, pageToken=None, q=None):
                fields = [a.strip() for a in fields.split(',')]
                parent_id = re.match(r'.*parents in \"([^\"]*)\"', q).group(1)
                name = None
                if q.find('name = "') >= 0:
                    name = re.match(r'.*name \= \"([^\"]*)\"', q).group(1)

                res_dict = {}
                if 'files' in fields:
                    res_dict['files'] = []
                if 'nextPageToken' in fields:
                    res_dict['nextPageToken'] = None

                parent_node = cachedsearch.find_by_attr(self._files_tree, name='drive_id', value=parent_id)
                if not parent_node:
                    return DriveMock.ServiceMock.FilesMock.RequestMock(res_dict)

                # TODO: lock the list every time it is used
                # TODO: add file sizing
                res = [node for node in parent_node.children if not name or node.name == name]
                pageToken = pageToken or 0
                nextPageToken = pageToken + pageSize
                if 'nextPageToken' in fields:
                    res_dict['nextPageToken'] = nextPageToken if nextPageToken < len(res) else None

                res = res[pageToken:nextPageToken]
                if 'files' in fields:
                    for node in res:
                        if node.is_file:
                            with open(f'tests{os.sep}mock_data_file.json', ) as t:
                                mock_data_file_template = json.load(t)
                                mock_data_file_template['id'] = node.drive_id
                                mock_data_file_template['name'] = node.name
                                mock_data_file_template['parents'] = [os.sep.join(
                                    [''] + [node.name for node in node.parent.path]) if node.parent else 'root']
                                mock_data_file_template[
                                    'webContentLink'] = f'https://drive.google.com/uc?id={node.drive_id}&export=download'
                                mock_data_file_template[
                                    'webViewLink'] = f'https://drive.google.com/file/d/{node.drive_id}/view?usp=drivesdk'
                                mock_data_file_template['originalFilename'] = node.name
                                mock_data_file_template['md5Checksum'] = self._md5(node.content)
                                res_dict['files'].append(mock_data_file_template)
                        else:
                            with open(f'tests{os.sep}mock_data_folder.json', ) as t:
                                mock_data_folder_template = json.load(t)
                                mock_data_folder_template['id'] = node.drive_id
                                mock_data_folder_template['name'] = node.name
                                mock_data_folder_template['parents'] = [os.sep.join(
                                    [''] + [node.name for node in node.parent.path]) if node.parent else 'root']
                                mock_data_folder_template[
                                    'webViewLink'] = f'https://drive.google.com/file/d/{node.drive_id}/view?usp=drivesdk'
                                res_dict['files'].append(mock_data_folder_template)

                return DriveMock.ServiceMock.FilesMock.RequestMock(res_dict)

            def _delete(self, fileId):
                try:
                    node = cachedsearch.find_by_attr(self._files_tree, name='drive_id', value=fileId)
                    node.parent.children = [c for c in node.parent.children if c.drive_id != node.drive_id]
                except:
                    pass
                return DriveMock.ServiceMock.FilesMock.RequestMock()

            def _create(self, body, media_body=None, fields=None):
                name = body['name']
                parent_id = body['parents'][0]
                parent_node = cachedsearch.find_by_attr(self._files_tree, name='drive_id', value=parent_id)
                new_node = None
                if media_body:  # It's a file
                    file_path = os.path.abspath(media_body._filename)
                    with open(file_path, "r") as f:
                        new_node = Node(name, parent=parent_node, size=0, drive_id=self._uuid(),
                                        is_file=True, content=f.read())
                else:  # It's a folder
                    new_node = Node(name, parent=parent_node, size=0, drive_id=self._uuid(),
                                    is_file=False)

                return DriveMock.ServiceMock.FilesMock.RequestMock({'id': new_node.drive_id})
                # os.sep.join([''] + [node.name for node in parent_node.path])

            def _update(self, fileId, body, media_body):
                node = cachedsearch.find_by_attr(self._files_tree, name='drive_id', value=fileId)
                with open(os.path.abspath(media_body._filename), "r") as f:
                    node.content = f.read()
                return DriveMock.ServiceMock.FilesMock.RequestMock()

            class RequestMock:

                def __init__(self, res_dict=None):
                    self._res_dict = res_dict
                    self.execute = self._execute

                def _execute(self):
                    return copy.deepcopy(self._res_dict)
