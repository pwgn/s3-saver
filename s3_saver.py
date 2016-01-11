__version__ = '0.2.2'

from glob import glob
import os
import re

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key


class S3Saver(object):
    """
    Saves files to Amazon S3 (or default local storage).
    """

    def __init__(self, storage_type=None, bucket_name=None,
                 access_key_id=None, access_key_secret=None,
                 acl='public-read', field_name=None,
                 storage_type_field=None, bucket_name_field=None,
                 filesize_field=None, base_path=None,
                 permission=0o666, static_root_parent=None):
        if storage_type and (storage_type != 's3'):
            raise ValueError('Storage type "%s" is invalid, the only supported storage type (apart from default local storage) is s3.' % storage_type)

        self.storage_type = storage_type
        self.bucket_name = bucket_name
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.acl = acl
        self.field_name = field_name
        self.storage_type_field = storage_type_field
        self.bucket_name_field = bucket_name_field
        self.filesize_field = filesize_field
        self.base_path = base_path
        self.permission = permission
        self.static_root_parent = static_root_parent

    def _get_path(self, filename):
        if not self.base_path:
            raise ValueError('S3Saver requires base_path to be set.')

        if callable(self.base_path):
            return os.path.join(self.base_path(), filename)
        return os.path.join(self.base_path, filename)

    def _get_s3_path(self, filename):
        if not self.static_root_parent:
            raise ValueError('S3Saver requires static_root_parent to be set.')

        return re.sub('^\/', '', self._get_path(filename).replace(self.static_root_parent, ''))

    def _delete_local(self, filename):
        """Deletes the specified file from the local filesystem."""

        if os.path.exists(filename):
            os.remove(filename)

    def _delete_s3(self, filename, bucket_name):
        """Deletes the specified file from the given S3 bucket."""

        conn = S3Connection(self.access_key_id, self.access_key_secret)
        bucket = conn.get_bucket(bucket_name)

        if type(filename).__name__ == 'Key':
            filename = '/' + filename.name

        path = self._get_s3_path(filename)
        k = Key(bucket)
        k.key = path

        try:
            bucket.delete_key(k)
        except S3ResponseError:
            pass

    def delete(self, filename):
        """Deletes the specified file, either locally or from S3, depending on the file's storage type."""

        if not (self.storage_type and self.bucket_name):
            self._delete_local(filename)
        else:
            if self.storage_type != 's3':
                raise ValueError('Storage type "%s" is invalid, the only supported storage type (apart from default local storage) is s3.' % storage_type)

            self._delete_s3(filename, self.bucket_name)

    def _save_local(self, temp_file, filename, obj):
        """Saves the specified file to the local file system."""

        path = self._get_path(filename)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), self.permission | 0o111)

        fd = open(path, 'wb')

        # Thanks to:
        # http://stackoverflow.com/a/3253276/2066849
        temp_file.seek(0)
        t = temp_file.read(1048576)
        while t:
            fd.write(t)
            t = temp_file.read(1048576)

        fd.close()

        if self.filesize_field:
            setattr(obj, self.filesize_field, os.path.getsize(path))

        return filename

    def _save_s3(self, temp_file, filename, obj):
        """Saves the specified file to the configured S3 bucket."""

        conn = S3Connection(self.access_key_id, self.access_key_secret)
        bucket = conn.get_bucket(self.bucket_name)

        path = self._get_s3_path(filename)
        k = bucket.new_key(path)
        k.set_contents_from_string(temp_file.read())
        k.set_acl(self.acl)

        if self.filesize_field:
            setattr(obj, self.filesize_field, k.size)

        return filename

    def save(self, temp_file, filename, obj):
        """Saves the specified file to either S3 or the local filesystem, depending on the currently enabled storage type."""

        if not (self.storage_type and self.bucket_name):
            ret = self._save_local(temp_file, filename, obj)
        else:
            if self.storage_type != 's3':
                raise ValueError('Storage type "%s" is invalid, the only supported storage type (apart from default local storage) is s3.' % self.storage_type)

            ret = self._save_s3(temp_file, filename, obj)

        if self.field_name:
            setattr(obj, self.field_name, ret)

        if self.storage_type == 's3':
            if self.storage_type_field:
                setattr(obj, self.storage_type_field, self.storage_type)
            if self.bucket_name_field:
                setattr(obj, self.bucket_name_field, self.bucket_name)
        else:
            if self.storage_type_field:
                setattr(obj, self.storage_type_field, '')
            if self.bucket_name_field:
                setattr(obj, self.bucket_name_field, '')

        return ret

    def _find_by_path_local(self, path):
        """Finds files by globbing on the local filesystem."""

        return glob('%s*' % path)

    def _find_by_path_s3(self, path, bucket_name):
        """Finds files by licking an S3 bucket's contents by prefix."""

        conn = S3Connection(self.access_key_id, self.access_key_secret)
        bucket = conn.get_bucket(bucket_name)

        s3_path = self._get_s3_path(path)

        return bucket.list(prefix=s3_path)

    def find_by_path(self, path):
        """Finds files at the specified path / prefix, either on S3 or on the local filesystem."""

        if not (self.storage_type and self.bucket_name):
            return self._find_by_path_local(path)
        else:
            if self.storage_type != 's3':
                raise ValueError('Storage type "%s" is invalid, the only supported storage type (apart from default local storage) is s3.' % self.storage_type)

            return self._find_by_path_s3(path, self.bucket_name)

    def find_by_filename(self, filename):
        path = self._get_path(filename)
        if not (self.storage_type and self.bucket_name):
            return self._find_by_path_local(path)
        else:
            if self.storage_type != 's3':
                raise ValueError('Storage type "%s" is invalid, the only supported storage type (apart from default local storage) is s3.' % self.storage_type)

            return self._find_by_path_s3(path, self.bucket_name)

    def download(self, f):
        """Downloads a file returned by find_by_path to the local file system."""

        if not (self.storage_type and self.bucket_name):
            ret = f
        else:
            if self.storage_type != 's3':
                raise ValueError('Storage type "%s" is invalid, the only supported storage type (apart from default local storage) is s3.' % self.storage_type)

            file_path = self._download_s3(f)

            ret = file_path

        return ret

    def _download_s3(self, f):
        """Download file from s3 to local fs"""

        file_path = os.path.join(self.static_root_parent, f.name)

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        with open(file_path, 'w+') as dl_file:
            f.get_contents_to_file(dl_file)

        return file_path
