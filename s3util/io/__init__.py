#!/usr/bin/env python

import time
import sys

import boto

from s3util.utils.ui import ProgressBar
from s3util.hooks import load_hook, apply_hooks

class AuthenticationError(Exception):
    pass

class Connection:
    OVERWRITE_SKIP, OVERWRITE_REPLACE, OVERWRITE_SUFFIX, \
            OVERWRITE_VERSION = range(4)
     
    def __init__(self, access_key_id=None, secret_access_key=None):
        self._conn = boto.connect_s3(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key)

    def get(self, bucket, key, abspath, overwrite=OVERWRITE_REPLACE,
            post_hooks=None, stream=sys.stdout):
        if type(abspath) not in (file, str):
            raise TypeError("invalid 'abspath' argument")

        if overwrite >= Connection.OVERWRITE_VERSION:
            raise ValueError(("invalid 'overwrite' argument: "
                    "local file does not support versioning."))

        fname = abspath if type(abspath) is str else abspath.name
        func = 'get_contents_to_filename' if type(abspath) is str else \
                'get_contents_to_file'

        try:
            _bucket = bucket if isinstance(bucket, boto.s3.bucket.Bucket) else \
                    self._conn.get_bucket(bucket)
        except:
            pass
            
        _key = key if isinstance(key, boto.s3.key.Key) else \
                boto.s3.key.Key(_bucket, key)

        if not _key.exists():
            raise IOError("S3 key {0}/{1} does not exist.".format(
                _bucket.name, _key.name))

        dirname = os.path.dirname(fname)
        if not os.path.exists(dirname):
            os.mkdir(dirname)

        if os.path.exists(fname) and \
                overwrite == self.OVERWRITE_SKIP:
            return

        if not os.path.exists(fname) or \
                overwrite == self.OVERWRITE_REPLACE:
            getattr(_key, func)(abspath)

        return apply_hooks(_bucket.name, _key.name, fname, post_hooks)

    def put(self, bucket, key, abspath, overwrite=OVERWRITE_REPLACE,
            pre_hooks=None, stream=sys.stdout, **kwargs):
        if type(abspath) not in (file, str):
            raise TypeError("invalid 'abspath' argument")

        fname = abspath if type(abspath) is str else abspath.name

        try:
            _bucket = bucket if isinstance(bucket, boto.s3.bucket.Bucket) else \
                    self._conn.get_bucket(bucket)
        except boto.exception.S3ResponseError as e:
            raise AuthenticationError(
                    "unable to access bucket '{0}'\n\n{1}".format(
                        bucket, e))

        _key = key if isinstance(key, boto.s3.key.Key) else \
                boto.s3.key.Key(_bucket, key)

        _hook_rslt = apply_hooks(_bucket.name, _key.name, fname, pre_hooks)

        # if the original file is NOT modified by the hooks (i.e. not deleted
        # either), upload the original file first.
        if not _hook_rslt['modified']:
            self._put(_bucket, _key, abspath, 
                    overwrite=overwrite, stream=stream,
                    **kwargs)

        # upload files created by hooks
        for hook, (hkey, hfname) in _hook_rslt['hooks'].items():
            self._put(_bucket, boto.s3.key.Key(_bucket, hkey), hfname,
                    overwrite=overwrite, stream=stream,
                    **kwargs)

        return _hook_rslt

    def _put(self, bucket, key, abspath, overwrite=OVERWRITE_REPLACE,
            stream=None, **kwargs):
        fname = abspath if type(abspath) is str else abspath.name
        func = 'set_contents_from_filename' if type(abspath) is str else \
                'set_contents_from_file'

        if stream:
            stream.write("Uploading: '{0}'\nDestination: '{1}/{2}'\n".format(
                fname, bucket.name, key.name))

        _cb = ProgressBar(fname, stream)
        if not key.exists() or overwrite == self.OVERWRITE_REPLACE:
            getattr(key, func)(abspath, replace=True, cb=_cb, num_cb=100)

        if stream:
            stream.write("\n")
            stream.flush()

    def __del__(self):
        self._conn.close()
