#!/usr/bin/env python

import time
import sys
import os

import boto

from math import ceil

from filechunkio import FileChunkIO

from s3util.utils.ui import ProgressBar
from s3util.hooks import load_hook, apply_hooks

OVERWRITE_SKIP, OVERWRITE_REPLACE, OVERWRITE_SUFFIX, \
    OVERWRITE_VERSION = range(4)

try:
    _ = unicode
except NameError:
    # compat for Python 3
    unicode = str

class AuthenticationError(Exception):
    pass

class Connection:
    def __init__(self, access_key_id=None, secret_access_key=None):
        self._conn = boto.connect_s3(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key)

    def get(self, bucket, key, abspath, overwrite=OVERWRITE_REPLACE,
            post_hooks=None, stream=sys.stdout):

        if type(abspath) is unicode:
            abspath = str(abspath)

        if type(abspath) not in (file, str):
            raise TypeError("invalid 'abspath' argument")


        if overwrite >= OVERWRITE_VERSION:
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
            os.makedirs(dirname)

        if os.path.exists(fname) and \
                overwrite == OVERWRITE_SKIP:
            return apply_hooks(_bucket.name, _key.name, fname, post_hooks)

        if not os.path.exists(fname) or \
                overwrite == OVERWRITE_REPLACE:
            getattr(_key, func)(abspath)

        return apply_hooks(_bucket.name, _key.name, fname, post_hooks)

    def put(self, bucket, key, abspath, overwrite=OVERWRITE_REPLACE,
            pre_hooks=None, stream=sys.stdout, **kwargs):

        if type(abspath) is unicode:
            abspath = str(abspath)

        if type(abspath) not in (file, str):
            raise TypeError("invalid 'abspath' argument")

        fname = abspath if type(abspath) is str else\
            os.path.abspath(abspath.name)

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
        fname = abspath if type(abspath) is str else \
            os.path.abspath(abspath.name)

        st_size = os.stat(fname).st_size
        oversized = st_size > 5 * 1024 * 1024 * 1024

        func = 'set_contents_from_filename' if type(abspath) is str else \
                'set_contents_from_file'

        _exists = key.exists()

        if stream:
            stream.write("Uploading: '{0}'\nDestination: '{1}/{2}'\n".format(
                fname, bucket.name, key.name))

        _cb = ProgressBar(fname, stream)

        if _exists and overwrite == OVERWRITE_SKIP:
            if stream:
                stream.write(
                    "Key '{1}' exists in bucket '{0}', skipped.\n".format(
                        bucket.name, key.name))
            return
        elif not _exists or overwrite == OVERWRITE_REPLACE:
            if not oversized:
                getattr(key, func)(abspath, replace=True, cb=_cb, num_cb=100)
            else:
                self._put_multipart(bucket, key, fname, cb=_cb, num_cb=100,
                                    stream=stream, st_size=st_size)
        else:
            # TODO: needs implementation
            raise NotImplementedError(
                ("OVERWRITE_SUFFIX and OVERWRITE_VERSION are not supported "
                 "at this moment"))

        if stream:
            stream.write("\n")
            stream.flush()

    def _put_multipart(self, bucket, key, abspath, cb=None, num_cb=100,
                       stream=None, chunk_size=100*1024*1024, st_size=None):

        if not st_size:
            st_size = os.stat(abspath).st_size

        mp = bucket.initiate_multipart_upload(key.name)
        chunk_count = int(ceil(st_size / float(chunk_size)))

        for cidx in range(chunk_count):
            if stream:
                stream.write("[{0}/{1}] Multipart uploading...\n".format(
                    cidx+1, chunk_count))

            # reset the callback progressbar's starting time to now.
            if cb:
                cb.reset()

            with FileChunkIO(
                    abspath, 'r', offset=chunk_size*cidx,
                    bytes=min(chunk_size, st_size - chunk_size*cidx)) as fp:

                mp.upload_part_from_file(fp, part_num=cidx+1, cb=cb,
                                         num_cb=num_cb, replace=True)
            if stream:
                stream.write("\n")

        mp.complete_upload()

    def __del__(self):
        self._conn.close()
