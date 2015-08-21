#!/usr/bin/env python

from s3util.hooks.base import BaseHook

import os
import subprocess

class CompressionHook(BaseHook):
    extension = None
    command = None

    def __init__(self, keep=False, **kwargs):
        self.keep = keep

        super(CompressionHook, self).__init__(**kwargs)

    def dry_run(self, bucket, key, fname):
        if self.extension is None or self.command is None:
            raise NotImplementedError(
                    "CompressionHook is a generic hook template.")

        return ("".join((key, self.extension)),
                "".join((fname, self.extension)),
                not self.keep)

    def __call__(self, bucket, key, fname):
        _retval = self.dry_run(bucket, key, fname)

        args = [self.command] + (['-k'] if self.keep else [])
        args.append(fname)

        subprocess.check_call(args)

        return _retval


class GzipHook(CompressionHook):
    def __init__(self, **kwargs):
        self.extension = '.gz'
        self.command = 'gzip'

        super(GzipHook, self).__init__(**kwargs)

class Bzip2Hook(CompressionHook):
    def __init__(self):
        self.extension = '.bz2'
        self.command = 'bzip2'

        super(Bzip2Hook, self).__init__(**kwargs)

class TarExtractionHook:
    pass

class DecompressionHook:
    """Hook for decompressing downloaded files.

    Unlike CompressionHook, DecompressionHook can deduce the decompressor based
    on file extension.

    The user may override the decompressor by specifying one explicitly via the
    `command` argument.
    """

    supported_commands = {
            '.gz': 'gunzip',
            '.bz2': 'bunzip2'
    }
    def __init__(self, keep=False, command=None, **kwargs):
        self.command = command
        self.keep = keep

    def dry_run(self, bucket, key, fname):
        if self.command is None:
            prefix, ext = os.path.splitext(fname)
            self.command = self.supported_commands.get(ext, None)

            if self.command is None:
                raise NotImplementedError(
                        ("DecompressionHook cannot deduce the compressor for "
                            "file type '{0}'").format(ext))

        return (key, prefix, not self.keep)

    def __call__(self, bucket, key, fname):
        _retval = self.dry_run(bucket, key, fname)

        args = [self.command] + (['-k'] if self.keep else [])
        args.append(fname)

        subprocess.check_call(args)

        return _retval 
