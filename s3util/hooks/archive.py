#!/usr/bin/env python

class CompressionHook:
    def __init__(self, keep=False, **kwargs):
        self.extension = None
        self.command = None
        self.keep = keep

    def __call__(self, bucket, key, fname):
        if self.extension is None or self.command is None:
            raise NotImplementedError(
                    "CompressionHook is a generic hook template.")

        args = [self.command] + ['-k'] if self.keep else []
        args.append(fname)

        subprocess.check_call(args)

        return ("{0}.{1}".format(key, self.extension),
                "{0}.{1}".format(fname, self.extension),
                not self.keep)

class GzipHook(CompressionHook):
    def __init__(self):
        self.extension = '.gz'
        self.command = 'gzip'

class Bzip2Hook(CompressionHook):
    def __init__(self):
        self.extension = '.bz2'
        self.command = 'bzip2'

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

    def __call__(self, bucket, key, fname):
        if self.command is None:
            prefix, ext = os.path.splitext(fname)
            self.command = DecompressionHook.supported_commands.get(ext, None)

            if self.command is None:
                raise NotImplementedError(
                        ("DecompressionHook cannot deduce the compressor for "
                            "file type '{0}'").format(ext))

            args = [self.command] + ['-k'] if self.keep else []
            args.append(fname)

            subprocess.check_call(args)

            return (key, prefix, not self.keep)
