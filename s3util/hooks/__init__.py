#!/usr/bin/env python

import os
import importlib
import subprocess

def load_hook(hook):
    _module, _func = hook.rsplit('.', 1)
    try:
        _module = importlib.import_module(_module)
        hook_entry = getattr(_module, _func)

        return hook_entry
    except ImportError:
        return None

def apply_hooks(bucket, key, fname, hooks):
    if not os.path.exists(fname):
        raise OSError("file '{0}' does not exist.".format(fname))

    if hooks is None:
        hooks = []

    hook_rslt = {}
    for hook, hfilter, kwargs in hooks:
        hook_cls = load_hook(hook)(**kwargs)

        # hook_entry should return three variables:
        # ret_fname: the file produced by the hook (if any, otherwise None)
        # modified: if the original file is modified (or deleted, so that 
        #           other hooks would no longer apply)
        ret_key, ret_fname, modified = hook_cls(
                bucket, key, fname, **kwargs)
        hook_rslt[hook] = (ret_key, ret_fname) 

        if modified:
            return dict(abspath=fname, modified=True, hooks=hook_rslt)

    return dict(abspath=fname, modified=False, hooks=hook_rslt) 

