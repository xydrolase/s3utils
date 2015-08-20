#!/usr/bin/env python

import os
import importlib
import subprocess

__hook_registry = {}

def load_hook(hook):
    if hook in __hook_registry:
        return __hook_registry[hook]
    else:
        _module, _func = hook.rsplit('.', 1)
    try:
        _module = importlib.import_module(_module)
        hook_entry = getattr(_module, _func)

        return hook_entry
    except (AttributeError, ImportError):
        return None

def apply_hooks(bucket, key, fname, hooks, dry=False):
    if not dry and not os.path.exists(fname):
        raise OSError("file '{0}' does not exist.".format(fname))

    if hooks is None:
        hooks = []

    hook_rslt = {}
    for hname, kwargs in hooks:
        _hook = load_hook(hname)
        if _hook is None:
            continue
        hook_cls = _hook(**kwargs)

        # a legitimate hook should return three variables:
        # ret_fname: the file produced by the hook (if any, otherwise None)
        # modified: if the original file is modified (or deleted, so that 
        #           other hooks would no longer apply)
        if dry:
            ret_key, ret_fname, modified = hook_cls.dry_run(
                    bucket, key, fname)
        else:
            ret_key, ret_fname, modified = hook_cls(
                    bucket, key, fname)

        hook_rslt[hname] = (ret_key, ret_fname) 

        if modified:
            return dict(abspath=fname, modified=True, hooks=hook_rslt)

    return dict(abspath=fname, modified=False, hooks=hook_rslt) 

