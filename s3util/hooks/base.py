#!/usr/bin/env python

from __future__ import print_function

from functools import wraps

import re

import six


class MetaHook(type):
    def __init__(self, name, bases, attrs):
        super(MetaHook, self).__init__(name, bases, attrs)

    def __new__(cls, name, bases, attrs):
        if not cls.implements(bases, attrs, '__call__'):
            raise NotImplementedError("class '{0}' is not callable".format(
                name))
        else:
            cls.wraps_filter(attrs, '__call__')

        if not cls.implements(bases, attrs, 'dry_run'):
            raise NotImplementedError(
                "class '{0}' does not implement dry_run()".format(name))
        else:
            cls.wraps_filter(attrs, 'dry_run')

        return super(MetaHook, cls).__new__(cls, name, bases, attrs)

    @classmethod
    def wraps_filter(cls, attrs, method):
        if method not in attrs:
            # the class does not directly implements `method`
            return

        attrs[method] = cls.callable_factory(attrs[method])

    @classmethod
    def callable_factory(cls, call_func):
        @wraps(call_func)
        def _filter_call(self, bucket, key, fname):
            if not self._filter or not hasattr(self._filter, 'match'):
                return call_func(self, bucket, key, fname)
            else:
                # check if the filename passes the filter, if not, there is no need
                # to call the hook explicitly
                if self._not(self._filter.match(fname) is not None):
                    return (key, fname)
                else:
                    return call_func(self, bucket, key, fname)

        return _filter_call

    @classmethod
    def implements(cls, bases, attrs, method):
        implemented = False

        if method in attrs:
            return True

        for base_cls in bases:
            if base_cls is not BaseHook and hasattr(base_cls, method):
                implemented = True
                break

        return implemented
        

class BaseHook(six.with_metaclass(MetaHook)):
    __metaclass__ = MetaHook

    re_regex = re.compile('^(~?)/([^/]+)/$')
    _filter = None

    def __init__(self, **kwargs):
        if 'filter' in kwargs:
            self._build_filter(kwargs['filter'])

    def __call__(self, *args, **kwargs):
        raise NotImplementedError("BaseHook.__call__() must be overrided")

    def dry_run(self, *args, **kwargs):
        raise NotImplementedError("BaseHook.dry_run() must be overrided")

    def _build_filter(self, pattern):
        pattern = pattern.strip()
        
        # inverse match
        inverse = False

        m_regex = BaseHook.re_regex.match(pattern)
        if m_regex:
            inverse, pattern = m_regex.groups()
        else:
            # wildcard
            if pattern[0] == '~':
                pattern = pattern[1:]
                inverse = True

            pattern = pattern.replace('.', '\\.').replace('*', '.*')

        self._filter = re.compile(pattern)
        self._not = (lambda x: x) if inverse else lambda x: not x

class TestHook(BaseHook):
    def __init__(self, name, **kwargs):
        self.name = name
        super(TestHook, self).__init__(**kwargs)

    def dry_run(self, bucket, key, fname):
        pass

    def __call__(self, bucket, key, fname):
        print("Calling hook '{3}' on key: {0}/{1}, file: {2}".format(
            bucket, key, fname, self.name))

