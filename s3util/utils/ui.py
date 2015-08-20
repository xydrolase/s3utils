#!/usr/bin/env python

from __future__ import division

from collections import deque
from math import floor

import time
import sys

def humanize_size(nbytes, formatter=".1f"):
    unit = 'B'
    multiplier = nbytes

    if nbytes >= 1024 and nbytes < 1024*1024:
        unit = 'KB'
        multiplier = float(nbytes) / 1024
    elif nbytes >= 1024*1024 and nbytes < 1024**3:
        unit = 'MB'
        multiplier = float(nbytes) / 1024**2
    elif nbytes >= 1024**3:
        unit = 'GB'
        multiplier = float(nbytes) / 1024**3

    _fmt = "{{0:{fmtter}}}{{1}}".format(fmtter=formatter)
    return _fmt.format(multiplier, unit)

class ProgressBar:
    def __init__(self, name, stream=None, steps=20):
        self.name = name
        self.ts_start = time.time()
        self.stream = stream
        self.steps = steps

        self.prev_bytes = 0
        self.prev_time = self.ts_start

        # used for averaging the last 5 ETAs for smoother estimates.
        self.etas = deque()

    def __call__(self, bytes_transmitted, bytes_total):
        perc = int(round(bytes_transmitted / bytes_total * 100))
        step = int(round(bytes_transmitted / bytes_total * self.steps))

        # download speed (in bps) within the last callback interval.
        intvl_bps = (bytes_transmitted - self.prev_bytes) / \
                (time.time() - self.prev_time)

        # average download speed
        avg_bps = bytes_transmitted / (time.time() - self.ts_start)

        # ETA in seconds
        intvl_eta = 0 if int(intvl_bps) == 0 else \
                int((bytes_total - bytes_transmitted) / intvl_bps)

        self.etas.append(intvl_eta)
        if len(self.etas) > 5:
            self.etas.popleft()

        eta = int(sum(self.etas) / len(self.etas))

        eta_hms = "{0:02d}:{1:02d}:{2:02d}".format(
                eta // 3600, (eta % 3600) // 60, eta % 60)

        pbar = "{0}{1}".format('=' * step,
                '>' if step < 20 else '')

        if self.stream:
            self.stream.write("\r  {0:3d}% [{1}] {2}   {3}/s  eta: {4}".format(
                perc, pbar.ljust(20),
                humanize_size(bytes_total), humanize_size(intvl_bps), eta_hms))

            self.stream.flush()

