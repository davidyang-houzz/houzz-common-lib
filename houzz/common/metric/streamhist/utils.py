#!/usr/bin/env python
"""Some useful utility functions and classes."""

from __future__ import absolute_import
from __future__ import print_function
import ctypes as _ctypes
from sys import platform as _platform
from math import log, sqrt
import types
import collections
from numbers import Number as numeric_types
from six.moves import range
from six.moves import zip

iterator_types = (types.GeneratorType, collections.Iterable)


try:
    from itertools import accumulate
except ImportError:
    # itertools.accumulate only in Py3.x
    def accumulate(iterable):
        it = iter(iterable)
        total = next(it)
        yield total
        for element in it:
            total += element
            yield total


_E = 2.718281828459045

__all__ = ["next_after", "bin_sums", "running_sums", "argmin", "bin_diff", "accumulate"]

if _platform == "linux" or _platform == "linux2":
    _libm = _ctypes.cdll.LoadLibrary('libm.so.6')
    _funcname = 'nextafter'
elif _platform == "darwin":
    try:
        _libm = _ctypes.cdll.LoadLibrary(
            _ctypes.macholib.dyld.dyld_find('libSystem.dylib'))
    except Exception as e:
        _libm = _ctypes.cdll.LoadLibrary('libSystem.dylib')
    _funcname = 'nextafter'
elif _platform == "win32":
    _libm = _ctypes.cdll.LoadLibrary('msvcrt.dll')
    _funcname = '_nextafter'
else:
    # these are the ones I have access to...
    # fill in library and function name for your system math dll
    print("Platform", repr(_platform), "is not supported")
    _sys.exit(0)

_nextafter = getattr(_libm, _funcname)
_nextafter.restype = _ctypes.c_double
_nextafter.argtypes = [_ctypes.c_double, _ctypes.c_double]


def next_after(x, y):
    """Returns the next floating-point number after x in the direction of y."""
    # This implementation comes from here:
    # http://stackoverflow.com/a/6163157/1256988
    return _nextafter(x, y)


def _diff(a, b, weighted):
        diff = b.value - a.value
        if weighted:
            diff *= log(_E + min(a.count, b.count))
        return diff


def bin_diff(array, weighted=False):
    return [_diff(a, b, weighted) for a, b in zip(array[:-1], array[1:])]


def argmin(array):
    # Turns out Python's min and max functions are super fast!
    # http://lemire.me/blog/archives/2008/12/17/fast-argmax-in-python/
    return array.index(min(array))


def bin_sums(array, less=None):
    return [(a.count + b.count)/2. for a, b in zip(array[:-1], array[1:])
            if less is None or b.value <= less]


def running_sums(bins):
    """Calculate running sum of the bin.count.

    NOTE: Append 0 in the front and sum(bin.count) at the end to represent the
          dummy P0(min) and PB1(max) bins, ie, len(result) = len(bins) + 2.

    Args:
        bins (Bin): bins
    Returns:
        list(int): running sums
    """
    result = [0.0]
    prev = 0.0
    sub_total = 0.0
    for b in bins:
        sub_total += (prev + b.count) / 2.0
        result.append(sub_total)
        prev = b.count
    result.append(sub_total + prev / 2.0)
    return result


def linspace(start, stop, num):
    """Custom version of numpy's linspace to avoid numpy depenency."""
    if num == 1:
        return stop
    h = (stop - start) / float(num)
    values = [start + h * i for i in range(num+1)]
    return values


def roots(a, b, c):
    """Super simple quadratic solver."""
    d = b**2.0 - (4.0 * a * c)
    if d < 0:
        raise ValueError("This equation has no real solution!")
    elif d == 0:
        x = -b / (2.0 * a)
        return (x, x)
    else:
        rt = sqrt(d)
        x1 = (-b + rt) / (2.0 * a)
        x2 = (-b - rt) / (2.0 * a)
        return (x1, x2)
