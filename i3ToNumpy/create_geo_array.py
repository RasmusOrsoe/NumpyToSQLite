#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position, wildcard-import

"""
Extract positional and calibration info for DOMs
and save the resulting dict in a pkl file for later use
"""

from __future__ import absolute_import, division, print_function

__all__ = ['N_STRINGS', 'N_DOMS', 'extract_gcd', 'parse_args']

__author__ = 'P. Eller, J.L. Lanfranchi'
__license__ = '''Copyright 2017 Philipp Eller and Justin L. Lanfranchi
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.'''

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import bz2
from collections import OrderedDict
import gzip
import hashlib
import os
from os.path import (
    abspath, expanduser, expandvars, dirname, isfile, join, split, splitext
)
import sys

import numpy as np
from six import PY2, BytesIO
from six.moves import cPickle as pickle
import zstandard

if __name__ == '__main__' and __package__ is None:
    RETRO_DIR = dirname(dirname(dirname(abspath(__file__))))
    if RETRO_DIR not in sys.path:
        sys.path.append(RETRO_DIR)
from retro import DATA_DIR, load_pickle
from retro.utils.misc import mkdir


N_STRINGS = 86
N_DOMS = 60


def extract_gcd(gcd_file, outdir=None):
    """Extract info from a GCD in i3 format, optionally saving to a simple
    Python pickle file.
    Parameters
    ----------
    gcd_file : str
    outdir : str, optional
        If provided, the gcd info is saved to a .pkl file with same name as
        `gcd_file` just with extension replaced.
    Returns
    -------
    gcd_info : OrderedDict
        'source_gcd_name': basename of the `gcd_file` provided
        'source_gcd_md5': direct md5sum of `gcd_file` (possibly compressed)
        'source_gcd_i3_md5': md5sum of `gcd_file` after decompressing to .i3
        'geo': (86, 60, 3) array of DOM x, y, z coords in m rel to IceCube coord system
        'rde' : (86, 60) array with relative DOM efficiencies
        'noise' : (86, 60) array with noise rate, in Hz, for each DOM
    """
    gcd_file = expanduser(expandvars(gcd_file))
    src_gcd_dir, src_gcd_basename = split(gcd_file)

    # Strip all recognized extensions to find base file name's "stem," then
    # attach ".pkl" extension to that
    src_gcd_stripped = src_gcd_basename
    while True:
        src_gcd_stripped, ext = splitext(src_gcd_stripped)
        if ext.lower().lstrip('.') not in ['i3', 'pkl', 'bz2', 'gz', 'zst']:
            # reattach unknown "extension"; presumably it's actually part of
            # the filename and not an extesion at all (or an extension we don't
            # care about, or an empty string in the case that there is no dot
            # remaining in the name)
            src_gcd_stripped += ext
            break
    pkl_outfname = src_gcd_stripped + '.pkl'

    pkl_outfpath = None
    if outdir is not None:
        outdir = expanduser(expandvars(outdir))
        mkdir(outdir)
        pkl_outfpath = join(outdir, pkl_outfname)
        if isfile(pkl_outfpath):
            return load_pickle(pkl_outfpath)

    def save_pickle_if_appropriate(gcd_info):
        if pkl_outfpath is not None:
            with open(pkl_outfpath, 'wb') as fobj:
                pickle.dump(gcd_info, fobj, protocol=pickle.HIGHEST_PROTOCOL)

    # Look for existing extracted (pkl) version in choice directories
    look_in_dirs = []
    if src_gcd_dir:
        look_in_dirs.append(src_gcd_dir)
    look_in_dirs += ['.', DATA_DIR]
    if 'I3_DATA' in os.environ:
        look_in_dirs.append('$I3_DATA/GCD')
    look_in_dirs = [expanduser(expandvars(d)) for d in look_in_dirs]

    for look_in_dir in look_in_dirs:
        uncompr_pkl_fpath = join(look_in_dir, pkl_outfname)
        if isfile(uncompr_pkl_fpath):
            gcd_info = load_pickle(uncompr_pkl_fpath)
            save_pickle_if_appropriate(gcd_info)
            return gcd_info

    # If we couldn't find the already-extracted file, find the source file
    # (if user doesn't specify a full path to the file, try in several possible
    # directories)
    if src_gcd_dir:
        look_in_dirs = [src_gcd_dir]
    else:
        look_in_dirs = ['.', DATA_DIR]
        if 'I3_DATA' in os.environ:
            look_in_dirs.append('$I3_DATA/GCD')
    look_in_dirs = [expanduser(expandvars(d)) for d in look_in_dirs]

    src_fpath = None
    for look_in_dir in look_in_dirs:
        fpath = join(look_in_dir, src_gcd_basename)
        if isfile(fpath):
            src_fpath = fpath
            break

    if src_fpath is None:
        raise IOError(
            'Cannot find file "{}" in dir(s) {}'.format(src_gcd_basename, look_in_dirs)
        )

    # Figure out what compression algorithms are used on the file; final state
    # will have `ext_lower` containing either "i3" or "pkl" indicating the
    # basic type of file we have
    compression = []
    src_gcd_stripped = src_gcd_basename
    while True:
        src_gcd_stripped, ext = splitext(src_gcd_stripped)
        ext_lower = ext.lower().lstrip('.')
        if ext_lower in ['gz', 'bz2', 'zst']:
            compression.append(ext_lower)
        elif ext_lower in ['i3', 'pkl']:
            break
        else:
            if ext:
                raise IOError(
                    'Unhandled extension "{}" found in GCD file "{}"'.format(
                        ext, gcd_file
                    )
                )
            raise IOError(
                'Illegal filename "{}"; must have either ".i3" or ".pkl" extesion,'
                " optionally followed by compression extension(s)".format(gcd_file)
            )

    with open(src_fpath, 'rb') as fobj:
        decompressed = fobj.read()

    # Don't hash a pickle file; all we care about is the hash of the original
    # i3 file, which is a value already stored in the pickle file
    if ext_lower == 'i3':
        source_gcd_md5 = hashlib.md5(decompressed).hexdigest()

    for comp_alg in compression:
        if comp_alg == 'gz':
            decompressed = gzip.GzipFile(fileobj=BytesIO(decompressed)).read()
        elif comp_alg == 'bz2':
            decompressed = bz2.decompress(decompressed)
        elif comp_alg == 'zst':
            decompressor = zstandard.ZstdDecompressor()
            decompressed = decompressor.decompress(
                decompressed, max_output_size=100000000
            )

    if ext_lower == 'pkl':
        if PY2:
            gcd_info = pickle.loads(decompressed)
        else:
            gcd_info = pickle.loads(decompressed, encoding='latin1')
        save_pickle_if_appropriate(gcd_info)
        return gcd_info

    # -- If we get here, we have an i3 file -- #

    decompressed_gcd_md5 = hashlib.md5(decompressed).hexdigest()

    from I3Tray import I3Units, OMKey  # pylint: disable=import-error
    from icecube import dataclasses, dataio  # pylint: disable=import-error, unused-variable, unused-import

    gcd = dataio.I3File(gcd_file) # pylint: disable=no-member
    frame = gcd.pop_frame()

    omgeo, dom_cal = None, None
    while gcd.more() and (omgeo is None or dom_cal is None):
        frame = gcd.pop_frame()
        keys = list(frame.keys())
        if 'I3Geometry' in keys:
            omgeo = frame['I3Geometry'].omgeo
        if 'I3Calibration' in keys:
            dom_cal = frame['I3Calibration'].dom_cal

    assert omgeo is not None
    assert dom_cal is not None

    # create output dict
    gcd_info = OrderedDict()
    gcd_info['source_gcd_name'] = src_gcd_basename
    gcd_info['source_gcd_md5'] = source_gcd_md5
    gcd_info['source_gcd_i3_md5'] = decompressed_gcd_md5
    gcd_info['geo'] = np.full(shape=(N_STRINGS, N_DOMS, 3), fill_value=np.nan)
    gcd_info['noise'] = np.full(shape=(N_STRINGS, N_DOMS), fill_value=np.nan)
    gcd_info['rde'] = np.full(shape=(N_STRINGS, N_DOMS), fill_value=np.nan)

    for string_idx in range(N_STRINGS):
        for dom_idx in range(N_DOMS):
            omkey = OMKey(string_idx + 1, dom_idx + 1)
            om = omgeo.get(omkey)
            gcd_info['geo'][string_idx, dom_idx, 0] = om.position.x
            gcd_info['geo'][string_idx, dom_idx, 1] = om.position.y
            gcd_info['geo'][string_idx, dom_idx, 2] = om.position.z
            try:
                gcd_info['noise'][string_idx, dom_idx] = (
                    dom_cal[omkey].dom_noise_rate / I3Units.hertz
                )
            except KeyError:
                gcd_info['noise'][string_idx, dom_idx] = 0.0

            try:
                gcd_info['rde'][string_idx, dom_idx] = dom_cal[omkey].relative_dom_eff
            except KeyError:
                gcd_info['rde'][string_idx, dom_idx] = 0.0

    save_pickle_if_appropriate(gcd_info)

    return gcd_info


def parse_args(description=__doc__):
    """Parse command line args"""
    parser = ArgumentParser(
        description=description,
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-f', '--file', metavar='GCD_FILE', dest='gcd_file', type=str,
        required=True,
        help='Input GCD file. See e.g. files in $I3_DATA/GCD directory.'
    )
    parser.add_argument(
        '--outdir', type=str, required=True,
        help='Directory into which to save the resulting .pkl file',
    )
    return parser.parse_args()


if __name__ == '__main__':
    extract_gcd(**vars(parse_args()))
