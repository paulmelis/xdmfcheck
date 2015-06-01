#!/usr/bin/env python2
#
# Check Xdmf file against referenced HDF5 files.
#
# Paul E.C. Melis <paul.melis@surfsara.nl>
# SURFsara Visualization group
#
# Limitations:
# - Checks DataItem elements referencing an HDF5 file only
# - Xdmf version 2 only
#
# Todo:
# - Cache a limited set of opened HDF5 files, so we don't open+close files
#   for every dataset check
#
#
# Copyright (c) 2015, SURFsara BV
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions 
# are met:
#
# 1. Redistributions of source code must retain the above copyright 
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in 
#    the documentation and/or other materials provided with the 
#    distribution.
#
# 3. Neither the name of the copyright holder nor the names of its 
#    contributors may be used to endorse or promote products derived 
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
# POSSIBILITY OF SUCH DAMAGE.

import sys, os, re, traceback
import h5py
import lxml.etree as ET

VERBOSE = False

def check_dataitem_hdf(elem, xdmf_number_type, xdmf_precision, xdmf_dimensions):
    
    text = elem.text        
    text = text.strip()     # XXX what does the spec say we should handle whitespace here?
    
    pat = re.compile('^(.*):(/.*)$')
    m = pat.search(text)
    if not m:
        print '(%s:%d) Failed to match file:dataset pattern in "%s"' % (xmffile, elem.sourceline, text)
        return

    hdf5_file = m.group(1)
    dataset_path = m.group(2)

    if VERBOSE:
        print 'Checking file "%s" for dataset "%s", numbertype "%s", precision "%d", dimensions "%s" ' % (fname, dataset_path, xdmf_number_type, xdmf_precision, ' '.join(xdmf_dimensions))

    if not os.path.exists(hdf5_file):
        print '(%s:%d) Referenced HDF5 file %s does not exist' % (xmffile, elem.sourceline, hdf5_file)
        return

    f = h5py.File(hdf5_file, 'r')

    try:
        dset = f[dataset_path]

        # Check datatype

        dataset_datatype = dset.dtype

        xdmf_datatype = ''
        if xdmf_number_type == 'Float':
            xdmf_datatype = 'float%d' % (xdmf_precision*8)
        elif xdmf_number_type == 'Int':
            xdmf_datatype = 'int%d' % (xdmf_precision*8)
        else:
            raise ValueError('Unhandled xdmf_number_type "%s"' % xdmf_number_type)

        if xdmf_datatype != '':
            if xdmf_datatype != dataset_datatype:
                print '(%s:%d) Data types in Xdmf (%s, %d) and HDF5 file (%s) don\'t match' % (xmffile, elem.sourceline, xdmf_number_type, xdmf_precision, dataset_datatype)

        # Check rank and shape

        dataset_shape = dset.shape
        dataset_rank = len(dataset_shape)
        
        xdmf_rank = len(xdmf_dimensions)
        
        dim_ok = True

        if xdmf_rank == dataset_rank:
            for i in xrange(xdmf_rank):
                if dataset_shape[i] != xdmf_dimensions[i]:
                    dim_ok = False
                    break
        else:
            dim_ok = False
                    
        if not dim_ok:
            print '(%s:%d) Dimensions in Xdmf %s and HDF5 file %s don\'t match' % (xmffile, elem.sourceline, tuple(xdmf_dimensions), tuple(dataset_shape))

    except KeyError:
        print '(%s:%d) Dataset "%s" not found in HDF5 file %s' % (xmffile, elem.sourceline, dataset_path, hdf5_file)
    except:
        print '(%s:%d) Exception during HDF5 processing' % (xmffile, elem.sourceline)
        traceback.print_exc()

    f.close()


def check_dataitem(elem):

    # Defaults
    precision = 4
    number_type = 'Float'
    format = 'XML'
    #item_type = 'Uniform'
    #endian = 'Native'
    #compression = 'raw'
    #seek = 0
    
    # Check attributes

    if 'NumberType' in elem.attrib:
        number_type = elem.attrib['NumberType']

    if 'Precision' in elem.attrib:
        precision = int(elem.attrib['Precision'])

    if 'Format' in elem.attrib:
        format = elem.attrib['Format']

    if 'Dimensions' not in elem.attrib:
        print '(%s:%d) no dimensions in DataItem element!' % (xmffile, elem.sourceline)
        return        
            
    # Check values
    
    if number_type not in ['Float', 'Int', 'UInt', 'Char', 'UChar']:
        print '(%s:%d) Invalid NumberType "%s" specified' % (xmffile, elem.sourceline, number_type)
        
    if precision not in [1, 2, 4, 8]:        
        print '(%s:%d) Invalid Precision %d specified' % (xmffile, elem.sourceline, precision)        
    elif precision == 2:
        if number_type not in ['Int', 'UInt']:
            print '(%s:%d) Precision 2 only allowed for NumberType "Int" and "UInt"' % (xmffile, elem.sourceline)        
            
    if format not in ['XML', 'HDF', 'Binary']:
        print '(%s:%d) Invalid Format "%s" specified' % (xmffile, elem.sourceline, format)        
            
    dimensions = elem.attrib['Dimensions']
    dimensions = map(int, dimensions.split())
    
    #assert endian in ['Native', 'Big', 'Little']       # only with Format == Binary
    #assert compression in ['Raw', 'Zlib', 'BZip2']     # XXX        

    # Process
            
    if format == 'HDF':     
        check_dataitem_hdf(elem, number_type, precision, dimensions)                    
    else:
        print '(%s:%d) Ignoring DataItem element having format "%s"' % (xmffile, elem.sourceline, format)


xmffile = sys.argv[1]
if not os.path.isfile(xmffile):
    print 'File %s not found' % xmffile
    sys.exit(-1)

tree = ET.parse(xmffile)

root = tree.getroot()

if root.tag != 'Xdmf':
    print 'Root tag of %s is no "Xdmf"!' % xmffile
    sys.exit(-1)
    
for elem in root.findall('.//DataItem'):
    check_dataitem(elem)

