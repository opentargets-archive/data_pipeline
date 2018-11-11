import logging
from logging.config import fileConfig
import sys
import os
import json
import gzip
import pypeln.process as pr
import addict
import uuid
import codecs
import itertools as iters
import more_itertools as miters
import functools as ftools

from mrtarget.Settings import file_or_resource


def to_source(filename):
    f_handle = None
    if filename.endswith('.gz'):
        f_handle = gzip.open(filename, mode='wb')
    else:
        f_handle = open(filename, mode='w')

    return f_handle


def from_source(filename):
    f_handle = None
    if filename.endswith('.gz'):
        f_handle = gzip.open(filename, mode='rb')
    else:
        f_handle = open(filename, mode='r')

    return iters.izip(iters.cycle([filename]),enumerate(f_handle))

class ProcessContext(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = addict.Dict(kwargs)
        self.logger = logging.getLogger(__name__ + '_' + str(os.getpid()))


def parse_line(line):
    (filename, (line_n, l)) = line
    parsed_line = {'file_name': filename,
                   'line_n': line_n,
                   'data_type': '',
                   'data_source': ''}
    try:
        e = json.loads(codecs.decode(l, 'utf-8', 'replace'))
        parsed_line['data_type'] = e['type']
        parsed_line['data_source'] = e['sourceID']

    finally:
        return json.dumps(parsed_line) + os.linesep


def output_stream_on_start():
    pc = ProcessContext()
    pc.logger.debug("called from %s", str(os.getpid()))

    file_name = 'evidences_' + uuid.uuid4().hex + '.json'
    file_handle = to_source(file_name)
    pc.kwargs.file_name = file_name
    pc.kwargs.file_handle = file_handle
    return pc


def output_stream_on_done(_status, process_context):
    process_context.logger.debug('closing file %s', process_context.kwargs.file_name)
    process_context.kwargs.file_handle.close()


def write_lines(x, process_context):
    process_context.kwargs.file_handle.writelines(x)


def main(filenames):
    logger = logging.getLogger(__name__)
    from multiprocessing import cpu_count

    logger.debug('create an iterable of handles from filenames %s', str(filenames))
    in_handles = iters.imap(from_source, filenames)

    logger.debug('create a iterable of lines from all file handles')
    chained_handles = iters.chain.from_iterable(iters.ifilter(lambda e: e is not None, in_handles))

    out_data = ( chained_handles # miters.take(1000, chained_handles)
            | pr.map(parse_line, workers=cpu_count(), maxsize=1000)
            | pr.map(write_lines, workers=2, maxsize=1000, on_start=output_stream_on_start,
                     on_done=output_stream_on_done)
            | pr.to_iterable
    )

    print(miters.ilen(out_data))
    iters.imap(lambda el: el.close(), in_handles)


if __name__ == '__main__':
    fileConfig(file_or_resource('logging.ini'),  disable_existing_loggers=False)
    logging.getLogger().setLevel(logging.DEBUG)

    args = sys.argv[1:]
    print(args)
    main(args)
