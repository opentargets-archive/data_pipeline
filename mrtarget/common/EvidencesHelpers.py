import json
import os
import logging

import functools
import itertools
import uuid
from abc import abstractmethod

import addict

from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.connection import new_es_client
from mrtarget.Settings import Config

import mrtarget.common.IO as IO


_l = logging.getLogger(__name__)


def make_lookup_data(es_client, redis_client):
    return LookUpDataRetriever(es_client,
                        redis_client,
                        data_types=(
                            LookUpDataType.TARGET,
                            LookUpDataType.DISEASE,
                            LookUpDataType.EFO,
                            LookUpDataType.ECO,
                            LookUpDataType.HPO,
                            LookUpDataType.MP
                        ),
                        autoload=True,
                        ).lookup


class ProcessContext(object):
    def __init__(self, **kwargs):
        self.kwargs = addict.Dict(kwargs)
        self.logger = logging.getLogger(__name__ + '_' + str(os.getpid()))

    @abstractmethod
    def put(self, line, **kwargs):
        pass


class ProcessContextFileWriter(ProcessContext):
    def __init__(self, output_folder='./', **kwargs):
        super(ProcessContextFileWriter, self).__init__(**kwargs)
        self.logger.debug("called output_stream from %s", str(os.getpid()))

        self.kwargs.valids_file_name = output_folder + os.path.sep + 'evidences-valid_' + uuid.uuid4().hex + '.json.gz'
        self.kwargs.valids_file_handle = IO.open_to_write(self.kwargs.valids_file_name)

        self.kwargs.invalids_file_name = output_folder + os.path.sep + 'evidences-invalid_' + uuid.uuid4().hex + '.json.gz'
        self.kwargs.invalids_file_handle = IO.open_to_write(self.kwargs.invalids_file_name)

    def put(self, line, **kwargs):
        (left, right) = line
        if right is not None:
            self.kwargs.valids_file_handle.writelines(serialise_object_to_json(right) + os.linesep)
        elif left is not None:
            self.kwargs.invalids_file_handle.writelines(serialise_object_to_json(left) + os.linesep)

    def __del__(self):
        self.logger.debug('closing files %s %s',
                          self.kwargs.valids_file_name,
                          self.kwargs.invalids_file_name)
        self.close()

    def close(self):
        try:
            self.kwargs.valids_file_handle.close()
            self.kwargs.invalids_file_handle.close()
        except:
            pass


class ProcessContextESWriter(ProcessContext):
    def __init__(self, **kwargs):
        super(ProcessContextESWriter, self).__init__(**kwargs)
        self.logger.debug("called output_stream from %s", str(os.getpid()))

        self.kwargs.es_client = new_es_client()
        self.kwargs.es_loader = Loader(es=self.kwargs.es_client)

        self.kwargs.index_name_validated = Config.ELASTICSEARCH_DATA_INDEX_NAME + '-' + 'generic-new'
        self.kwargs.doc_type_validated = Config.ELASTICSEARCH_DATA_DOC_NAME + '-' + 'generic'

        self.kwargs.index_name_invalidated = Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME + '-' + 'generic-new'
        self.kwargs.doc_type_invalidated = Config.ELASTICSEARCH_VALIDATED_DATA_DOC_NAME + '-' + 'generic'

    def put(self, line, **kwargs):
        (left, right) = line
        if right is not None:
            self.kwargs.es_loader.put(body=right['line'], ID=right['id'],
                                index_name=self.kwargs.index_name_validated,
                                doc_type=self.kwargs.doc_type_validated,
                                create_index=True)
        elif left is not None:
            self.kwargs.es_loader.put(body=serialise_object_to_json(left), ID=left['id'],
                                index_name=self.kwargs.index_name_invalidated,
                                doc_type=self.kwargs.doc_type_invalidated,
                                create_index=True)

    def __del__(self):
        self.close()

    def close(self):
        try:
            self.kwargs.es_loader.flush()
            self.kwargs.es_loader.close()
        except:
            pass


def open_writers_on_start(enable_output_to_es=False, output_folder='.'):
    """construct the processcontext to write lines to the files. we have to sets,
    the good validated ones and the failed ones.
    """
    pc = None
    if enable_output_to_es:
        pc = ProcessContextESWriter()
    else:
        pc = ProcessContextFileWriter(output_folder=output_folder)

    return pc


def close_writers_on_done(_status, process_context):
    """close the processcontext after writing to the files."""
    process_context.close()


def serialise_object_to_json(obj):
    serialised_obj = obj
    if not(isinstance(obj, str) or isinstance(obj, unicode)):
        if isinstance(obj, JSONSerializable):
            serialised_obj = obj.to_json()
        else:
            serialised_obj = json.dumps(obj)

    return serialised_obj


def reduce_tuple_with_sum(iterable):
    return functools.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), iterable, (0, 0))


def emit_global_stats_from_evidence(ev):
    """generate a list of tuples containing the required information to map-reduce"""
    def _strip_lit_id(lit_url):
        return lit_url['lit_id'].split('/')[-1]

    internal_ev = ev.evidence
    ev_id = internal_ev['id']
    ev_t = internal_ev['target']['id']
    ev_d = internal_ev['disease']['id']
    ids = []
    r = []

    if 'literature' in internal_ev and 'references' in internal_ev['literature']:
        ids += list(itertools.imap(_strip_lit_id, iter(internal_ev['literature']['references'])))

    if 'unique_experiment_reference' in internal_ev:
        ids += [internal_ev['unique_experiment_reference']]

    r += list(itertools.izip(itertools.cycle([ev_id]),
                             iter(ids),
                             itertools.cycle([ev_t]),
                             itertools.cycle([1])))

    r += list(itertools.izip(itertools.cycle([ev_id]),
                             iter(ids),
                             itertools.cycle([ev_d]),
                             itertools.cycle([1])))
    return r


def make_validated_evs_obj(filename, hash, line, line_n, is_valid=False, explanation_type='', explanation_str='',
                           target_id=None, efo_id=None, data_type=None, id=None):
    return addict.Dict(is_valid=is_valid, explanation_type=explanation_type, explanation_str=explanation_str,
                       target_id=target_id, efo_id=efo_id, data_type=data_type, id=id, line=line, line_n=line_n,
                       filename=filename, hash=hash)
