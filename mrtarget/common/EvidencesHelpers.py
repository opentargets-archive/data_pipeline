import json
import os
import logging
import functools
import uuid
import addict

from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.connection import new_es_client
from mrtarget.constants import Const
import mrtarget.common.IO as IO

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


def make_validated_evs_obj(filename, hash, line, line_n, is_valid=False, explanation_type='', explanation_str='',
                           target_id=None, efo_id=None, data_type=None, id=None):
    return addict.Dict(is_valid=is_valid, explanation_type=explanation_type, explanation_str=explanation_str,
                       target_id=target_id, efo_id=efo_id, data_type=data_type, id=id, line=line, line_n=line_n,
                       filename=filename, hash=hash)

"""
This function is called once on the main thread to do global setup for 
writing into elasticsearch

Note that es_loader.restore_after_bulk_indexing() only restore those indexes it has
setup previously. And it has only setup those by *the same instance* so therefore
the instance has to be shared between init and shutdown
"""
def elasticsearch_global_init(es_loader):
    logger = logging.getLogger(__name__)
    logger.debug('creating elasticsearch indexs')
    es_loader.create_new_index(Const.ELASTICSEARCH_DATA_INDEX_NAME)
    es_loader.create_new_index(Const.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME)

    #need to directly get the versioned index name for this function
    es_loader.prepare_for_bulk_indexing(
        es_loader.get_versioned_index(Const.ELASTICSEARCH_DATA_INDEX_NAME))
    es_loader.prepare_for_bulk_indexing(
        es_loader.get_versioned_index(Const.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME))

"""
This function is called once in each child process to do local setup for 
writing to elasticsearch
"""
def elasticsearch_local_init(es_hosts):
    return Loader(new_es_client(es_hosts)) , 

"""
This function is called on every item within the child processess. It is passed the output
from the local init function as additional arguments.
"""
def elasticsearch_main(line, es_loader):
    (left, right) = line
    if right is not None:
        #valid
        es_loader.put(body=right['line'], ID=right['hash'],
            index_name=Const.ELASTICSEARCH_DATA_INDEX_NAME,
            doc_type=Const.ELASTICSEARCH_DATA_DOC_NAME)
        return (0,1)
    elif left is not None:
        #invalid
        es_loader.put(body=serialise_object_to_json(left), ID=left['id'],
            index_name=Const.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME,
            doc_type=Const.ELASTICSEARCH_VALIDATED_DATA_DOC_NAME)
        return (1,0)


"""
This function is called once in each child process to do local cleanup for 
writing to elasticsearch
It is passed the output from the local init function as additional arguments.
"""
def elasticsearch_local_shutdown(status, es_loader):
    logger = logging.getLogger(__name__)
    logger.debug('local shutdown started') 
    #flush but dont close because it changes index settings
    #and that needs to be done in a single place outside of multiprocessing
    es_loader.flush_all_and_wait(Const.ELASTICSEARCH_DATA_INDEX_NAME)
    es_loader.flush_all_and_wait(Const.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME)
    logger.debug('local shutdown finished') 


"""
This function is called once on the main thread to do global cleanup for
writing into elasticsearch

Note that es_loader.restore_after_bulk_indexing() only restore those indexes it has
setup previously. And it has only setup those by *the same instance* so therefore
the instance has to be shared between init and shutdown
"""
def elasticsearch_global_shutdown(es_loader):
    logger = logging.getLogger(__name__)
    logger.debug('flushing elasticsearch indexes')  
    #ensure everything pending has been flushed to index
    es_loader.flush_all_and_wait(Const.ELASTICSEARCH_DATA_INDEX_NAME)
    es_loader.flush_all_and_wait(Const.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME)
    #restore old pre-load settings
    #note this automatically does all prepared indexes
    es_loader.restore_after_bulk_indexing()
    logger.debug('flushed elasticsearch indexes')  


def dry_run_main(line):
    (left, right) = line
    if right:
        return (0,1)
    elif left:
        return (1,0)

"""
This function is called once on the main thread to do global setup for 
writing into files
"""
def file_global_init(output_folder):
    pass

"""
This function is called once in each child process to do local setup for 
writing into files
"""
def file_local_init(output_folder):
    valids_file_name = output_folder + os.path.sep + 'evidences-valid_' + uuid.uuid4().hex + '.json.gz'
    valids_file_handle = IO.open_to_write(valids_file_name)

    invalids_file_name = output_folder + os.path.sep + 'evidences-invalid_' + uuid.uuid4().hex + '.json.gz'
    invalids_file_handle = IO.open_to_write(invalids_file_name)

    return valids_file_handle, invalids_file_handle


"""
This function is called on every item within the child processess. It is passed the output
from the local init function as additional arguments.
"""
def file_main(line, valids_file_handle, invalids_file_handle):
    (left, right) = line
    if right is not None:
        valids_file_handle.writelines(right['line'] + os.linesep)
        return (0,1)
    elif left is not None:
        invalids_file_handle.writelines(serialise_object_to_json(left) + os.linesep)
        return (1,0)


"""
This function is called once in each child process to do local cleanup for 
writing to files
It is passed the output from the local init function as additional arguments.
"""
def file_local_shutdown(status, valids_file_handle, invalids_file_handle):
    valids_file_handle.close()
    invalids_file_handle.close()

"""
If dry_run : do a dry run
If not dry_run and es_hosts : write to ES
if not dry_run and not_es_hosts and output_folder : write to disk
"""
def setup_writers(dry_run, es_hosts, output_folder):
    global_init = None
    local_init = None
    main = None
    local_shutdown = None
    global_shutdown = None

    if dry_run:
        main = dry_run_main
    elif es_hosts:
        #have to bake the loader object in so that the prepare for bulk indexing works
        es_loader = Loader(new_es_client(es_hosts))
        #use partial to "bake" arguments into the function we return
        global_init = functools.partial(elasticsearch_global_init, es_loader)
        local_init = functools.partial(elasticsearch_local_init, es_hosts)
        main = elasticsearch_main
        local_shutdown = elasticsearch_local_shutdown
        global_shutdown = functools.partial(elasticsearch_global_shutdown, es_loader)
    elif output_folder:
        #use partial to "bake" arguments into the function we return
        global_init = functools.partial(file_global_init, output_folder)
        local_init = functools.partial(file_local_init, output_folder)
        main = file_main
        local_shutdown = file_local_shutdown
    else:
        raise ValueError("Must specify one of dry_run, es_hosts, output_folder")

    return global_init, local_init, main, local_shutdown, global_shutdown
