import hashlib
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

import opentargets_validator.helpers

from mrtarget.Settings import file_or_resource, Config
from mrtarget.common.EvidenceJsonUtils import DatatStructureFlattener


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


def validate_evidence_on_start():
    pc = ProcessContext()
    pc.logger.debug("called validate_evidence on_start from %s", str(os.getpid()))
    pc.logger.debug("creating schema validators")

    schemas_map = Config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS
    for schema_name, schema_uri in schemas_map.iteritems():
        # per kv we create the validator and instantiate it
        pc.logger.info('generate_validator_from_schema %s using the uri %s',
                         schema_name, schema_uri)
        pc.kwargs.validators[schema_name] = opentargets_validator.helpers.generate_validator_from_schema(schema_uri)
    return pc


def validate_evidence_on_done(_status, process_context):
    process_context.logger.debug("called validate_evidence on_done from %s", str(os.getpid()))


def validate_evidence(line, process_context):
    (filename, (line_n, l)) = line

    validated_evs = addict.Dict(is_valid=False, explanation_type='', explanation_str='', target_id=None,
                                efo_id=None,data_type=None, id=None, line=l, line_n=line_n,
                                filename=filename, hash='')

    data_type = None
    parsed_line = None
    parsed_line_bad = None
    try:
        parsed_line = json.loads(codecs.decode(l, 'utf-8', 'replace'))
        validated_evs.id = DatatStructureFlattener(parsed_line).get_hexdigest()

    except Exception as e:
        validated_evs.explanation_type = 'unparseable_json'
        validated_evs.id = hashlib.md5(line).hexdigest()
        return (validated_evs, None)

    if ('label' in parsed_line or 'type' in parsed_line):
        # setting type from label in case we have label??
        if 'label' in parsed_line:
            parsed_line['type'] = parsed_line.pop('label', None)

        data_type = parsed_line['type']

    else:
        validated_evs.explanation_type = 'key_fields_missing'
        return (validated_evs, None)

    if data_type is None:
        validated_evs.explanation_type = 'missing_datatype'
        return (validated_evs, None)

    if data_type not in Config.EVIDENCEVALIDATION_DATATYPES:
        validated_evs.explanation_type = 'unsupported_datatype'
        return (validated_evs, None)

    # validate line
    validation_errors = \
        [str(e) for e in process_context.kwargs.validators[data_type].iter_errors(parsed_line)]

    if validation_errors:
        # here I have to log all fails to logger and elastic
        error_messages = ' '.join(validation_errors).replace('\n', ' ; ').replace('\r', '')

        error_messages_len = len(error_messages)

        # capping error message to 2048
        error_messages = error_messages if error_messages_len <= 2048 \
            else error_messages[:2048] + ' ; ...'

        validated_evs.explanation_type = 'validation_error'
        validated_evs.explanation_str = error_messages

        return (validated_evs, None)

    target_id = None
    efo_id = None
    # generate fantabulous dict from addict
    evidence_obj = addict.Dict(parsed_line)
    evidence_obj.unique_association_fields['datasource'] = evidence_obj.sourceID


    if evidence_obj.target.id:
        target_id = evidence_obj.target.id
    if evidence_obj.disease.id:
        efo_id = evidence_obj.disease.id

    # flatten but is it always valid unique_association_fields?
    validated_evs.hash = \
        DatatStructureFlattener(evidence_obj.unique_association_fields).get_hexdigest()
    evidence_obj.id = validated_evs.hash
    return (None, evidence_obj)

    if efo_id:
        # Check disease term or phenotype term
        #if (short_disease_id not in self.lookup_data.available_efos) and \
        if (efo_id not in self.lookup_data.efo_ontology.current_classes) and \
                (efo_id not in self.lookup_data.hpo_ontology.current_classes) and \
                (efo_id not in self.lookup_data.mp_ontology.current_classes):# or \
                # (disease_id in self.efo_uncat):
            explanation['invalid_disease'] = efo_id
            disease_failed = True
        if (efo_id in self.lookup_data.efo_ontology.obsolete_classes) or \
                (efo_id in self.lookup_data.hpo_ontology.obsolete_classes) or \
                (efo_id in self.lookup_data.mp_ontology.obsolete_classes):

            explanation['obsolete_disease'] = efo_id
    else:
        explanation['missing_disease'] = True
        disease_failed = True

    # CHECK GENE/PROTEIN IDENTIFIER Check Ensembl ID, UniProt ID
    # and UniProt ID mapping to a Gene ID
    # http://identifiers.org/ensembl/ENSG00000178573
    if target_id:
        if 'ensembl' in target_id:
            # ensembl_id = ensemblMatch.groups()[0].rstrip("\s")
            ensembl_id = target_id.split('/')[-1]
            if not ensembl_id in self.lookup_data.available_genes:
                gene_failed = True
                explanation['unknown_ensembl_gene'] = ensembl_id
            elif ensembl_id in self.lookup_data.non_reference_genes:
                explanation['nonref_ensembl_gene'] = ensembl_id
                gene_mapping_failed = True

        elif 'uniprot' in target_id:
            uniprot_id =  target_id.split('/')[-1]
            if uniprot_id not in self.lookup_data.uni2ens:
                gene_failed = True
                explanation['unknown_uniprot_entry'] = uniprot_id
            elif not self.lookup_data.uni2ens[uniprot_id]:#TODO:this will not happen wit the current gene processing pipeline
                gene_mapping_failed = True
                gene_failed = True
                explanation['missing_ensembl_xref_for_uniprot_entry'] = uniprot_id

            elif (uniprot_id in self.lookup_data.uni2ens) and  \
                            self.lookup_data.uni2ens[uniprot_id] in self.lookup_data.available_genes  and \
                            'is_reference' in  self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]  and \
                            (not self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]['is_reference'] is True) :
                gene_failed = True
                explanation['nonref_ensembl_xref_for_uniprot_entry'] = uniprot_id
            else:
                try:
                    reference_target_list = self.lookup_data.available_genes[self.lookup_data.uni2ens[uniprot_id]]['is_reference'] is True
                except KeyError:
                    reference_target_list = []
                if reference_target_list:
                    target_id = 'http://identifiers.org/ensembl/%s' % reference_target_list[0]
                else:
                    # get the first one, needs a better way
                    target_id = self.lookup_data.uni2ens[uniprot_id]
                # self.logger.info("Found target id being: %s for %s" %(target_id, uniprot_id))
                if target_id is None:
                    self.log_acc.log(l.INFO, "Found no target id for %s", uniprot_id)


    # If there is no target id after the processing step
    if target_id is None:
        explanation['missing_target_id'] = True
        gene_failed = True

    # flag as valid or not
    if not (disease_failed or gene_failed or other_failures):
        is_valid = True
    else:
        explanation['disease_error'] = disease_failed
        explanation['gene_error'] = gene_failed
        explanation['gene_mapping_failed'] = gene_mapping_failed
        self.log_acc.log(l.ERROR, 'evidence validation step failed at the end with an '
                    'explanation %s', str(explanation))


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
    (left, right) = x
    if right is not None:
        process_context.kwargs.file_handle.writelines(json.dumps(right) + os.linesep)


def error_stream_on_start():
    pc = ProcessContext()
    pc.logger.debug("called error_stream on_start from %s", str(os.getpid()))

    file_name = 'validations_' + uuid.uuid4().hex + '.json'
    file_handle = to_source(file_name)
    pc.kwargs.file_name = file_name
    pc.kwargs.file_handle = file_handle
    return pc


def error_stream_on_done(_status, process_context):
    process_context.logger.debug('closing file %s', process_context.kwargs.file_name)
    process_context.kwargs.file_handle.close()


def error_write_lines(x, process_context):
    (left, right) = x
    if left is not None:
        process_context.kwargs.file_handle.writelines(json.dumps(left) + os.linesep)

    return x

def main(filenames):
    logger = logging.getLogger(__name__)
    from multiprocessing import cpu_count

    logger.debug('create an iterable of handles from filenames %s', str(filenames))
    in_handles = iters.imap(from_source, filenames)

    logger.debug('create a iterable of lines from all file handles')
    chained_handles = iters.chain.from_iterable(iters.ifilter(lambda e: e is not None, in_handles))

    out_data = (miters.take(1000, chained_handles)
            | pr.map(validate_evidence, workers=cpu_count(), maxsize=1000)
            | pr.map(error_write_lines, workers=2, maxsize=1000, on_start=error_stream_on_start,
                     on_done=error_stream_on_done)
            | pr.filter(lambda el: el[1] is not None, workers=3, maxsize=1000)
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
