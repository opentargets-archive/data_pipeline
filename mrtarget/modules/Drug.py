import logging

import simplejson as json
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

from opentargets_urlzsource import URLZSource
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from mrtarget.common.connection import new_es_client
from mrtarget.common.chembl_lookup import ChEMBLLookup

import tempfile
import dbm
import shelve
import codecs

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(items, index, doc):
    for ident, item in items:
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = ident
        #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
        #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
        action["_source"] = item

        yield action


class DrugProcess(object):

    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            workers_write, queue_write,
            chembl_target_uris, 
            chembl_mechanism_uris, 
            chembl_component_uris, 
            chembl_protein_uris, 
            chembl_molecule_uris,
            chembl_indication_uris):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.workers_write = workers_write
        self.queue_write = queue_write
        self.chembl_target_uris = chembl_target_uris
        self.chembl_mechanism_uris = chembl_mechanism_uris
        self.chembl_component_uris = chembl_component_uris
        self.chembl_protein_uris = chembl_protein_uris
        self.chembl_molecule_uris = chembl_molecule_uris
        self.chembl_indication_uris = chembl_indication_uris

        self.logger = logging.getLogger(__name__)

    def process_all(self, dry_run):
        drugs = self.generate()
        self.store(dry_run, drugs)


    def create_shelf(self, uris, key_f):
        # Shelve creates a file with specific database. Using a temp file requires a workaround to open it.
        # dumbdbm creates an empty database file. In this way shelve can open it properly.

        #note: this file is never deleted!
        filename = tempfile.NamedTemporaryFile(delete=False).name
        shelf = shelve.Shelf(dict=dbm.open(filename, 'n'))
        for uri in uris:
            with URLZSource(uri).open() as f_obj:
                f_obj = codecs.getreader("utf-8")(f_obj)
                for line_no, line in enumerate(f_obj):
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as e:
                        self.logger.error("Unable to read line %d %s %s", line_no, uri, e)
                        raise e
                        
                    key = key_f(obj)
                    shelf[str(key)] = obj
        return shelf

    def create_shelf_multi(self, uris, key_f):
        # Shelve creates a file with specific database. Using a temp file requires a workaround to open it.
        # dumbdbm creates an empty database file. In this way shelve can open it properly.

        #note: this file is never deleted!
        filename = tempfile.NamedTemporaryFile(delete=False).name
        shelf = shelve.Shelf(dict=dbm.open(filename, 'n'))
        for uri in uris:
            with URLZSource(uri).open() as f_obj:
                f_obj = codecs.getreader("utf-8")(f_obj)
                for line_no, line in enumerate(f_obj):
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as e:
                        self.logger.error("Unable to read line %d %s", line_no, uri)
                        raise e

                    key = str(key_f(obj))
                    if key not in shelf:
                        shelf[key] = [obj]
                    else:
                        shelf[key] = shelf[key]+[obj]
        return shelf

    def handle_indications(self, indication):
        out = {}

        if "efo_id" in indication \
                and indication["efo_id"] is not None \
                and indication["efo_id"] is not "*":
            #TODO ideally we want a full URI here
            #TODO make sure this is an ID we care about
            #TODO make sure this is with an underscore not colon
            out["efo_id"] = indication["efo_id"]
            #TODO get label from our EFO index

        return out

    '''
    This will create the mechanism ES dictionary from the provided shelf dict
    '''
    def handle_mechanism(self, mech, targets):
        out = {}

        if "action_type" in mech and mech["action_type"] is not None:
            # convert to lowercase
            out["action_type"] = mech["action_type"].lower()

        if "mechanism_of_action" in mech and mech["mechanism_of_action"] is not None:
            out["description"] = str(mech["mechanism_of_action"])
        if "mechanism_refs" in mech and mech["mechanism_refs"] is not None:
            references = {}
            for ref in mech["mechanism_refs"]:
                #TODO warn if one of these is missing
                if "ref_type" in ref and ref["ref_type"] is not None \
                        and "ref_id" in ref and ref["ref_id"] is not None:

                    #don't keep the URL, can build a better one later to handle multi-id
                    #ref_type = unicode(ref["ref_type"], "utf-8")
                    #ref_id = unicode(ref["ref_id"], "utf-8")
                    ref_type = str(ref["ref_type"])
                    ref_id = str(ref["ref_id"])

                    #create a set to ensure uniqueness
                    if ref_type not in references:
                        references[ref_type] = set()
                    references[ref_type].add(ref_id)
            
            for ref_type in references:
                if "references" not in out:
                    out["references"] = []

                reference = {}
                reference["source"] = ref_type
                reference["ids"] = sorted(references[ref_type])
                #TODO build a URL list that can handle multiple ids (when possible)

        #handle target information from target endpoint
        if "target_chembl_id" in mech and mech["target_chembl_id"] is not None:
            target_id = str(mech["target_chembl_id"])
            target = targets[target_id]

            if "target_type" in target and target["target_type"] is not None:
                #TODO check this can be lower cased
                out["target_type"] = str(target["target_type"]).lower()

            if "pref_name" in target and target["pref_name"] is not None:
                out["target_name"] = str(target["pref_name"])

            #TODO target_symbols

        return out


    '''
    This will create the drug dictionary object suitable for storing in elasticsearch
    from the provided shelf-backed dictionaries of relevant chembl endpoint data
    '''
    def handle_drug(self, ident, mol, indications, mechanisms, all_targets):

        drug = {}
        drug["id"] = ident

        if "internal_compound" in mol and mol["internal_compound"] is not None:
            # note, not in chembl
            drug["internal_compound"] = bool(mol["internal_compound"])

        if "type" in mol and mol["molecule_type"] is not None:
            #TODO format check
            drug["type"] = str(mol["molecule_type"])
            
        if "first_approval" in mol and mol["first_approval"] is not None:
            #TODO format check
            drug["first_approval"] = str(mol["first_approval"])

        if "max_clinical_trial_phase" in mol and mol["max_phase"] is not None:
            #TODO check this is 0 1 2 3 4
            drug["max_clinical_trial_phase"] = str(mol["max_phase"])

        if "chebi_par_id" in mol and mol["chebi_par_id"] is not None:
            #TODO check this is always an int
            drug["chebi_par_id"] = str(mol["chebi_par_id"])
            
        if "pref_name" in mol and mol["pref_name"] is not None:
            #TODO casing? always uppercase, do we inital case, lower case?
            drug["pref_name"] = str(mol["pref_name"])
            
        if "therapeutic_flag" in mol and mol["therapeutic_flag"] is not None:
            #TODO check always true?
            drug["therapeutic_flag"] = bool(mol["therapeutic_flag"])
            
        if "usan_stem" in mol and mol["usan_stem"] is not None:
            #may be prefix or suffix
            drug["usan_stem"] = str(mol["usan_stem"])

        if "parenteral" in mol and mol["parenteral"] is not None:
            #TODO check always true
            drug["parenteral"] = bool(mol["parenteral"])

        if "withdrawn_flag" in mol and mol["withdrawn_flag"] is not None:
            #TODO check always true
            drug["withdrawn_flag"] = bool(mol["withdrawn_flag"])

        if "withdrawn_reason" in mol and mol["withdrawn_reason"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #note, this is noisy e.g.
            #  "Self-poisonings"
            #  "Self-poisoning"
            #  "Self-Poisonings"
            drug["withdrawn_reason"] = str(mol["withdrawn_reason"])

        if "withdrawn_year" in mol and mol["withdrawn_year"] is not None:
            #TODO check always sensible
            #not everything flagged as withdrawn has this
            drug["withdrawn_year"] = str(mol["withdrawn_year"])

        if "withdrawn_country" in mol and mol["withdrawn_country"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #split and trim by semicolon
            #TODO casing?
            countries = set()
            for country in str(mol["withdrawn_country"]).split(";"):
                countries.add(country.strip())
            drug["withdrawn_country"] = sorted(countries)

        if "withdrawn_class" in mol and mol["withdrawn_class"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #TODO casing?
            classes = set()
            for clazz in str(mol["withdrawn_class"]).split(";"):
                classes.add(clazz.strip())
            drug["withdrawn_class"] = sorted(classes)

        if "black_box_warning" in mol and mol["black_box_warning"] is not None:
            #converted to true/false
            bbw = int(mol["black_box_warning"])
            if bbw == 0:
                drug["black_box_warning"] = False
            elif bbw == 1:
                drug["black_box_warning"] = True
            else:
                raise ValueError("Unexpected value for black_box_warning: %d"%bbw)

        if "molecule_hierarchy" in mol and mol["molecule_hierarchy"] is not None:

            if "molecule_chembl_id" in mol["molecule_hierarchy"] \
                    and mol["molecule_hierarchy"]["molecule_chembl_id"] is not None:
                #TODO is this needed? same as id ?
                if "molecule_hierarchy" not in drug:
                    drug["molecule_hierarchy"] = {}
                drug["molecule_hierarchy"]["molecule_chembl_id"] = \
                    str(mol["molecule_hierarchy"]["molecule_chembl_id"])

            if "parent_chembl_id" in mol["molecule_hierarchy"] \
                    and mol["molecule_hierarchy"]["parent_chembl_id"] is not None:
                #TODO is this needed? same as id ?
                if "molecule_hierarchy" not in drug:
                    drug["molecule_hierarchy"] = {}
                drug["molecule_hierarchy"]["parent_chembl_id"] = \
                    str(mol["molecule_hierarchy"]["parent_chembl_id"])


        if "molecule_synonyms" in mol and mol["molecule_synonyms"] is not None:
            # use set to avoid duplicates
            synonyms = set() 
            trade_names = set()

            for molecule_synonym in mol["molecule_synonyms"]:
                if "molecule_synonym" in molecule_synonym \
                        and molecule_synonym["molecule_synonym"] is not None \
                        and "syn_type" in molecule_synonym \
                        and molecule_synonym["syn_type"] is not None:

                    syn_type = str(molecule_synonym["syn_type"])
                    synonym = str(molecule_synonym["molecule_synonym"])

                    if "TRADE_NAME" == syn_type.upper():
                        trade_names.add(synonym)
                    else:
                        synonyms.add(synonym)

            #TODO do we need "synonyms" or "syn_type" fields ?

            if len(synonyms) > 0:
                drug["molecule_synonyms"] = sorted(synonyms)

        if "cross_references" in mol and mol["cross_references"] is not None:
            cross_references = list()  # this is a list because no frozendict
            for mol_cross_reference in mol["cross_references"]:
                drug_cross_reference = {}
                if "xref_src" in mol_cross_reference \
                        and mol_cross_reference["xref_src"] is not None:
                    drug_cross_reference["source"] = str(mol_cross_reference["xref_src"])

                if "xref_id" in mol_cross_reference \
                        and mol_cross_reference["xref_id"] is not None:
                    drug_cross_reference["id"] = str(mol_cross_reference["xref_id"])

                #TODO build a URL list that can handle multiple ids (when possible)

                if len(drug_cross_reference) > 0 \
                        and drug_cross_reference not in cross_references:
                    cross_references.append(drug_cross_reference)

            if len(synonyms) > 0:
                drug["cross_references"] = sorted(cross_references)

        if indications is not None and len(indications) > 0:
            drug["indications"] = []
            for indication in indications:
                out = self.handle_indications(indication)
                drug["indications"].append(out)
            drug["number_of_indications"] = len(drug["indications"])
        else:
            drug["number_of_indications"] = 0

        if mechanisms is not None and len(mechanisms) > 0:
            drug["mechanisms_of_action"] = []
            for mechanism in mechanisms:
                out = self.handle_mechanism(mechanism, all_targets)
                drug["mechanisms_of_action"].append(out)
            drug["number_of_mechanisms_of_action"] = len(drug["mechanisms_of_action"])
        else:
            drug["number_of_mechanisms_of_action"] = 0

        return drug

    def generate(self):

        # pre-load into indexed shelf dicts

        self.logger.debug("Starting pre-loading")

        # these are all separate files
        # intentional, partly because its what chembl API gives us, and partly because
        # it is easier for partners to add information to existing chembl records

        # TODO potentially load these in separate processes?
        mols = self.create_shelf(self.chembl_molecule_uris, lambda x : x["molecule_chembl_id"])
        indications = self.create_shelf_multi(self.chembl_indication_uris, lambda x : x["molecule_chembl_id"])
        mechanisms = self.create_shelf_multi(self.chembl_mechanism_uris, lambda x : x["molecule_chembl_id"])
        targets = self.create_shelf_multi(self.chembl_target_uris, lambda x : x["target_chembl_id"])

        self.logger.debug("Completed pre-loading")

        drugs = {}
        #TODO finish

        for ident in mols:
            mol = mols[ident]
            indications_list = []
            if ident in indications_list:
                indications_list = indications[ident]
            mechanisms_list = []
            if ident in mechanisms:
                mechanisms_list = mechanisms[ident]

            drug = self.handle_drug(ident, mol,
                indications_list, mechanisms_list,
                targets)

            # only keep those with indications or mechanisms 
            if drug["number_of_indications"] > 0 \
                    or drug["number_of_mechanisms_of_action"] > 0:
                drugs[ident] = drug

        return drugs

    def store(self, dry_run, data):
        self.logger.debug("Starting storage")
        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):
            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(data.items(), self.es_index, self.es_doc)
            failcount = 0
            if not dry_run:
                for result in elasticsearch.helpers.parallel_bulk(es, actions,
                        thread_count=self.workers_write, queue_size=self.queue_write, 
                        chunk_size=chunk_size):
                    success, details = result
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s failed to index" % failcount)
        
        self.logger.debug("Completed storage")


    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):
        self.logger.info("Starting QC")

        #number of drug entries
        drug_count = 0
        #Note: try to avoid doing this more than once!
        for drug_entry in Search().using(es).index(index).query(MatchAll()).scan():
            drug_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["drug.count"] = drug_count

        self.logger.info("Finished QC")
        return metrics
        
