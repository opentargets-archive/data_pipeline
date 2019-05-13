import logging

import simplejson as json
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

from opentargets_urlzsource import URLZSource
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from mrtarget.common.connection import new_es_client

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

def get_parent_id(mol):
    #if it has a parent use the parents id
    if "molecule_hierarchy" in mol and mol["molecule_hierarchy"] is not None \
            and "parent_chembl_id" in mol["molecule_hierarchy"] \
            and mol["molecule_hierarchy"]["parent_chembl_id"] is not None:
        return mol["molecule_hierarchy"]["parent_chembl_id"]
    else:
    #if there is no parent, use its own id
        #print("Unable to find .molecule_hierarchy.parent_chembl_id for %s"%mol["molecule_chembl_id"])
        return mol["molecule_chembl_id"]

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
                    if key is not None:
                        if str(key) in shelf:
                            raise ValueError("Duplicate key %s in uri %s" % (key,uri))
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

                    key = key_f(obj)
                    if key is not None:
                        if str(key) not in shelf:
                            shelf[str(key)] = [obj]
                        else:
                            shelf[str(key)] = shelf[str(key)]+[obj]
        return shelf

    def handle_indication(self, indication):
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
            assert isinstance(mech["mechanism_of_action"], unicode)
            out["description"] = mech["mechanism_of_action"]

        if "mechanism_refs" in mech and mech["mechanism_refs"] is not None:
            references = {}
            for ref in mech["mechanism_refs"]:
                #TODO warn if one of these is missing
                if "ref_type" in ref and ref["ref_type"] is not None \
                        and "ref_id" in ref and ref["ref_id"] is not None:

                    #don't keep the URL, can build a better one later to handle multi-id

                    assert isinstance(ref["ref_type"], unicode)
                    ref_type = ref["ref_type"]
                    assert isinstance(ref["ref_id"], unicode)
                    ref_id = ref["ref_id"] 

                    #create a set to ensure uniqueness
                    if ref_type not in references:
                        references[ref_type] = set()
                    references[ref_type].add(ref_id)
            
            for ref_type in references:
                if "references" not in out:
                    out["references"] = []

                reference = {}
                reference["source"] = ref_type
                reference["ids"] = tuple(sorted(references[ref_type]))
                #TODO build a URL list that can handle multiple ids (when possible)
                if reference not in out["references"]:
                    out["references"].append(reference)
            
            if "references" in out:
                out["references"] = sorted(out["references"])

        #handle target information from target endpoint
        if "target_chembl_id" in mech and mech["target_chembl_id"] is not None:
            assert isinstance(mech["target_chembl_id"], unicode)
            target_id = mech["target_chembl_id"]
            target = targets[target_id]

            if "target_type" in target and target["target_type"] is not None:
                #TODO check this can be lower cased
                assert isinstance(target["target_type"], unicode)
                out["target_type"] = target["target_type"].lower()

            if "pref_name" in target and target["pref_name"] is not None:
                assert isinstance(target["pref_name"], unicode)
                out["target_name"] = target["pref_name"]

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
            assert isinstance(mol["internal_compound"], bool), ident
            drug["internal_compound"] = mol["internal_compound"]

        if "molecule_type" in mol and mol["molecule_type"] is not None:
            #TODO format check
            assert isinstance(mol["molecule_type"], unicode), ident
            drug["type"] = mol["molecule_type"]
            
        if "pref_name" in mol and mol["pref_name"] is not None:
            #TODO casing? always uppercase, do we inital case, lower case?
            assert isinstance(mol["pref_name"], unicode), ident
            drug["pref_name"] = mol["pref_name"]
            
        if "first_approval" in mol and mol["first_approval"] is not None:
            assert isinstance(mol["first_approval"], int), ident
            assert mol["first_approval"] > 1900, ident
            assert mol["first_approval"] < 2100, ident
            drug["year_first_approved"] = mol["first_approval"]

        if "max_phase" in mol and mol["max_phase"] is not None:
            #TODO check this is 0 1 2 3 4
            assert isinstance(mol["max_phase"], unicode) \
                    or isinstance(mol["max_phase"], int), ident
            if isinstance(mol["max_phase"], unicode):
                drug["max_clinical_trial_phase"] = mol["max_phase"]
            else:
                #this should be an integer?
                drug["max_clinical_trial_phase"] = unicode(str(mol["max_phase"]), "utf-8")

        if "withdrawn_flag" in mol and mol["withdrawn_flag"] is not None:
            #TODO check always true
            assert isinstance(mol["withdrawn_flag"], bool), ident
            drug["withdrawn_flag"] = mol["withdrawn_flag"]

        if "withdrawn_reason" in mol and mol["withdrawn_reason"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #note, this is noisy e.g.
            #  "Self-poisonings"
            #  "Self-poisoning"
            #  "Self-Poisonings"
            reasons = set()
            assert isinstance(mol["withdrawn_reason"], unicode), ident
            for reason in mol["withdrawn_reason"].split(";"):
                reasons.add(reason.strip())
            drug["withdrawn_reason"] = sorted(reasons)

        if "withdrawn_year" in mol and mol["withdrawn_year"] is not None:
            assert isinstance(mol["withdrawn_year"], int), ident
            assert mol["withdrawn_year"] > 1900, ident
            assert mol["withdrawn_year"] < 2100, ident
            drug["withdrawn_year"] = mol["withdrawn_year"]

        if "withdrawn_country" in mol and mol["withdrawn_country"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #split and trim by semicolon
            #TODO casing?
            countries = set()
            assert isinstance(mol["withdrawn_country"], unicode), ident
            for country in mol["withdrawn_country"].split(";"):
                countries.add(country.strip())
            drug["withdrawn_country"] = sorted(countries)

        if "withdrawn_class" in mol and mol["withdrawn_class"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #TODO casing?
            classes = set()
            assert isinstance(mol["withdrawn_class"], unicode), ident
            for clazz in mol["withdrawn_class"].split(";"):
                classes.add(clazz.strip())
            drug["withdrawn_class"] = sorted(classes)

        if "black_box_warning" in mol and mol["black_box_warning"] is not None:
            #unicode converted to true/false
            #check it comes in as a unicode
            assert isinstance(mol["black_box_warning"], unicode), \
                "%s black_box_warning = %s " % (ident,repr(mol["black_box_warning"]))
            #convert unicode to an integer - will throw if it can't
            bbw = int(mol["black_box_warning"])
            if bbw == 0:
                drug["black_box_warning"] = False
            elif bbw == 1:
                drug["black_box_warning"] = True
            else:
                raise ValueError("Unexpected value for black_box_warning: %d"%bbw)

        if "molecule_synonyms" in mol and mol["molecule_synonyms"] is not None:
            # use set to avoid duplicates
            synonyms = set() 
            trade_names = set()

            for molecule_synonym in mol["molecule_synonyms"]:
                if "molecule_synonym" in molecule_synonym \
                        and molecule_synonym["molecule_synonym"] is not None \
                        and "syn_type" in molecule_synonym \
                        and molecule_synonym["syn_type"] is not None:

                    assert isinstance(molecule_synonym["syn_type"], unicode)
                    syn_type = molecule_synonym["syn_type"]
                    assert isinstance(
                            molecule_synonym["molecule_synonym"], unicode)
                    synonym = molecule_synonym["molecule_synonym"]

                    if "TRADE_NAME" == syn_type.upper():
                        trade_names.add(synonym)
                    else:
                        synonyms.add(synonym)

            if len(synonyms) > 0:
                drug["synonyms"] = sorted(synonyms)
            if len(synonyms) > 0:
                drug["trade_names"] = sorted(trade_names)

        if "cross_references" in mol and mol["cross_references"] is not None:
            references = {}

            for ref in mol["cross_references"]:
                #TODO warn if one of these is missing
                if "xref_src" in ref and ref["xref_src"] is not None \
                        and "xref_id" in ref and ref["xref_id"] is not None:

                    #don't keep the URL, can build a better one later to handle multi-id

                    assert isinstance(ref["xref_src"], unicode)
                    ref_type = ref["xref_src"]
                    assert isinstance(ref["xref_id"], unicode)
                    ref_id = ref["xref_id"] 

                    #create a set to ensure uniqueness
                    if ref_type not in references:
                        references[ref_type] = set()
                    references[ref_type].add(ref_id)
            
            for ref_type in references:
                if "cross_references" not in drug:
                    drug["cross_references"] = []

                reference = {}
                reference["source"] = ref_type
                reference["ids"] = tuple(sorted(references[ref_type]))
                #TODO build a URL list that can handle multiple ids (when possible)
                drug["cross_references"].append(reference)

        if "chebi_par_id" in mol and mol["chebi_par_id"] is not None:
            assert isinstance(mol["chebi_par_id"], int)
            chebi_id = unicode(str(mol["chebi_par_id"]), "utf-8")

            if "cross_references" not in drug:
                drug["cross_references"] = []
            reference = {}
            reference["source"] = "ChEBI"
            reference["ids"] = (chebi_id,)
            #TODO build a URL 
            if reference not in drug["cross_references"]:
                drug["cross_references"].append(reference)

        #sort cross references for consistent order after all possible ones have been added
        if "cross_references" in drug:
            drug["cross_references"] = sorted(drug["cross_references"])

        if ident in indications:
            drug["indications"] = []
            for indication in indications[ident]:
                out = self.handle_indication(indication)
                drug["indications"].append(out)

        if ident in mechanisms:
            drug["mechanisms_of_action"] = []
            for mechanism in mechanisms[ident]:
                out = self.handle_mechanism(mechanism, all_targets)
                drug["mechanisms_of_action"].append(out)

        return drug


    def handle_drug_child(self, drug, ident, mol, indications, mechanisms, targets):

        #get a drug object for the child, validated and cleaned
        child_drug = self.handle_drug(ident, mol, indications, mechanisms, targets)

        #add extra information to the drug based on the child

        if "child_chembl_ids" not in drug:
            drug["child_chembl_ids"] = []
        drug["child_chembl_ids"].append(child_drug["id"])

        if "synonyms" in child_drug:
            for synonym in child_drug["synonyms"]:
                if "synonyms" in drug:
                    if synonym not in drug["synonyms"]:
                        drug["synonyms"].append(synonym)
                        drug["synonyms"] = sorted(drug["synonyms"])
                else:
                    drug["synonyms"] = [synonym]

        if "trade_names" in child_drug:
            for name in child_drug["trade_names"]:
                if "trade_names" in drug:
                    if name not in drug["trade_names"]:
                        drug["trade_names"].append(name)
                        drug["trade_names"] = sorted(drug["trade_names"])
                else:
                    drug["trade_names"] = [name]

        if "indications" in child_drug:
            for indication in child_drug["indications"]:
                if "indications" in drug:
                    if indication not in drug["indications"]:
                        drug["indications"].append(indication)
                else:
                    drug["indications"] = [indication]

        if "mechanisms_of_action" in child_drug:
            for mechanism in child_drug["mechanisms_of_action"]:
                if "mechanisms_of_action" in drug:
                    if mechanism not in drug["mechanisms_of_action"]:
                        drug["mechanisms_of_action"].append(mechanism)
                else:
                    drug["mechanisms_of_action"] = [mechanism]



    def generate(self):

        # pre-load into indexed shelf dicts

        self.logger.debug("Starting pre-loading")

        # these are all separate files
        # intentional, partly because its what chembl API gives us, and partly because
        # it is easier for partners to add information to existing chembl records

        # TODO potentially load these in separate processes?

        mols = self.create_shelf_multi(self.chembl_molecule_uris, get_parent_id)
        indications = self.create_shelf_multi(self.chembl_indication_uris, lambda x : x["molecule_chembl_id"])
        mechanisms = self.create_shelf_multi(self.chembl_mechanism_uris, lambda x : x["molecule_chembl_id"])
        targets = self.create_shelf_multi(self.chembl_target_uris, lambda x : x["target_chembl_id"])

        self.logger.debug("Completed pre-loading")

        drugs = {}
        #TODO finish

        for ident in mols:

            parent_mol = None
            child_mols = []

            for mol in mols[ident]:
                if mol["molecule_chembl_id"] == ident:
                    #this is the parent
                    assert parent_mol is None
                    parent_mol = mol
                else:
                    #this is a child
                    assert mol not in child_mols
                    child_mols.append(mol)

            #TODO sure no grandparenting
            child_mols = sorted(child_mols)

            drug = self.handle_drug(ident, parent_mol,
                indications, mechanisms,
                targets)

            #append information from children
            for child_mol in child_mols:
                self.handle_drug_child(drug, child_mol["molecule_chembl_id"], child_mol,
                    indications, mechanisms,
                    targets)

            if "indications" in drug:
                drug["number_of_indications"] = len(drug["indications"])
            else:
                drug["number_of_indications"] = 0

            if "mechanisms_of_action" in drug:
                drug["number_of_mechanisms_of_action"] = len(drug["mechanisms_of_action"])
            else:
                drug["number_of_mechanisms_of_action"] = 0

            # only keep those with indications or mechanisms 
            if drug["number_of_indications"] > 0 \
                    or drug["number_of_mechanisms_of_action"] > 0:
                self.logger.debug("storing %s",ident)
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
        
