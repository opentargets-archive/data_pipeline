from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
import logging

import simplejson as json
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

from opentargets_urlzsource import URLZSource
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from mrtarget.common.connection import new_es_client
from mrtarget.common.LookupHelpers import LookUpDataRetriever

import tempfile
import sys
#for python3 the module name has changed
if sys.version_info >= (3, 0):
    import dbm
else:
    import anydbm as dbm

import shelve
import codecs
import urllib.request, urllib.parse, urllib.error

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(items, index):
    for ident, item in items:
        action = {}
        action["_index"] = index
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

    def __init__(self, es_hosts, es_index, es_mappings, es_settings,
            es_index_gene, es_index_efo,
            workers_write, queue_write,
            cache_efo, cache_efo_contains,
            cache_target, cache_target_u2e, cache_target_contains,
            chembl_target_uris, 
            chembl_mechanism_uris, 
            chembl_component_uris, 
            chembl_protein_uris, 
            chembl_molecule_uris,
            chembl_indication_uris):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.es_index_gene = es_index_gene
        self.es_index_efo = es_index_efo
        self.workers_write = workers_write
        self.queue_write = queue_write

        self.cache_efo = cache_efo
        self.cache_efo_contains = cache_efo_contains
        self.cache_target = cache_target
        self.cache_target_u2e = cache_target_u2e
        self.cache_target_contains = cache_target_contains

        self.chembl_target_uris = chembl_target_uris
        self.chembl_mechanism_uris = chembl_mechanism_uris
        self.chembl_component_uris = chembl_component_uris
        self.chembl_protein_uris = chembl_protein_uris
        self.chembl_molecule_uris = chembl_molecule_uris
        self.chembl_indication_uris = chembl_indication_uris

        self.logger = logging.getLogger(__name__)

    def process_all(self, dry_run):
        es = new_es_client(self.es_hosts)

        drugs = self.generate(es)
        self.store(es, dry_run, drugs)


    def create_shelf(self, uris, key_f):
        #sanity check inputs
        assert uris is not None
        assert len(uris) > 0
        
        # Shelve creates a file with specific database. Using a temp file requires a workaround to open it.
        # dumbdbm creates an empty database file. In this way shelve can open it properly.

        #note: this file is never deleted!
        filename = tempfile.NamedTemporaryFile(delete=False).name
        shelf = shelve.Shelf(dict=dbm.open(filename, 'n'))
        for uri in uris:
            with URLZSource(uri).open() as f_obj:
                #for python2 we need to decode utf-8
                if sys.version_info < (3, 0):
                    f_obj = codecs.getreader("utf-8")(f_obj)
                for line_no, line in enumerate(f_obj):
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as e:
                        self.logger.error("Unable to read line %d %s %s", line_no, uri, e)
                        raise e
                        
                    key = key_f(obj)
                    if key is not None:
                        if key in shelf:
                            raise ValueError("Duplicate key %s in uri %s" % (key,uri))
                        shelf[key] = obj
        return shelf

    def create_shelf_multi(self, uris, key_f):
        #sanity check inputs
        assert uris is not None
        assert len(uris) > 0

        # Shelve creates a file with specific database. Using a temp file requires a workaround to open it.
        # dumbdbm creates an empty database file. In this way shelve can open it properly.

        #note: this file is never deleted!
        filename = tempfile.NamedTemporaryFile(delete=False).name
        shelf = shelve.Shelf(dict=dbm.open(filename, 'n'))
        for uri in uris:
            with URLZSource(uri).open() as f_obj:
                #for python2 we need to decode utf-8
                if sys.version_info < (3, 0):
                    f_obj = codecs.getreader("utf-8")(f_obj)
                for line_no, line in enumerate(f_obj):
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as e:
                        self.logger.error("Unable to read line %d %s", line_no, uri)
                        raise e

                    key = key_f(obj)
                    if key is not None:
                        existing = shelf.get(key,[])
                        existing.append(obj)
                        shelf[key] = existing
        return shelf


    def clean_ids(self, source, ids):
        if source == "ClinicalTrials":
            #can be comma separated, so split em
            split_ids = set()
            for id in ids:
                for split_id in id.split(","):
                    split_id = split_id.strip()
                    split_ids.add(split_id)
            ids = sorted(split_ids)
        return ids

    def build_urls(self, source, ids):
        urls = []

        if source == "FDA":
            for id in ids:
                args = {}
                args["search"] = "set_id:%s" % id
                urls.append("https://api.fda.gov/drug/label.json?"+urllib.parse.urlencode(args))
        elif source == "ATC":
            for id in ids:
                args = {}
                args["code"] = id
                urls.append("https://www.whocc.no/atc_ddd_index/?"+urllib.parse.urlencode(args))
        elif source == "DailyMed":
            for id in ids:
                #these already come from chembl with setid= in the identifer
                urls.append("https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?"+id)
        elif source == "ClinicalTrials":
            args = {}
            args["id"] = "OR".join(['"%s"' % id for id in ids])
            urls.append("https://clinicaltrials.gov/search?"+urllib.parse.urlencode(args))
        elif source == "PubMed":
            args = {}
            args["query"] = " OR ".join(['EXT_ID:%s' % id for id in ids])
            urls.append("https://europepmc.org/search?"+urllib.parse.urlencode(args))
        elif source == "Wikipedia":
            for id in ids:
                urls.append("https://www.wikipedia.org/"+id)
        elif source == "DOI":
            for id in ids:
                urls.append("http://dx.doi.org/"+id)
        elif source == "Other":
            #assume this is an url
            #TODO check?
            for id in ids:
                urls.append(id)
        elif source == "ISBN":
            #we can't do anything useful with these
            pass
        elif source == "KEGG":
            for id in ids:
                urls.append("https://www.genome.jp/dbget-bin/www_bget?dr:"+id)
        elif source == "PMC":
            for id in ids:
                urls.append("https://www.ncbi.nlm.nih.gov/pmc/articles/"+id)
        else:
            # TODO only report each source once
            self.logger.warning("Unregonized source %s for %s", source, ids)
            return None

        return urls



    def handle_indication(self, indication):

        if "efo_id" in indication \
                and indication["efo_id"] is not None \
                and indication["efo_id"] is not "*":
            out = {}

            efo_id = indication["efo_id"]
            #make sure this is with an underscore not colon
            efo_id = efo_id.replace(":","_")

            out["efo_id"] = efo_id

            if efo_id not in self.lookup_data.available_efos:
                # TODO throw an exception to allow to bubble up
                # TODO only log each one once
                self.logger.warning("Unrecognized disease %s",efo_id)
                return None

            stored_efo = self.lookup_data.available_efos.get_efo(efo_id)

            #get label from our EFO index
            out["efo_label"] = stored_efo["label"]

            #get full URI from our EFO index
            out["efo_uri"] = stored_efo["code"]

            #max phase
            if "max_phase_for_ind" in indication \
                    and indication["max_phase_for_ind"] is not None:
                assert isinstance(indication["max_phase_for_ind"], int)
                out["max_phase_for_indication"] = indication["max_phase_for_ind"]

            # indication references
            if "indication_refs" in indication and indication["indication_refs"] is not None:
                references = {}
                for ref in indication["indication_refs"]:
                    if "ref_type" in ref and ref["ref_type"] is not None \
                            and "ref_id" in ref and ref["ref_id"] is not None:

                        #don't keep the URL, can build a better one later to handle multi-id

                        assert isinstance(ref["ref_type"], str)
                        ref_type = ref["ref_type"]
                        assert isinstance(ref["ref_id"], str)
                        ref_id = ref["ref_id"] 

                        #create a set to ensure uniqueness
                        if ref_type not in references:
                            references[ref_type] = set()
                        references[ref_type].add(ref_id)
                    else:
                        # warn if one of these is missing
                        self.logger.warn("missing ref_type and/or ref_id")
                
                for ref_type in references:
                    if "references" not in out:
                        out["references"] = []

                    reference = {}
                    reference["source"] = ref_type
                    reference["ids"] = tuple(sorted(references[ref_type]))
                    reference["ids"] = self.clean_ids(reference["source"], reference["ids"])
                    urls = self.build_urls(reference["source"], reference["ids"])
                    if urls is not None:
                        reference["urls"] = urls
                    #TODO build a URL list that can handle multiple ids (when possible)
                    if reference not in out["references"]:
                        out["references"].append(reference)
                
                if "references" in out:
                    out["references"] = sorted(out["references"],key = lambda x:x["source"])

            return out
        else:
            #indication without EFO ID, skipping
            return None

    '''
    This will create the mechanism ES dictionary from the provided shelf dict
    '''
    def handle_mechanism(self, mech, targets):
        out = {}

        #handle target information from target endpoint
        #do this first, so we can stop early if its a target we are not interested in
        if "target_chembl_id" in mech and mech["target_chembl_id"] is not None:
            assert isinstance(mech["target_chembl_id"], str)
            target_id = mech["target_chembl_id"]
            target = targets[target_id]

            if "target_components" not in target \
                or target["target_components"] is None \
                or len(target["target_components"]) == 0:
                # we can't handle this at the moment, skipping
                #self.logger.warning("No component for %s",target_id)
                return None

            for target_component in target["target_components"]:

                out_component = {}
                assert "accession" in target_component                
                target_accession = target_component["accession"]
                if target_accession is None:
                    self.logger.warning("skipping unaccessioned component in %s", target_id)
                    continue

                #at the end of this we need a valid ensembl id that we have in the gene index
                ensembl_id = None
                if target_accession in self.lookup_data.available_genes:
                    ensembl_id = target_accession
                    out_component["ensembl"] = ensembl_id
                else:
                    try:
                        ensembl_id = self.lookup_data.available_genes.get_uniprot2ensembl(target_accession)
                    except ValueError as e:
                        #multiple ensembl ids per protein
                        #log with a warning, and ignore
                        self.logger.warning("multiple ensembl ids for uniprot id %s",target_accession)
                        continue

                    if ensembl_id is not None:
                        out_component["ensembl"] = ensembl_id
                    else:
                        # TODO only log each one once
                        self.logger.warning("Unrecognized target accession %s",target_accession)
                        continue

                gene = self.lookup_data.available_genes.get_gene(ensembl_id)

                if "approved_name" in gene \
                        and gene["approved_name"] is not None \
                        and len(gene["approved_name"]) > 0:
                    out_component["approved_name"] = gene["approved_name"]

                if "approved_symbol" in gene \
                        and gene["approved_symbol"] is not None \
                        and len(gene["approved_symbol"]) > 0:
                    assert isinstance(gene["approved_symbol"], str)
                    out_component["approved_symbol"] = gene["approved_symbol"]

                if "target_components" not in out:
                    out["target_components"] = []
                out["target_components"].append(out_component)

            #add some information from the chembl source
            # TODO what if this is different from ensembl ?
            if "target_type" in target and target["target_type"] is not None:
                assert isinstance(target["target_type"], str)
                #chembl stores them as all-caps, we want them to be pretty
                out["target_type"] = target["target_type"].lower()

            if "pref_name" in target and target["pref_name"] is not None:
                assert isinstance(target["pref_name"], str)
                out["target_name"] = target["pref_name"]

        else:
            # no target_chembl_id - should this be dropped?
            self.logger.warning("no target_chembl_id found")
            return None

        if "action_type" in mech and mech["action_type"] is not None:
            # convert to lowercase
            out["action_type"] = mech["action_type"].lower()

        if "mechanism_of_action" in mech and mech["mechanism_of_action"] is not None:
            assert isinstance(mech["mechanism_of_action"], str)
            out["description"] = mech["mechanism_of_action"]


        if "mechanism_refs" in mech and mech["mechanism_refs"] is not None:
            references = {}
            for ref in mech["mechanism_refs"]:
                if "ref_type" in ref and ref["ref_type"] is not None \
                        and "ref_id" in ref and ref["ref_id"] is not None:

                    #don't keep the URL, can build a better one later to handle multi-id

                    assert isinstance(ref["ref_type"], str)
                    ref_type = ref["ref_type"]
                    assert isinstance(ref["ref_id"], str)
                    ref_id = ref["ref_id"] 

                    #create a set to ensure uniqueness
                    if ref_type not in references:
                        references[ref_type] = set()
                    references[ref_type].add(ref_id)
                else:
                    # warn if one of these is missing
                    self.logger.warn("missing ref_type and/or ref_id")
            
            for ref_type in references:
                if "references" not in out:
                    out["references"] = []

                reference = {}
                reference["source"] = ref_type
                reference["ids"] = tuple(sorted(references[ref_type]))
                reference["ids"] = self.clean_ids(reference["source"], reference["ids"])
                urls = self.build_urls(reference["source"], reference["ids"])
                if urls is not None:
                    reference["urls"] = urls
                #TODO build a URL list that can handle multiple ids (when possible)
                if reference not in out["references"]:
                    out["references"].append(reference)
            
            if "references" in out:
                out["references"] = sorted(out["references"],key=lambda x : x["source"])

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
        else:
            #default to explicitly false 
            drug["internal_compound"] = False

        if "molecule_type" in mol and mol["molecule_type"] is not None:
            #TODO format check
            assert isinstance(mol["molecule_type"], str), ident
            drug["type"] = mol["molecule_type"]
            
        if "pref_name" in mol and mol["pref_name"] is not None:
            #TODO casing? always uppercase, do we inital case, lower case?
            assert isinstance(mol["pref_name"], str), ident
            drug["pref_name"] = mol["pref_name"]
            
        if "first_approval" in mol and mol["first_approval"] is not None:
            assert isinstance(mol["first_approval"], int), ident
            assert mol["first_approval"] > 1900, ident
            assert mol["first_approval"] < 2100, ident
            drug["year_first_approved"] = mol["first_approval"]

        if "max_phase" in mol and mol["max_phase"] is not None:
            #check this is 0 1 2 3 4
            assert isinstance(mol["max_phase"], int), ident
            #this should be an integer?
            drug["max_clinical_trial_phase"] = mol["max_phase"]

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
            assert isinstance(mol["withdrawn_reason"], str), ident
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
            assert isinstance(mol["withdrawn_country"], str), ident
            for country in mol["withdrawn_country"].split(";"):
                countries.add(country.strip())
            drug["withdrawn_country"] = sorted(countries)

        if "withdrawn_class" in mol and mol["withdrawn_class"] is not None:
            #TODO check always string
            #TODO check only present when withdrawn_flag
            #TODO casing?
            classes = set()
            assert isinstance(mol["withdrawn_class"], str), ident
            for clazz in mol["withdrawn_class"].split(";"):
                classes.add(clazz.strip())
            drug["withdrawn_class"] = sorted(classes)

        if "black_box_warning" in mol and mol["black_box_warning"] is not None:
            #unicode converted to true/false
            #check it comes in as a unicode
            assert isinstance(mol["black_box_warning"], str), \
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

                    assert isinstance(molecule_synonym["syn_type"], str)
                    syn_type = molecule_synonym["syn_type"]
                    assert isinstance(
                            molecule_synonym["molecule_synonym"], str)
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

                    assert isinstance(ref["xref_src"], str)
                    ref_type = ref["xref_src"]
                    assert isinstance(ref["xref_id"], str)
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
            chebi_id = mol["chebi_par_id"]

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
            drug["cross_references"] = sorted(drug["cross_references"],key=lambda x: x["source"])

        if ident in indications:
            drug["indications"] = []
            for indication in indications[ident]:
                out = self.handle_indication(indication)
                if out is not None:
                    drug["indications"].append(out)

        if ident in mechanisms:
            drug["mechanisms_of_action"] = []
            for mechanism in mechanisms[ident]:
                out = self.handle_mechanism(mechanism, all_targets)
                if out is not None:
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

        # TODO add child prefered name as a synonym ?

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

        if "max_clinical_trial_phase" in child_drug:
            if "max_clinical_trial_phase" in drug:
                #compare and take highest
                if child_drug["max_clinical_trial_phase"] > drug["max_clinical_trial_phase"]:
                    drug["max_clinical_trial_phase"] = child_drug["max_clinical_trial_phase"]
            else:
                #in child but not parent, add to parent
                drug["max_clinical_trial_phase"] = child_drug["max_clinical_trial_phase"]

        if "year_first_approved" in child_drug:
            if "year_first_approved" in drug:
                #compare and take lowest
                if child_drug["year_first_approved"] < drug["year_first_approved"]:
                    drug["year_first_approved"] = child_drug["year_first_approved"]
            else:
                #in child but not parent, add to parent
                drug["year_first_approved"] = child_drug["year_first_approved"]

        # TODO withdrawn_year and other withdrawn

        # TODO black box warning

    def generate(self, es):

        # pre-load into indexed shelf dicts

        self.logger.info("Starting pre-loading")

        #create lookup tables
        self.lookup_data = LookUpDataRetriever(es,  
            gene_index = self.es_index_gene,
            gene_cache_size = self.cache_target,
            gene_cache_u2e_size = self.cache_target_u2e,
            gene_cache_contains_size = self.cache_target_contains,
            efo_index = self.es_index_efo,
            efo_cache_size = self.cache_efo,
            efo_cache_contains_size = self.cache_efo_contains
            ).lookup


        # these are all separate files
        # intentional, partly because its what chembl API gives us, and partly because
        # it is easier for partners to add information to existing chembl records

        # TODO potentially load these in separate processes?

        self.logger.debug("Loading molecules")
        mols = self.create_shelf_multi(self.chembl_molecule_uris, get_parent_id)
        self.logger.debug("Loaded %d molecules", len(mols))
        self.logger.debug("Loading indications")
        indications = self.create_shelf_multi(self.chembl_indication_uris, lambda x : x["molecule_chembl_id"])
        self.logger.debug("Loaded %d indications", len(indications))
        self.logger.debug("Loading mechanisms")
        mechanisms = self.create_shelf_multi(self.chembl_mechanism_uris, lambda x : x["molecule_chembl_id"])
        self.logger.debug("Loaded %d mechanisms", len(mechanisms))
        self.logger.debug("Loading targets")
        targets = self.create_shelf(self.chembl_target_uris, lambda x : x["target_chembl_id"])
        self.logger.debug("Loaded %d targets", len(targets))
        self.logger.info("Completed pre-loading")

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

            assert parent_mol is not None, ident

            #TODO sure no grandparenting
            
            child_mols = sorted(child_mols, key = lambda x: x["molecule_chembl_id"])

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
                drugs[ident] = drug

        return drugs

    def store(self, es, dry_run, data):
        self.logger.info("Starting drug storage")
        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):
            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(list(data.items()), self.es_index)
            failcount = 0
            if not dry_run:
                results = None
                if self.workers_write > 0:
                    results = elasticsearch.helpers.parallel_bulk(es, actions,
                            thread_count=self.workers_write,
                            queue_size=self.queue_write, 
                            chunk_size=chunk_size)
                else:
                    results = elasticsearch.helpers.streaming_bulk(es, actions,
                            chunk_size=chunk_size)
                for success, details in results:
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s relations failed to index" % failcount)
        
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
        
