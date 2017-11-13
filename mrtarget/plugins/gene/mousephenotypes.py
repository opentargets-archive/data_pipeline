from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.Settings import Config
import sys
import urllib2
import ujson as json
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.DEBUG)

PHENOTYPE_CATEGORIES = {
    "MP:0005386" : "behavior/neurological phenotype",
    "MP:0005375" : "adipose tissue phenotype",
    "MP:0005385" : "cardiovascular system phenotype",
    "MP:0005384" : "cellular phenotype",
    "MP:0005382" : "craniofacial phenotype",
    "MP:0005381" : "digestive/alimentary phenotype",
    "MP:0005380" : "embryo phenotype",
    "MP:0005379" : "endocrine/exocrine phenotype",
    "MP:0005378" : "growth/size/body region phenotype",
    "MP:0005377" : "hearing/vestibular/ear phenotype",
    "MP:0005397" : "hematopoietic system phenotype",
    "MP:0005376" : "homeostasis/metabolism phenotype",
    "MP:0005387" : "immune system phenotype",
    "MP:0010771" : "integument phenotype",
    "MP:0005371" : "limbs/digits/tail phenotype",
    "MP:0005370" : "liver/biliary system phenotype",
    "MP:0010768" : "mortality/aging",
    "MP:0005369" : "muscle phenotype",
    "MP:0002006" : "neoplasm",
    "MP:0003631" : "nervous system phenotype",
    "MP:0002873" : "normal phenotype",
    "MP:0001186" : "pigmentation phenotype",
    "MP:0005367" : "renal/urinary system phenotype",
    "MP:0005389" : "reproductive system phenotype",
    "MP:0005388" : "respiratory system phenotype",
    "MP:0005390" : "skeleton phenotype",
    "MP:0005394" : "taste/olfaction phenotype",
    "MP:0005391" : "vision/eye phenotype"
}

class MousePhenotypes(IPlugin):

    def __init__(self):
        super(MousePhenotypes, self).__init__()
        self.loader = None
        self.r_server = None
        self.mouse_genes = dict()
        self.ancestors = dict()
        self.lookup_data = None
        self.mps = dict()
        self.mp_labels = dict()
        self.mp_to_label = dict()
        self.top_levels = dict()
        self.homologs = dict()
        self.human_genes = dict()
        self.not_found_genes = dict()
        self.human_ensembl_gene_ids = dict()
        self.tqdm_out = None

    def print_name(self):
        logging.info("MousePhenotypes gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):

        self.loader = loader
        self.r_server = r_server
        self.tqdm_out = tqdm_out

        self._get_mp_classes()

        self.get_genotype_phenotype()

        self.assign_to_human_genes()

        self.write_all_to_file(filename=Config.GENOTYPE_PHENOTYPE_OUTPUT)

        for gene_id, gene in tqdm(genes.iterate(),
                                  desc='Adding phenotype data from MGI',
                                  unit=' gene',
                                  file=self.tqdm_out):
            ''' extend gene with related mouse phenotype data '''
            if gene.approved_symbol in self.human_genes:
                    logging.info("Adding %i phenotype data from MGI to gene %s" % (len(self.human_genes[gene.approved_symbol]["mouse_orthologs"][0]["phenotypes"]), gene.approved_symbol))
                    gene.mouse_phenotypes = self.human_genes[gene.approved_symbol]["mouse_orthologs"]

    def _get_mp_classes(self):
        logging.info("_get_mp_classes")
        lookup_data_types = (LookUpDataType.MP_LOOKUP,)
        logging.debug(LookUpDataType.MP)
        self.lookup_data = LookUpDataRetriever(self.loader.es,
                                               self.r_server,
                                               data_types=lookup_data_types,
                                               autoload=True
                                               ).lookup
        mp_class = None
        for mp_id in self.lookup_data.available_mps.get_available_mp_ids():
            logging.debug(mp_id)
            mp_class = self.lookup_data.available_mps.get_mp(mp_id,
                                                             self.r_server)
            self.mps[mp_id] = mp_class
            self.mp_labels[mp_class["label"]] = mp_id
            self.mp_to_label[mp_id] = mp_class["label"]
            paths = list()
            for path in mp_class["path"]:
                item = path[0]
                paths.append(item)
            self.top_levels[mp_id] = paths

        #print json.dumps(mp_class, indent=2)


    def assign_to_human_genes(self):

        logging.info("Assigning %i entries to human genes "%(len(self.mouse_genes)))
        '''
        for any mouse gene...
        '''
        for id, obj in self.mouse_genes.items():
            '''
            retrieve the human orthologs...
            '''
            for ortholog in obj["human_orthologs"]:
                human_gene_symbol = ortholog["gene_symbol"]
                #logging.info("Assign %i phenotype categories to from mouse %s to human %s"%(len(obj["phenotypes"].values()), obj["gene_symbol"], human_gene_symbol))
                '''
                assign all the phenotypes for this specific gene
                all phenotypes are classified per category
                '''
                self.human_genes[human_gene_symbol]["mouse_orthologs"].append({ "mouse_gene_id" : obj["gene_id"],
                                                                          "mouse_gene_symbol" : obj["gene_symbol"],
                                                                          "phenotypes" : obj["phenotypes"].values()})

                #print json.dumps(self.human_genes[gene_symbol]["mouse_orthologs"], indent=2)


    def write_all_to_file(self, filename=None):

        with open(filename, "wb") as fh:
            for k,v in self.human_genes.items():
                raw = json.dumps({ k: v })
                fh.write("%s\n"%(raw))

    def get_genotype_phenotype(self):
        logging.info("get_genotype_phenotype")
        try:
            logging.info("get %s"% Config.GENOTYPE_PHENOTYPE_MGI_REPORT_ORTHOLOGY)
            req = urllib2.Request(Config.GENOTYPE_PHENOTYPE_MGI_REPORT_ORTHOLOGY)
            response = urllib2.urlopen(req)
            lines = response.readlines()
            logging.info("get %i lines" % len(lines))
            for line in tqdm(
                    lines,
                    desc='Reading mouse phenotypes for human orthologs',
                    unit='line',
                    total=len(lines),
                    unit_scale=True,
                    leave=False,
                    file=self.tqdm_out):
                logging.debug(line)
                array = line.rstrip().split("\t")
                if len(array) == 7:
                    (human_gene_symbol, a, b, c, mouse_gene_symbol, mouse_gene_id, phenotypes_raw) = array
                    '''
                    There are phenotypes for this gene
                    '''
                    if len(phenotypes_raw) > 0:
                        logging.debug("get phenotypes for %s" % (human_gene_symbol))
                        #ensembl_gene_id = self._get_human_gene_ensembl_id(human_gene_symbol)
                        #if ensembl_gene_id is not None:
                        #    print ensembl_gene_id
                        mouse_gene_id = mouse_gene_id.strip()
                        mouse_gene_symbol = mouse_gene_symbol.strip()
                        if mouse_gene_id not in self.mouse_genes:
                            self.mouse_genes[mouse_gene_id] = {"gene_id": mouse_gene_id, "gene_symbol": mouse_gene_symbol,
                                                         "phenotypes": dict(), "human_orthologs": list(), "phenotypes_summary" : list(phenotypes_raw.strip().split("\s+"))}
                        self.mouse_genes[mouse_gene_id]["human_orthologs"].append(
                            {"gene_symbol": human_gene_symbol, "gene_id": None})

                        if human_gene_symbol not in self.human_genes:
                            self.human_genes[human_gene_symbol] = {"gene_symbol": human_gene_symbol, "ensembl_gene_id": None,
                                                             "gene_id": None,
                                                             "mouse_orthologs": list()}
            logging.info("Retrieved %i mouse genes"%(len(self.mouse_genes.keys())))
            logging.info("get %s" % (Config.GENOTYPE_PHENOTYPE_MGI_REPORT_PHENOTYPES))
            req = urllib2.Request(Config.GENOTYPE_PHENOTYPE_MGI_REPORT_PHENOTYPES)
            response = urllib2.urlopen(req)
            lines = response.readlines()
            logging.info("get %i lines" % len(lines))
            count_symbols = set()
            count_accepted_symbols = set()
            for line in tqdm(
                    lines,
                    desc='Reading mouse KO phenotypes',
                    unit='line',
                    total=len(lines),
                    unit_scale=True,
                    leave=False,
                    file=self.tqdm_out):
                logging.debug(line)
                # Allelic Composition	Allele Symbol(s)	Genetic Background	Mammalian Phenotype ID	PubMed ID	MGI Marker Accession ID
                array = line.rstrip().split("\t")
                if len(array) == 6:
                    (allelic_composition, allele_symbol, genetic_background, mp_id, pmid, mouse_gene_ids) = array
                    # check for double-mutant but exclude duplicates
                    for mouse_gene_id in set(mouse_gene_ids.split(",")):
                        # exclude heritable phenotypic marker like http://www.informatics.jax.org/marker/MGI:97446
                        count_symbols.add(mouse_gene_id)
                        if mouse_gene_id in self.mouse_genes:
                            count_accepted_symbols.add(mouse_gene_id)
                            logging.info('get class for %s'% mp_id)
                            mp_class = self.mps[mp_id.replace(":", "_")]
                            mp_label = mp_class["label"]

                            for k, v in PHENOTYPE_CATEGORIES.items():
                                if k not in self.mouse_genes[mouse_gene_id]["phenotypes"]:
                                    self.mouse_genes[mouse_gene_id]["phenotypes"][k] = \
                                        {
                                            "category_mp_identifier": k,
                                            "category_mp_label": v,
                                            "genotype_phenotype": []
                                        }

                            # it's possible that there are multiple paths to the same root.
                            mp_category_ids = set(map(lambda x: x[0], mp_class["path_codes"]))
                            for i, category_id in enumerate(mp_category_ids):
                                mp_category_id = category_id.replace("_", ":")
                                self.mouse_genes[mouse_gene_id]["phenotypes"][mp_category_id]["genotype_phenotype"].append(
                                    {
                                        "subject_allelic_composition": allelic_composition,
                                        "subject_background": genetic_background,
                                        "pmid" : pmid,
                                        "mp_identifier": mp_id,
                                        "mp_label": mp_label
                                    })
                else:
                    logging.info("could not process %i %s"%(len(array), line))
            logging.info("Count symbols %i / %i with phenotypes" %(len(count_accepted_symbols), len(count_symbols)))

        except Exception:
            print(traceback.format_exc())
            # or
            print(sys.exc_info()[0])
        except KeyError:
            logging.error('Error with MP key')
            print(traceback.format_exc())
            # or
            print(sys.exc_info()[0])
        except urllib2.HTTPError, e:
            logging.error('HTTPError = ' + str(e.code))
        except urllib2.URLError, e:
            logging.error('URLError = ' + str(e.reason))
        except httplib.HTTPException, e:
            logging.error('HTTPException')
