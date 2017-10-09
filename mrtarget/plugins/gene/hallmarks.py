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

class Hallmarks(IPlugin):

    def __init__(self):
        super(Hallmarks, self).__init__()
        self.loader = None
        self.r_server = None
        self.gene_hallmarks = dict()
        self.tqdm_out = None

    def print_name(self):
        logging.info("Hallmarks of cancer gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):

        self.loader = loader
        self.r_server = r_server
        self.tqdm_out = tqdm_out



        for gene_id, gene in tqdm(genes.iterate(),
                                  desc='Adding phenotype data from MGI',
                                  unit=' gene',
                                  file=self.tqdm_out):
            ''' extend gene with related mouse phenotype data '''
            if gene.approved_symbol in self.human_genes:
                    logging.info("Adding phenotype data from MGI for gene %s" % (gene.approved_symbol))
                    gene.mouse_phenotypes = self.human_genes[gene.approved_symbol]["mouse_orthologs"]


    def _get_mp_classes(self):
        logging.info("_get_mp_classes")
        lookup_data_types = (LookUpDataType.MP,)
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

    def _get_human_gene_ensembl_id(self, gene_symbol):
        result = None
        if gene_symbol not in self.human_genes and gene_symbol not in self.not_found_genes:
            logging.info("Getting Human ENSEMBL GENE ID for %s"%(gene_symbol))
            url = "https://rest.ensembl.org/lookup/symbol/homo_sapiens/%s?content-type=application/json;expand=0"%(gene_symbol)
            r = requests.get(url)
            results = r.json()
            if 'error' in results:
                logging.error(results['error'])
                self.not_found_genes[gene_symbol] = results['error']
            else:
                gene_id = results["id"]
                self.human_genes[gene_symbol] = { "gene_symbol" : gene_symbol, "ensembl_gene_id" : gene_id, "gene_id": "http://identifiers.org/ensembl/" + gene_id, "mouse_orthologs" : list() }
                self.human_ensembl_gene_ids[gene_id] = gene_symbol
                result = self.human_genes[gene_symbol]
        else:
            result = self.human_genes[gene_symbol]
        return result


    def assign_to_human_genes(self):

        logging.debug("Assigning %i entries to human genes "%(len(self.mouse_genes)))
        '''
        for any mouse gene...
        '''
        for id, obj in self.mouse_genes.items():
            '''
            retrieve the human orthologs...
            '''
            for ortholog in obj["human_orthologs"]:
                gene_symbol = ortholog["gene_symbol"]
                logging.debug("Assign mouse orthologs to human gene %s"%(gene_symbol))
                '''
                assign all the phenotypes for this specific gene
                all phenotypes are classified per category
                '''
                self.human_genes[gene_symbol]["mouse_orthologs"].append({ "mouse_gene_id" : obj["gene_id"],
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
                        #ensembl_gene_id = self._get_human_gene_ensembl_id(human_gene_symbol)
                        #if ensembl_gene_id is not None:
                        #    print ensembl_gene_id
                        mouse_gene_id = mouse_gene_id.strip()
                        mouse_gene_symbol = mouse_gene_symbol.strip()
                        if mouse_gene_id not in self.mouse_genes:
                            self.mouse_genes[mouse_gene_id] = {"gene_id": mouse_gene_id, "gene_symbol": mouse_gene_symbol,
                                                         "phenotypes": dict(), "human_orthologs": [], "phenotypes_summary" : list(phenotypes_raw.strip().split("\s+"))}
                        self.mouse_genes[mouse_gene_id]["human_orthologs"].append(
                            {"gene_symbol": human_gene_symbol, "gene_id": None})

                        if human_gene_symbol not in self.human_genes:
                            self.human_genes[human_gene_symbol] = {"gene_symbol": human_gene_symbol, "ensembl_gene_id": None,
                                                             "gene_id": None,
                                                             "mouse_orthologs": list()}

            logging.info("get %s" % Config.GENOTYPE_PHENOTYPE_MGI_REPORT_PHENOTYPES)
            req = urllib2.Request(Config.GENOTYPE_PHENOTYPE_MGI_REPORT_PHENOTYPES)
            response = urllib2.urlopen(req)
            lines = response.readlines()
            logging.info("get %i lines" % len(lines))
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
                        if mouse_gene_id in self.mouse_genes:
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