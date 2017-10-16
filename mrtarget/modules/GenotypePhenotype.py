# import json
# import logging
# import logging.config
# import time
# import urllib2
#
# import requests
#
# from mrtarget.Settings import Config
# from mrtarget.Settings import file_or_resource
# from mrtarget.common import Actions
# from mrtarget.common import TqdmToLogger
# from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
#
#
# logging.config.fileConfig(file_or_resource('logging.ini'),
#                               disable_existing_loggers=False)
#
#
# PHENOTYPE_CATEGORIES = {
#     "MP:0005386" : "behavior/neurological phenotype",
#     "MP:0005375" : "adipose tissue phenotype",
#     "MP:0005385" : "cardiovascular system phenotype",
#     "MP:0005384" : "cellular phenotype",
#     "MP:0005382" : "craniofacial phenotype",
#     "MP:0005381" : "digestive/alimentary phenotype",
#     "MP:0005380" : "embryo phenotype",
#     "MP:0005379" : "endocrine/exocrine phenotype",
#     "MP:0005378" : "growth/size/body region phenotype",
#     "MP:0005377" : "hearing/vestibular/ear phenotype",
#     "MP:0005397" : "hematopoietic system phenotype",
#     "MP:0005376" : "homeostasis/metabolism phenotype",
#     "MP:0005387" : "immune system phenotype",
#     "MP:0010771" : "integument phenotype",
#     "MP:0005371" : "limbs/digits/tail phenotype",
#     "MP:0005370" : "liver/biliary system phenotype",
#     "MP:0010768" : "mortality/aging",
#     "MP:0005369" : "muscle phenotype",
#     "MP:0002006" : "neoplasm",
#     "MP:0003631" : "nervous system phenotype",
#     "MP:0002873" : "normal phenotype",
#     "MP:0001186" : "pigmentation phenotype",
#     "MP:0005367" : "renal/urinary system phenotype",
#     "MP:0005389" : "reproductive system phenotype",
#     "MP:0005388" : "respiratory system phenotype",
#     "MP:0005390" : "skeleton phenotype",
#     "MP:0005394" : "taste/olfaction phenotype",
#     "MP:0005391" : "vision/eye phenotype"
# }
#
# class MousePhenotypeActions(Actions):
#     PROCESS = 'process'
#     UPLOAD = 'upload'
#
# class MouseminePhenotypeETL(object):
#
#     def __init__(self, loader=None, r_server=None):
#         self.loader = loader
#         self.r_server = r_server
#         self._logger = logging.getLogger(__name__)
#         self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)
#         self.mouse_genes = dict()
#         self.ancestors = dict()
#         self.lookup_data = None
#         self.mps = dict()
#         self.mp_labels = dict()
#         self.mp_to_label = dict()
#         self.top_levels = dict()
#         self.homologs = dict()
#         self.human_genes = dict()
#         self.not_found_genes = dict()
#         self.human_ensembl_gene_ids = dict()
#
#     def process_all(self):
#
#         # process_all
#         self._get_mp_classes()
#
#         self.get_genotype_phenotype()
#
#         self.assign_to_human_genes()
#
#         self.write_all_to_file(filename=Config.GENOTYPE_PHENOTYPE_OUTPUT)
#
#
#     def _get_mp_classes(self):
#
#         lookup_data_types = (LookUpDataType.MP,)
#         self._logger.debug(LookUpDataType.MP)
#         self.lookup_data = LookUpDataRetriever(self.loader.es,
#                                                self.r_server,
#                                                data_types=lookup_data_types,
#                                                autoload=True
#                                                ).lookup
#         mp_class = None
#         for mp_id in self.lookup_data.available_mps.get_available_mp_ids():
#             self._logger.debug(mp_id)
#             mp_class = self.lookup_data.available_mps.get_mp(mp_id,
#                                                              self.r_server)
#             self.mps[mp_id] = mp_class
#             self.mp_labels[mp_class["label"]] = mp_id
#             self.mp_to_label[mp_id] = mp_class["label"]
#             paths = list()
#             for path in mp_class["path"]:
#                 item = path[0]
#                 paths.append(item)
#             self.top_levels[mp_id] = paths
#
#         #print json.dumps(mp_class, indent=2)
#
#     def _get_human_gene_ensembl_id(self, gene_symbol):
#         result = None
#         if gene_symbol not in self.human_genes and gene_symbol not in self.not_found_genes:
#             self._logger.info("Getting Human ENSEMBL GENE ID for %s"%(gene_symbol))
#             url = "https://rest.ensembl.org/lookup/symbol/homo_sapiens/%s?content-type=application/json;expand=0"%(gene_symbol)
#             r = requests.get(url)
#             results = r.json()
#             if 'error' in results:
#                 self._logger.error(results['error'])
#                 self.not_found_genes[gene_symbol] = results['error']
#             else:
#                 gene_id = results["id"]
#                 self.human_genes[gene_symbol] = { "gene_symbol" : gene_symbol, "ensembl_gene_id" : gene_id, "gene_id": "http://identifiers.org/ensembl/" + gene_id, "mouse_orthologs" : list() }
#                 self.human_ensembl_gene_ids[gene_id] = gene_symbol
#                 result = self.human_genes[gene_symbol]
#         else:
#             result = self.human_genes[gene_symbol]
#         return result
#
#
#     def assign_to_human_genes(self):
#
#         self._logger.debug("Assigning %i entries to human genes "%(len(self.mouse_genes)))
#         '''
#         for any mouse gene...
#         '''
#         for id, obj in self.mouse_genes.items():
#             '''
#             retrieve the human orthologs...
#             '''
#             for ortholog in obj["human_orthologs"]:
#                 gene_symbol = ortholog["gene_symbol"]
#                 self._logger.debug("Assign mouse orthologs to human gene %s"%(gene_symbol))
#                 '''
#                 assign all the phenotypes for this specific gene
#                 all phenotypes are classified per category
#                 '''
#                 self.human_genes[gene_symbol]["mouse_orthologs"].append({ "mouse_gene_id" : obj["gene_id"],
#                                                                           "mouse_gene_symbol" : obj["gene_symbol"],
#                                                                           "phenotypes" : obj["phenotypes"].values()})
#
#                 #print json.dumps(self.human_genes[gene_symbol]["mouse_orthologs"], indent=2)
#
#
#     def write_all_to_file(self, filename=None):
#
#         with open(filename, "wb") as fh:
#             for k,v in self.human_genes.items():
#                 raw = json.dumps({ k: v })
#                 fh.write("%s\n"%(raw))
#
#     def get_genotype_phenotype(self):
#
#         req = urllib2.Request(Config.GENOTYPE_PHENOTYPE_MGI_REPORT_ORTHOLOGY)
#         response = urllib2.urlopen(req)
#         lines = response.readlines()
#         for line in lines:
#             self._logger.debug(line)
#             array = line.rstrip().split("\t")
#             if len(array) == 7:
#                 (human_gene_symbol, a, b, c, mouse_gene_symbol, mouse_gene_id, phenotypes_raw) = array
#                 '''
#                 There are phenotypes for this gene
#                 '''
#                 if len(phenotypes_raw) > 0:
#                     #ensembl_gene_id = self._get_human_gene_ensembl_id(human_gene_symbol)
#                     #if ensembl_gene_id is not None:
#                     #    print ensembl_gene_id
#                     mouse_gene_id = mouse_gene_id.strip()
#                     mouse_gene_symbol = mouse_gene_symbol.strip()
#                     if mouse_gene_id not in self.mouse_genes:
#                         self.mouse_genes[mouse_gene_id] = {"gene_id": mouse_gene_id, "gene_symbol": mouse_gene_symbol,
#                                                      "phenotypes": dict(), "human_orthologs": [], "phenotypes_summary" : list(phenotypes_raw.strip().split("\s+"))}
#                     self.mouse_genes[mouse_gene_id]["human_orthologs"].append(
#                         {"gene_symbol": human_gene_symbol, "gene_id": None})
#
#                     if human_gene_symbol not in self.human_genes:
#                         self.human_genes[human_gene_symbol] = {"gene_symbol": human_gene_symbol, "ensembl_gene_id": None,
#                                                          "gene_id": None,
#                                                          "mouse_orthologs": list()}
#
#         req = urllib2.Request(Config.GENOTYPE_PHENOTYPE_MGI_REPORT_PHENOTYPES)
#         response = urllib2.urlopen(req)
#         lines = response.readlines()
#         for line in lines:
#             self._logger.debug(line)
#             # Allelic Composition	Allele Symbol(s)	Genetic Background	Mammalian Phenotype ID	PubMed ID	MGI Marker Accession ID
#             array = line.rstrip().split("\t")
#             if len(array) == 6:
#                 (allelic_composition, allele_symbol, genetic_background, mp_id, pmid, mouse_gene_ids) = array
#                 # check for double-mutant but exclude duplicates
#                 for mouse_gene_id in set(mouse_gene_ids.split(",")):
#                     # exclude heritable phenotypic marker like http://www.informatics.jax.org/marker/MGI:97446
#                     if mouse_gene_id in self.mouse_genes:
#                         mp_class = self.mps[mp_id.replace(":", "_")]
#                         mp_label = mp_class["label"]
#
#                         for k, v in PHENOTYPE_CATEGORIES.items():
#                             if k not in self.mouse_genes[mouse_gene_id]["phenotypes"]:
#                                 self.mouse_genes[mouse_gene_id]["phenotypes"][k] = \
#                                     {
#                                         "category_mp_identifier": k,
#                                         "category_mp_label": v,
#                                         "genotype_phenotype": []
#                                     }
#
#                         # it's possible that there are multiple paths to the same root.
#                         mp_category_ids = set(map(lambda x: x[0], mp_class["path_codes"]))
#                         for i, category_id in enumerate(mp_category_ids):
#                             mp_category_id = category_id.replace("_", ":")
#                             self.mouse_genes[mouse_gene_id]["phenotypes"][mp_category_id]["genotype_phenotype"].append(
#                                 {
#                                     "subject_allelic_composition": allelic_composition,
#                                     "subject_background": genetic_background,
#                                     "pmid" : pmid,
#                                     "mp_identifier": mp_id,
#                                     "mp_label": mp_label
#                                 })
