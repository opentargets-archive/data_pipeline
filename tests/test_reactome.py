import unittest
import os, tempfile
from mrtarget.modules.Reactome import ReactomeDataDownloader


class ReactomeTestCase(unittest.TestCase):

    def setUp(self):
        self.file_pathway = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.file_pathway_relations = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.file_wrong_tab = tempfile.NamedTemporaryFile(mode='w', delete=False)

        with self.file_pathway as f:
            f.write("R-HSA-422475\tAxon guidance\tHomo sapiens\n")
            f.write("R-HSA-193634\tAxonal growth inhibition (RHOA activation)\tHomo sapiens\n")
            f.write("R-HSA-209563\tAxonal growth stimulation\tHomo sapiens\n")

        with self.file_pathway_relations as f:
            f.write("R-HSA-422475\tR-HSA-209563\n")
            f.write("R-HSA-193634\tR-HSA-422475\n")

        with self.file_wrong_tab as f:
            f.write("R-HSA-422475\tAxon guidance\tHomo sapiens\textra_field\n")
            f.write("R-HSA-193634\tAxonal growth inhibition (RHOA activation)\tHomo sapiens\textra_field\n")
            f.write("R-HSA-209563\tAxonal growth stimulation\tHomo sapiens\textra_field\n")

    def test_get_pathway_data_succeed(self):
        downloader = ReactomeDataDownloader(self.file_pathway.name, self.file_pathway_relations.name)
        num_lines_file = sum(1 for line in open(self.file_pathway.name))
        num_entries = sum(1 for _ in downloader.get_pathway_data())

        self.assertEqual(num_entries, num_lines_file)

    def test_get_pathway_data_failure(self):
        downloader = ReactomeDataDownloader(self.file_wrong_tab.name, self.file_pathway_relations.name)
        with self.assertRaises(ValueError):
            list(downloader.get_pathway_data())

    def test_get_pathway_relations_data_succeed(self):
        downloader = ReactomeDataDownloader(self.file_pathway.name, self.file_pathway_relations.name)
        # load the valid_pathway_ids
        load_pathways_ids = sum(1 for _ in downloader.get_pathway_data())
        num_lines_file = sum(1 for line in open(self.file_pathway_relations.name))
        num_entries = sum(1 for _ in downloader.get_pathway_relations())

        self.assertEqual(num_entries, num_lines_file)

    def test_get_pathway_relations_data_failure(self):
        downloader = ReactomeDataDownloader(self.file_pathway.name, self.file_wrong_tab.name)
        with self.assertRaises(ValueError):
            list(downloader.get_pathway_relations())

    def tearDown(self):
        os.remove(self.file_pathway.name)
        os.remove(self.file_pathway_relations.name)
        os.remove(self.file_wrong_tab.name)