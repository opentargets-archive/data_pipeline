import unittest




class ImportTestCase(unittest.TestCase):
    def test_dummy(self):
        import mrtarget
        import mrtarget.cfg
        import mrtarget.CommandLine

        import mrtarget.common
        import mrtarget.common.chembl_lookup
        import mrtarget.common.connection
        import mrtarget.common.DataStructure
        import mrtarget.common.esutil
        import mrtarget.common.EvidenceString
        import mrtarget.common.IO
        import mrtarget.common.LookupHelpers
        import mrtarget.common.LookupTables
        import mrtarget.common.safercast
        import mrtarget.common.Scoring
        import mrtarget.common.UniprotIO

        import mrtarget.modules
        import mrtarget.modules.Association
        import mrtarget.modules.DataDrivenRelation
        import mrtarget.modules.Drug
        import mrtarget.modules.ECO
        import mrtarget.modules.EFO
        import mrtarget.modules.Evidences
        import mrtarget.modules.GeneData
        import mrtarget.modules.HPA
        import mrtarget.modules.QC
        import mrtarget.modules.Reactome
        import mrtarget.modules.SearchObjects
        import mrtarget.modules.Uniprot