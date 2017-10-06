import unittest

from mrtarget.common.Scoring import HarmonicSumScorer
from mrtarget.modules.EvidenceString import DataNormaliser, Evidence


class HarmonicSumTestCase(unittest.TestCase):
    def test_harmonic_sum(self):

        '''test sorting and first value being not scaled'''
        data = range(2)
        buffer = 100
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data),len(data))
        self.assertEqual(harmonic_sum_scorer.score(), 1.)
        self.assertEqual(harmonic_sum_scorer.score(scale_factor=2.), 1)
        self.assertEqual(harmonic_sum_scorer.score(scale_factor=3.), 1)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        '''test correct scaling'''
        data =[1]*2
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), len(data))
        self.assertEqual(harmonic_sum_scorer.score(), 1.5)
        self.assertEqual(harmonic_sum_scorer.score(scale_factor=2.), 1.2500)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        '''test correct scaling'''
        data = [1] * 100
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), len(data))
        self.assertEqual(harmonic_sum_scorer.score(), 5.187377517639621)
        self.assertEqual(harmonic_sum_scorer.score(scale_factor=2.), 1.6349839001848923)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        '''test buffer'''
        data = [1] * buffer + [0.2]*10
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), buffer)
        self.assertEqual(harmonic_sum_scorer.score(), 5.187377517639621)
        self.assertEqual(harmonic_sum_scorer.score(scale_factor=2.), 1.6349839001848923)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        data = [1] * buffer + [0.2] * 10 + [1.4] * 2
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), buffer)
        self.assertEqual(harmonic_sum_scorer.score(), 5.787377517639621)
        self.assertEqual(harmonic_sum_scorer.score(scale_factor=2.), 2.1349839001848925)
        self.assertEqual(harmonic_sum_scorer.score(cap=2), 2)

    def test_renormalize(self):
        value = DataNormaliser.renormalize(0.2,[0.,.9],[.5,1])
        self.assertEqual(value,0.6111111111111112)
        value = DataNormaliser.renormalize(2, [0., .9], [.5, 1])
        self.assertEqual(value, 1)
        value = DataNormaliser.renormalize(2, [0., .9], [.5, 1], cap=False)
        self.assertEqual(value, 1.6111111111111112)
        value = DataNormaliser.renormalize(-.2, [0., .9], [.5, 1])
        self.assertEqual(value, 0.5)
        value = DataNormaliser.renormalize(-.2, [0., .9], [.5, 1], cap=False)
        self.assertEqual(value, 0.3888888888888889)
        value = DataNormaliser.renormalize(10, [1, 100000], [0., 1])
        self.assertEqual(value, 9.000090000900009e-05)
        value = DataNormaliser.renormalize(1, [1, 100000], [0., 1])
        self.assertEqual(value, 0)
        value = DataNormaliser.renormalize(100005, [1, 100000], [0., 1])
        self.assertEqual(value, 1)
        value = DataNormaliser.renormalize(2500., [0, 5000], [0., 1])
        self.assertEqual(value, 0.5)



    def test_pvalue_transform(self):
        value = Evidence._get_score_from_pvalue_linear(1)
        self.assertEqual(value,0)
        value = Evidence._get_score_from_pvalue_linear(10)
        self.assertEqual(value, 0)
        value = Evidence._get_score_from_pvalue_linear(1e-10)
        self.assertEqual(value, 1)
        value = Evidence._get_score_from_pvalue_linear(1e-30)
        self.assertEqual(value, 1)
        value = Evidence._get_score_from_pvalue_linear(1e-5)
        self.assertEqual(value, .5)
        value = Evidence._get_score_from_pvalue_linear(1e-2, range_min=1e-2)
        self.assertEqual(value, 0.)
        value = Evidence._get_score_from_pvalue_linear(1e-10, range_min=1e-2)
        self.assertEqual(value, 1.)
        value = Evidence._get_score_from_pvalue_linear(1, range_min=1e-2)
        self.assertEqual(value, 0.)
        value = Evidence._get_score_from_pvalue_linear(1e-5, range_min=1e-2, range_max= 1e-6)
        self.assertEqual(value, .75)

    def test_sigmoind_scaling(self):
        value = HarmonicSumScorer.sigmoid_scaling(1)
        self.assertEqual(value, 1)

        value = HarmonicSumScorer.sigmoid_scaling(100)
        self.assertEqual(value, 0.542)

        value = HarmonicSumScorer.sigmoid_scaling(1000)
        self.assertEqual(value, 0)

        value = HarmonicSumScorer.sigmoid_scaling(100, precision=6)
        self.assertEqual(value, 0.541824)

        value = HarmonicSumScorer.sigmoid_scaling(100, mid_value=10)
        self.assertEqual(value, 0)





if __name__ == '__main__':
    unittest.main()
