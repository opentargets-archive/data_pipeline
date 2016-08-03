import unittest

from modules.Association import HarmonicSumScorer


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
        self.assertEqual(harmonic_sum_scorer.score(scale_index=2.), 1)
        self.assertEqual(harmonic_sum_scorer.score(scale_index=3.), 1)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        '''test correct scaling'''
        data =[1]*2
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), len(data))
        self.assertEqual(harmonic_sum_scorer.score(), 1.5)
        self.assertEqual(harmonic_sum_scorer.score(scale_index=2.), 1.2500)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        '''test correct scaling'''
        data = [1] * 100
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), len(data))
        self.assertEqual(harmonic_sum_scorer.score(), 5.187377517639621)
        self.assertEqual(harmonic_sum_scorer.score(scale_index=2.), 1.6349839001848923)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        '''test buffer'''
        data = [1] * buffer + [0.2]*10
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), buffer)
        self.assertEqual(harmonic_sum_scorer.score(), 5.187377517639621)
        self.assertEqual(harmonic_sum_scorer.score(scale_index=2.), 1.6349839001848923)
        self.assertEqual(harmonic_sum_scorer.score(cap=.5), .5)

        data = [1] * buffer + [0.2] * 10 + [1.4] * 2
        harmonic_sum_scorer = HarmonicSumScorer(buffer=buffer)
        for i in data:
            harmonic_sum_scorer.add(i)
        self.assertEqual(len(harmonic_sum_scorer.data), buffer)
        self.assertEqual(harmonic_sum_scorer.score(), 5.787377517639621)
        self.assertEqual(harmonic_sum_scorer.score(scale_index=2.), 2.1349839001848925)
        self.assertEqual(harmonic_sum_scorer.score(cap=2), 2)






if __name__ == '__main__':
    unittest.main()
