from stupendous_cow.importer.abstracts import *
from stupendous_cow.testing import get_resource_dir, set_resource_dir
import os.path
import unittest

class NipsAbstractFileReaderTests(unittest.TestCase):
    def test_read_abstracts(self):
        abs_title_1 = 'Memory Augmented Policy Optimization for Program ' + \
                      'Synthesis and Semantic Parsing'
        abs_body_1 = 'This paper presents MAPO: a novel policy ' + \
                     'optimization formulation...'
        true_abs_1 = Abstract(1, abs_title_1, (), abs_body_1)

        abs_title_2 = 'Fast deep reinforcement learning using online ' + \
                      'adjustments from the past'
        abs_body_2 = 'We propose Ephemeral Value Adjusments (EVA): a means ' + \
                     'of allowing deep reinforcement learning agents...\n' + \
                     '  EVA shifts the value predicted by a neural ' + \
                     'network with an estimate of the value function...\n' + \
                     '  We show that EVA is performant on a demonstration ' + \
                     'task and Atari games.'
        true_abs_2 = Abstract(6, abs_title_2, (), abs_body_2)

        abs_title_3 = 'Diversity-Driven Exploration Strategy for ' + \
                      'Deep Reinforcement Learning'
        abs_body_3 = 'Efficient exploration remains a challenging ' + \
                     'research problem in reinforcement learning...'
        true_abs_3 = Abstract(13, abs_title_3, (), abs_body_3)
        
        filename = os.path.join(get_resource_dir(), 'test_abstracts.txt')
        reader = NipsAbstractFileReader(filename)
        abstracts = [ a for a in reader ]

        self.assertEqual(3, len(abstracts))
        self._verify_abstract(true_abs_1, abstracts[0])
        self._verify_abstract(true_abs_2, abstracts[1])
        self._verify_abstract(true_abs_3, abstracts[2])

    def _verify_abstract(self, truth, abstract):
        self.assertEqual(truth.start, abstract.start)
        self.assertEqual(truth.title, abstract.title)
        self.assertEqual(truth.authors, abstract.authors)
        self.assertEqual(truth.body, abstract.body)
        

if __name__ == '__main__':
    set_resource_dir('importer')
    unittest.main()
