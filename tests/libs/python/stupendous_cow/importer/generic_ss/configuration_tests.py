from stupendous_cow.importers.generic_ss.configuration import *
import StringIO
import unittest

class ConfigurationParserTests:
    def _create_stream(self, config):
        return StringIO.cStringIO(yaml.dump(config), default_flow_style = False)

    def _verify_config(self, true_config, config):
        self.assertEqual(true_config['Venue'], config.venue)
        self.assertEqual(true_config['Year'], config.year)
        true_dgs = sorted([ x for x in true_config \
                              if x.startswith('DocumentGroup_') ])
        for (true_dg_name, dg) in zip(true_dgs, config.document_groups):
            true_dg = true_config[true_dg_name]
            self._verify_document_group(true_dg_name, true_dg, dg)

    def _verify_document_group(self, true_name, true_dg, dg):
        pass

if __name__ == '__main__':
    unittest.main()
