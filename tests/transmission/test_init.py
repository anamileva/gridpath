#!/usr/bin/env python

from collections import OrderedDict
from importlib import import_module
import os.path
import sys
import unittest

from tests.common_functions import create_abstract_model, \
    add_components_and_load_data

TEST_DATA_DIRECTORY = \
    os.path.join(os.path.dirname(__file__), "..", "test_data")

# Import prerequisite modules
PREREQUISITE_MODULE_NAMES = []
NAME_OF_MODULE_BEING_TESTED = \
    "transmission"
IMPORTED_PREREQ_MODULES = list()
for mdl in PREREQUISITE_MODULE_NAMES:
    try:
        imported_module = import_module("." + str(mdl), package='modules')
        IMPORTED_PREREQ_MODULES.append(imported_module)
    except ImportError:
        print("ERROR! Module " + str(mdl) + " not found.")
        sys.exit(1)
# Import the module we'll test
try:
    MODULE_BEING_TESTED = import_module("." + NAME_OF_MODULE_BEING_TESTED,
                                        package='modules')
except ImportError:
    print("ERROR! Couldn't import module " + NAME_OF_MODULE_BEING_TESTED +
          " to test.")


class TestTransmissionInit(unittest.TestCase):
    """

    """

    def test_add_model_components(self):
        """
        Test that there are no errors when adding model components
        :return:
        """
        create_abstract_model(prereq_modules=IMPORTED_PREREQ_MODULES,
                              module_to_test=MODULE_BEING_TESTED,
                              test_data_dir=TEST_DATA_DIRECTORY,
                              horizon="",
                              stage=""
                              )

    def test_load_model_data(self):
        """
        Test that data are loaded with no errors
        :return:
        """
        add_components_and_load_data(prereq_modules=IMPORTED_PREREQ_MODULES,
                                     module_to_test=MODULE_BEING_TESTED,
                                     test_data_dir=TEST_DATA_DIRECTORY,
                                     horizon="",
                                     stage=""
                                     )

    def test_data_loaded_correctly(self):
        """
        Test that the data loaded are as expected
        :return:
        """
        m, data = add_components_and_load_data(
            prereq_modules=IMPORTED_PREREQ_MODULES,
            module_to_test=MODULE_BEING_TESTED,
            test_data_dir=TEST_DATA_DIRECTORY,
            horizon="",
            stage=""
        )
        instance = m.create_instance(data)

        # Set: TRANSMISSION_LINES
        expected_tx_lines = sorted(["Tx1", "Tx_New"])
        actual_tx_lines = sorted([tx for tx in instance.TRANSMISSION_LINES])
        self.assertListEqual(expected_tx_lines, actual_tx_lines)

        # Param: tx_capacity_type
        expected_cap_type = OrderedDict(sorted(
            {"Tx1": "specified_transmission", "Tx_New": "new_build_transmission"
             }.items()
                                        )
                                              )
        actual_cap_type = OrderedDict(sorted(
            {tx: instance.tx_capacity_type[tx]
             for tx in instance.TRANSMISSION_LINES}.items()
                                        )
                                              )
        self.assertDictEqual(expected_cap_type, actual_cap_type)

        # Param: load_zone_from
        expected_load_zone_from = OrderedDict(sorted(
            {"Tx1": "Zone1", "Tx_New": "Zone1"
             }.items()
                                        )
                                              )
        actual_load_zone_from = OrderedDict(sorted(
            {tx: instance.load_zone_from[tx]
             for tx in instance.TRANSMISSION_LINES}.items()
                                        )
                                              )
        self.assertDictEqual(expected_load_zone_from, actual_load_zone_from)

        # Param: load_zone_to
        expected_load_zone_to = OrderedDict(sorted(
            {"Tx1": "Zone2", "Tx_New": "Zone2"
             }.items()
                                        )
                                              )
        actual_load_zone_to = OrderedDict(sorted(
            {tx: instance.load_zone_to[tx]
             for tx in instance.TRANSMISSION_LINES}.items()
                                        )
                                              )
        self.assertDictEqual(expected_load_zone_to, actual_load_zone_to)

if __name__ == "__main__":
    unittest.main()