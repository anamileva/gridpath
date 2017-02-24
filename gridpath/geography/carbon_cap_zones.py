#!/usr/bin/env python
# Copyright 2017 Blue Marble Analytics LLC. All rights reserved.

"""
Zones where carbon cap enforced; these can be different from the load
zones and other balancing areas.
"""

import os.path
from pyomo.environ import Set


def add_model_components(m, d):
    """

    :param m:
    :param d:
    :return:
    """

    m.CARBON_CAP_ZONES = Set()


def load_model_data(m, d, data_portal, scenario_directory, horizon, stage):

    data_portal.load(filename=os.path.join(scenario_directory, horizon, stage,
                                           "inputs", "carbon_cap_zones.tab"),
                     set=m.CARBON_CAP_ZONES
                     )
