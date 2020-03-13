#!/usr/bin/env python
# Copyright 2019 Blue Marble Analytics LLC. All rights reserved.

"""
Load system planning reserve margin prm targets data from csvs
"""

from db.utilities import system_prm

def load_system_prm_requirement(io, c, subscenario_input, data_input):
    """
    System prm dictionary
    {prm_zone: {period: prm_requirement_mw}}
    :param io:
    :param c:
    :param subscenario_input:
    :param data_input:
    :return:
    """

    for i in subscenario_input.index:
        sc_id = int(subscenario_input['prm_requirement_scenario_id'][i])
        sc_name = subscenario_input['name'][i]
        sc_description = subscenario_input['description'][i]

        data_input_subscenario = data_input.loc[(data_input['prm_requirement_scenario_id'] == sc_id)]

        zone_period_requirement = dict()
        for z in data_input_subscenario['prm_zone'].unique():
            zone_period_requirement[z] = dict()
            zone_period_requirement_by_zone = data_input_subscenario.loc[data_input_subscenario['prm_zone'] == z]
            for p in zone_period_requirement_by_zone['period'].unique():
                p = int(p)
                zone_period_requirement[z][p] = dict()
                zone_period_requirement[z][p] = float(zone_period_requirement_by_zone.loc[
                    zone_period_requirement_by_zone['period'] == p, 'prm_requirement_mw'].iloc[0])

        # Load data into GridPath database
        system_prm.prm_requirement(
            io=io, c=c,
            prm_requirement_scenario_id=sc_id,
            scenario_name=sc_name,
            scenario_description=sc_description,
            zone_period_requirement=zone_period_requirement
        )