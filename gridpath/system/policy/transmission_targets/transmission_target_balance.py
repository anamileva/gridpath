# Copyright 2022 (c) Crown Copyright, GC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Simplest implementation with a MWh target by period.
"""


import csv
import os.path
import pandas as pd

from pyomo.environ import Var, Constraint, NonNegativeReals, Expression, value

from db.common_functions import spin_on_database_lock
from gridpath.auxiliary.db_interface import setup_results_import
from gridpath.common_functions import create_results_df
from gridpath.system.policy.transmission_targets import TX_TARGETS_DF


def add_model_components(m, d, scenario_directory, subproblem, stage):
    """

    :param m:
    :param d:
    :return:
    """

    m.Transmission_Target_Shortage_Pos_Dir_MWh = Var(
        m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET,
        within=NonNegativeReals,
    )

    m.Transmission_Target_Shortage_Neg_Dir_MWh = Var(
        m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET,
        within=NonNegativeReals,
    )

    def violation_pos_dir_expression_rule(mod, z, bt, hz):
        if mod.transmission_target_allow_violation[z]:
            return mod.Transmission_Target_Shortage_Pos_Dir_MWh[z, bt, hz]
        else:
            return 0

    m.Transmission_Target_Shortage_Pos_Dir_MWh_Expression = Expression(
        m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET,
        rule=violation_pos_dir_expression_rule,
    )

    def violation_neg_dir_expression_rule(mod, z, bt, hz):
        if mod.transmission_target_allow_violation[z]:
            return mod.Transmission_Target_Shortage_Neg_Dir_MWh[z, bt, hz]
        else:
            return 0

    m.Transmission_Target_Shortage_Neg_Dir_MWh_Expression = Expression(
        m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET,
        rule=violation_neg_dir_expression_rule,
    )

    def transmission_target_pos_dir_rule(mod, z, bt, hz):
        """
        Total delivered transmission-target-eligible energy in positive
        direction must exceed target
        :param mod:
        :param z:
        :param p:
        :return:
        """
        if mod.transmission_target_pos_dir_min_mwh[z, bt, hz] == 0:
            return Constraint.Skip
        else:
            return (
                mod.Total_Transmission_Target_Energy_Pos_Dir_MWh[z, bt, hz]
                + mod.Transmission_Target_Shortage_Pos_Dir_MWh_Expression[z, bt, hz]
                >= mod.transmission_target_pos_dir_min_mwh[z, bt, hz]
            )

    m.Transmission_Target_Pos_Dir_Constraint = Constraint(
        m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET,
        rule=transmission_target_pos_dir_rule,
    )

    def transmission_target_neg_dir_rule(mod, z, bt, hz):
        """
        Total delivered transmission-target-eligible energy in negative
        direction must exceed target
        :param mod:
        :param z:
        :param p:
        :return:
        """
        if mod.transmission_target_neg_dir_min_mwh[z, bt, hz] == 0:
            return Constraint.Skip
        else:
            return (
                mod.Total_Transmission_Target_Energy_Neg_Dir_MWh[z, bt, hz]
                + mod.Transmission_Target_Shortage_Neg_Dir_MWh_Expression[z, bt, hz]
                >= mod.transmission_target_neg_dir_min_mwh[z, bt, hz]
            )

    m.Transmission_Target_Neg_Dir_Constraint = Constraint(
        m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET,
        rule=transmission_target_neg_dir_rule,
    )


def export_results(scenario_directory, subproblem, stage, m, d):
    """

    :param scenario_directory:
    :param subproblem:
    :param stage:
    :param m:
    :param d:
    :return:
    """

    results_columns = [
        "fraction_of_transmission_target_pos_dir_min_met",
        "transmission_target_shortage_pos_dir_min_mwh",
        "fraction_of_transmission_target_neg_dir_met",
        "transmission_target_shortage_neg_dir_min_mwh",
    ]
    data = [
        [
            z,
            bt,
            hz,
            1
            if float(m.transmission_target_pos_dir_min_mwh[z, bt, hz]) == 0
            else value(m.Total_Transmission_Target_Energy_Pos_Dir_MWh[z, bt, hz])
            / float(m.transmission_target_pos_dir_min_mwh[z, bt, hz]),
            value(m.Transmission_Target_Shortage_Pos_Dir_MWh_Expression[z, bt, hz]),
            1
            if float(m.transmission_target_neg_dir_min_mwh[z, bt, hz]) == 0
            else value(m.Total_Transmission_Target_Energy_Neg_Dir_MWh[z, bt, hz])
            / float(m.transmission_target_neg_dir_min_mwh[z, bt, hz]),
            value(m.Transmission_Target_Shortage_Neg_Dir_MWh_Expression[z, bt, hz]),
        ]
        for (
            z,
            bt,
            hz,
        ) in m.TRANSMISSION_TARGET_ZONE_BLN_TYPE_HRZS_WITH_TRANSMISSION_TARGET
    ]
    results_df = create_results_df(
        index_columns=["transmission_target_zone", "balancing_type", "horizon"],
        results_columns=results_columns,
        data=data,
    )

    for c in results_columns:
        getattr(d, TX_TARGETS_DF)[c] = None
    getattr(d, TX_TARGETS_DF).update(results_df)
