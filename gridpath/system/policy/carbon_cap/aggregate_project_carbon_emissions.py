# Copyright 2016-2023 Blue Marble Analytics LLC.
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
Aggregate carbon emissions from the project-timepoint level to
the carbon cap zone - period level.
"""

from pyomo.environ import Expression, value

from gridpath.auxiliary.dynamic_components import carbon_cap_balance_emission_components
from gridpath.common_functions import create_results_df
from gridpath.system.policy.carbon_cap import CARBON_CAP_ZONE_PRD_DF


def add_model_components(
    m,
    d,
    scenario_directory,
    weather_iteration,
    hydro_iteration,
    availability_iteration,
    subproblem,
    stage,
):
    """

    :param m:
    :param d:
    :return:
    """

    def total_carbon_emissions_rule(mod, z, p):
        """
        Calculate total emissions from all carbonaceous projects in carbon
        cap zone
        :param mod:
        :param z:
        :param p:
        :return:
        """
        return sum(
            mod.Project_Carbon_Emissions[g, tmp]
            * mod.hrs_in_tmp[tmp]
            * mod.tmp_weight[tmp]
            for (g, tmp) in mod.CRBN_PRJ_OPR_TMPS
            if g in mod.CRBN_PRJS_BY_CARBON_CAP_ZONE[z] and tmp in mod.TMPS_IN_PRD[p]
        )

    m.Total_Carbon_Cap_Project_Emissions = Expression(
        m.CARBON_CAP_ZONE_PERIODS_WITH_CARBON_CAP, rule=total_carbon_emissions_rule
    )

    record_dynamic_components(dynamic_components=d)


def record_dynamic_components(dynamic_components):
    """
    :param dynamic_components:

    This method adds project emissions to carbon balance
    """

    getattr(dynamic_components, carbon_cap_balance_emission_components).append(
        "Total_Carbon_Cap_Project_Emissions"
    )


def export_results(
    scenario_directory,
    weather_iteration,
    hydro_iteration,
    availability_iteration,
    subproblem,
    stage,
    m,
    d,
):
    """

    :param scenario_directory:
    :param subproblem:
    :param stage:
    :param m:
    :param d:
    :return:
    """

    results_columns = [
        "project_emissions",
    ]
    data = [
        [
            z,
            p,
            value(m.Total_Carbon_Cap_Project_Emissions[z, p]),
        ]
        for (z, p) in m.CARBON_CAP_ZONE_PERIODS_WITH_CARBON_CAP
    ]
    results_df = create_results_df(
        index_columns=["carbon_cap_zone", "period"],
        results_columns=results_columns,
        data=data,
    )

    for c in results_columns:
        getattr(d, CARBON_CAP_ZONE_PRD_DF)[c] = None
    getattr(d, CARBON_CAP_ZONE_PRD_DF).update(results_df)
