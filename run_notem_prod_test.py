"""
Temporary file for testing DVec - should be moved over to proper tests when there is time!
"""

# BACKLOG: Move run script for DVector into pytest
#  labels: core, testing

# Third party imports
import pandas as pd

import tqdm

# local imports
import normits_demand as nd

from normits_demand.models import notem_production_model as notem
from normits_demand.models import notem_attraction_model as notem_attr

# ## GLOBAL VARIABLES ## #
# PRODUCTIONS
POPULATION_PATH = {
    2018: r"I:\NorMITs Land Use\base_land_use\iter3d\outputs\land_use_output_tfn_msoa1.csv",
    # 2033: r"I:\NorMITs Land Use\future_land_use\iter3b\scenarios\NTEM\land_use_2033_pop.csv",
    # 2040: r"I:\NorMITs Land Use\future_land_use\iter3b\scenarios\NTEM\land_use_2040_pop.csv",
    # 2050: r"I:\NorMITs Land Use\future_land_use\iter3b\scenarios\NTEM\land_use_2050_pop.csv",
}
TRIP_RATES_PATH = r"I:\NorMITs Demand\import\NoTEM\HB_Productions\hb_trip_rates_v1.9.csv"
MODE_TIME_SPLITS_PATH = r"I:\NorMITs Demand\import\NoTEM\HB_Productions\hb_mode_time_split_v1.9.csv"

# p_export_path = r"C:\Data\Nirmal_Atkins\ss"
p_export_path = "E:/Productions"

# ATTRACTIONS
attraction_path = {
    2018: r"I:\NorMITs Land Use\base_land_use\iter3b\outputs\land_use_2018_emp.csv"
}
attr_trip_rates_path = r"I:\NorMITs Demand\import\NoTEM\Attractions\sample_attraction_trip_rate.csv"
attr_mode_splits_path = r"I:\NorMITs Demand\import\NoTEM\Attractions\attraction_mode_split_new_infill.csv"

# pure_demand_production = r"C:\Data\Nirmal_Atkins\hb_msoa_pure_demand_2018_dvec.pkl"
pure_demand_production = r"E:\Productions\hb_msoa_notem_segmented_2018_dvec.pkl"

# attr_export_path = r"C:\Data\Nirmal_Atkins\Attractions"
attr_export_path = "E:/Attractions"


def main():
    hb_prod = notem.HBProductionModel(
        POPULATION_PATH,
        TRIP_RATES_PATH,
        MODE_TIME_SPLITS_PATH,
        p_export_path
    )

    hb_prod.run(
        export_pure_demand=False,
        export_fully_segmented=False,
        export_notem_segmentation=False,
        export_reports=True,
        verbose=True,
    )


def main_attr():
    hb_attr = notem_attr.HBAttractionModel(
        land_use_paths=attraction_path,
        notem_segmented_productions=pure_demand_production,
        attraction_trip_rates_path=attr_trip_rates_path,
        mode_splits_path=attr_mode_splits_path,
        export_path=attr_export_path,

    )

    hb_attr.run(
        export_pure_attractions=True,
        export_fully_segmented=True,
        export_reports=True,
        verbose=True,
    )


if __name__ == '__main__':
    # main()
    main_attr()
