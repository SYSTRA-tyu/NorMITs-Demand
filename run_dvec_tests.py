# -*- coding: utf-8 -*-
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

from normits_demand.utils import pandas_utils as pd_utils

# GLOBAL VARIABLES
# I Drive Path locations
POPULATION_PATH = r"I:\NorMITs Land Use\base_land_use\iter3b\outputs\land_use_output_tfn_msoa1.csv"
TRIP_RATES_PATH = r"I:\Data\NTS\outputs\hb\hb_trip_rates\hb_trip_rates_normalised.csv"
MODE_TIME_SPLITS_PATH = r"I:\Data\NTS\outputs\hb\hb_time_mode_split_long.csv"

# # Nirmal C Drive locations
# POPULATION_PATH = r"C:\Data\NorMITS\land_use_output_tfn_msoa1.csv"
# TRIP_RATES_PATH = r"C:\Data\NorMITS\hb_trip_rates_normalised.csv"


def main():

    # Define wanted columns
    target_cols = {
        'land_use': ['msoa_zone_id', 'area_type', 'tfn_traveller_type', 'people'],
        'trip_rate': ['tfn_traveller_type', 'area_type', 'p', 'trip_rate']
    }

    # Read in pop and trip rates
    print("Reading in files...")
    pop = pd.read_csv(POPULATION_PATH, usecols=target_cols['land_use'])
    trip_rates = pd.read_csv(TRIP_RATES_PATH, usecols=target_cols['trip_rate'])

    # ## CONVERT POP TO DVECTOR ## #
    # Define new combo columns
    pop['segment'] = pop['tfn_traveller_type'].astype(str) + "_" + pop['area_type'].astype(str)
    pop['zone_at'] = pop['msoa_zone_id'].astype(str) + "_" + pop['area_type'].astype(str)

    # Get unique columns names
    # TODO(BT): This should come from a ModelZone object once they are implemented!
    unq_zoning = pop['msoa_zone_id'].unique()

    # Filter down to just the columns we need for this
    group_cols = ['segment', 'msoa_zone_id']
    index_cols = group_cols.copy() + ['people']
    temp_pop = pop.reindex(columns=index_cols)
    temp_pop = temp_pop.groupby(group_cols).sum().reset_index()

    # Get the pop data for each segment
    dvec_pop = dict()
    desc = "To Dvec"
    for segment in tqdm.tqdm(pop['segment'].unique(), desc=desc):
        # Get all available pop for this segment
        seg_pop = temp_pop[temp_pop['segment'] == segment].copy()

        # Filter down to just pop as values, and zoning system as the index
        seg_pop = seg_pop.reindex(columns=['msoa_zone_id', 'people'])
        seg_pop = seg_pop.set_index('msoa_zone_id')

        # Infill any missing zones as 0
        seg_pop = seg_pop.reindex(unq_zoning, fill_value=0)

        # Assign to dict for storage
        dvec_pop[segment] = seg_pop.values


def dvec_obj_main():

    # Define the zoning and segmentations we want to use
    import_drive = "I:/"
    msoa_zoning = nd.get_zoning_system(name='msoa', import_drive=import_drive)
    pop_seg = nd.get_segmentation_level(name='lu_pop', import_drive=import_drive)
    pure_demand_seg = nd.get_segmentation_level(name='pure_demand', import_drive=import_drive)

    # Define wanted columns
    target_cols = {
        'land_use': ['msoa_zone_id', 'area_type', 'tfn_traveller_type', 'people'],
        'trip_rate': ['tfn_traveller_type', 'area_type', 'p', 'trip_rate']
    }

    # Read in pop and trip rates
    print("Reading in files...")
    pop = pd.read_csv(POPULATION_PATH, usecols=target_cols['land_use'])
    trip_rates = pd.read_csv(TRIP_RATES_PATH, usecols=target_cols['trip_rate'])

    # ## CREATE THE POP DVEC ## #
    print("Creating pop DVec...")

    # Add a segment column
    naming_conversion = {
        'tfn_tt': 'tfn_traveller_type',
        'tfn_at': 'area_type',
    }
    pop['segment'] = pop_seg.create_segment_col(pop, naming_conversion)

    # Filter pop down ready for import into Dvec
    pop = pd_utils.reindex_and_groupby(
        df=pop,
        index_cols=['segment', 'msoa_zone_id', 'people'],
        value_cols=['people'],
    )

    # Instantiate
    pop_dvec = nd.DVector(
        zoning_system=msoa_zoning,
        segmentation=pop_seg,
        import_data=pop,
        zone_col="msoa_zone_id",
        segment_col="segment",
        val_col="people",
        verbose=True,
    )

    # ## CREATE THE TRIP RATES DVEC ## #
    print("Creating trip rates DVec...")

    # Add a segment column
    # Create inside DVec!!!
    naming_conversion = {
        'p': 'p',
        'tfn_tt': 'tfn_traveller_type',
        'tfn_at': 'area_type',
    }
    trip_rates['segment'] = pure_demand_seg.create_segment_col(trip_rates, naming_conversion)

    # Filter pop down ready for import into Dvec
    trip_rates = pd_utils.reindex_and_groupby(
        df=trip_rates,
        index_cols=['segment', 'trip_rate'],
        value_cols=['trip_rate'],
    )

    # Instantiate
    trip_rates_dvec = nd.DVector(
        zoning_system=None,
        segmentation=pure_demand_seg,
        import_data=trip_rates,
        zone_col=None,
        segment_col="segment",
        val_col="trip_rate",
        verbose=True,
    )

    # ## MULTIPLY TOGETHER ## #
    # TODO(BT): Need to implement this - example calling structure?
    #  Need to think about how this will work in more detail
    #  Specifically, how to generalise properly!
    pure_demand = pop_dvec * trip_rates_dvec


if __name__ == '__main__':
    # main()
    dvec_obj_main()
