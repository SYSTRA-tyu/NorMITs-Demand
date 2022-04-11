# -*- coding: utf-8 -*-
"""
Created on: Tues August 17 2021
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:
Master run file to run NoTEM
"""
import sys


sys.path.append("..")
import normits_demand as nd
from normits_demand import constants
from normits_demand.models import NoTEM
from normits_demand.pathing import NoTEMImportPaths


# GLOBAL VARIABLES
# years = [2018, 2033, 2040, 2050]
years = [2018]
scenario = constants.SC01_JAM
notem_iter = '9.7'
lu_drive = "I:/"
by_iteration = "iter3e"
fy_iteration = "iter3e"
notem_import_home = r"I:\NorMITs Demand\import\NoTEM"
notem_export_home = r"E:\NorMITs Demand\NoTEM"
# notem_export_home = r"C:\Data_test\


def main():
    hb_production_import_version = '2.7'
    hb_attraction_import_version = '2.3'
    nhb_production_import_version = '2.1'

    # Define different balancing zones for each mode
    mode_balancing_zones = {5: nd.get_zoning_system("ca_sector_2020")}
    hb_attraction_balance_zoning = nd.BalancingZones.build_single_segment_group(
        nd.get_segmentation_level('notem_hb_output'),
        nd.get_zoning_system('gor'),
        "m",
        mode_balancing_zones,
    )
    nhb_attraction_balance_zoning = nd.BalancingZones.build_single_segment_group(
        nd.get_segmentation_level('notem_nhb_output'),
        nd.get_zoning_system('gor'),
        "m",
        mode_balancing_zones,
    )

    import_builder = NoTEMImportPaths(
        import_home=notem_import_home,
        scenario=scenario,
        years=years,
        land_use_import_home=lu_drive,
        by_land_use_iter=by_iteration,
        fy_land_use_iter=fy_iteration,
        hb_production_import_version=hb_production_import_version,
        hb_attraction_import_version=hb_attraction_import_version,
        nhb_production_import_version=nhb_production_import_version,
    )

    n = NoTEM(
        years=years,
        scenario=scenario,
        iteration_name=notem_iter,
        import_builder=import_builder,
        export_home=notem_export_home,
        hb_attraction_balance_zoning=hb_attraction_balance_zoning,
        nhb_attraction_balance_zoning=nhb_attraction_balance_zoning,
    )
    n.run(
        generate_all=True,
        generate_hb=False,
        generate_nhb=False,
        generate_hb_production=False,
        generate_hb_attraction=True,
        generate_nhb_production=False,
        generate_nhb_attraction=True,
    )


if __name__ == '__main__':
    main()
