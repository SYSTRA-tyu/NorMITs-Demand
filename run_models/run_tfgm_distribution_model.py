# -*- coding: utf-8 -*-
"""
Created on: 07/12/2021
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:

"""
# Built-Ins
import os
import sys

from typing import Tuple

# Third Party

# Local Imports
sys.path.append("..")
import normits_demand as nd
from normits_demand import constants as consts

from normits_demand.models import DistributionModel
from normits_demand.pathing.distribution_model import DistributionModelArgumentBuilder


# ## CONSTANTS ## #
# Trip end import args
notem_iteration_name = '9.3'
notem_export_home = r"G:\NorMITs Demand\NoTEM"
tram_export_home = ""   # Not needed as not using TRAM
cache_path = "E:/dm_cache"      # Set locally for now for a local cache

# Distribution running args
base_year = 2018
scenario = consts.SC01_JAM
dm_iteration_name = '9.3.3-tfgm'
dm_import_home = r"G:\NorMITs Demand\import"
dm_export_home = r"E:\NorMITs Demand\Distribution Model"

# General constants
INIT_PARAMS_BASE = '{trip_origin}_{zoning}_{area}_init_params_{seg}.csv'


def main():
    # mode = nd.Mode.WALK
    # mode = nd.Mode.CYCLE
    mode = nd.Mode.BUS

    # Running params
    use_tram = False
    overwrite_cache = False

    run_hb = True
    run_nhb = True

    run_all = False
    run_upper_model = False
    run_lower_model = True
    run_pa_matrix_reports = False
    run_pa_to_od = False
    run_od_matrix_reports = False
    compile_to_assignment = False

    # ## DEFINE HOW TO RUN ## #
    upper_zoning_system = nd.get_zoning_system('msoa')
    lower_zoning_system = nd.get_zoning_system('tfgm_pt')
    compile_zoning_system = None

    if mode == nd.Mode.WALK:
        # Define cost arguments
        intrazonal_cost_infill = 0.5

        # Define segmentations for trip ends and running
        hb_agg_seg = nd.get_segmentation_level('hb_p_m')
        nhb_agg_seg = nd.get_segmentation_level('tms_nhb_p_m_tp_wday')
        hb_running_seg = nd.get_segmentation_level('hb_p_m_walk')
        nhb_running_seg = nd.get_segmentation_level('tms_nhb_p_m_tp_wday_walk')
        hb_seg_name = 'p_m'
        nhb_seg_name = 'p_m_tp'

    elif mode == nd.Mode.CYCLE:
        # Define cost arguments
        intrazonal_cost_infill = 0.5

        # Define segmentations for trip ends and running
        hb_agg_seg = nd.get_segmentation_level('hb_p_m')
        nhb_agg_seg = nd.get_segmentation_level('tms_nhb_p_m_tp_wday')
        hb_running_seg = nd.get_segmentation_level('hb_p_m_cycle')
        nhb_running_seg = nd.get_segmentation_level('tms_nhb_p_m_tp_wday_cycle')
        hb_seg_name = 'p_m'
        nhb_seg_name = 'p_m_tp'

    elif mode == nd.Mode.BUS:
        # Define cost arguments
        intrazonal_cost_infill = 0.5

        # Define segmentations for trip ends and running
        hb_agg_seg = nd.get_segmentation_level('hb_p_m')
        nhb_agg_seg = nd.get_segmentation_level('tms_nhb_p_m_tp_wday')
        hb_running_seg = nd.get_segmentation_level('hb_p_m_bus')
        nhb_running_seg = nd.get_segmentation_level('tms_nhb_p_m_tp_wday_bus')
        hb_seg_name = 'p_m'
        nhb_seg_name = 'p_m_tp'

    else:
        raise ValueError(
            "Don't know what mode %s is!" % mode.value
        )

    # ## DEFINE HOW TO RUN DISTRIBUTIONS ## #
    upper_calibration_area = 'gb'
    upper_model_method = nd.DistributionMethod.GRAVITY
    upper_calibration_zones_fname = None
    upper_calibration_areas = upper_calibration_area
    upper_calibration_naming = None

    lower_calibration_area = 'north_and_mids'
    lower_model_method = nd.DistributionMethod.GRAVITY
    lower_calibration_zones_fname = None
    lower_calibration_areas = lower_calibration_area
    lower_calibration_naming = None

    gm_cost_function = nd.BuiltInCostFunction.LOG_NORMAL.get_cost_function()

    gravity_kwargs = {
        'cost_function': gm_cost_function,
        'target_convergence': 0.9,
        'grav_max_iters': 100,
        'furness_max_iters': 3000,
        'furness_tol': 0.1,
        'calibrate_params': True,
        'estimate_init_params': False
    }

    upper_distributor_kwargs = gravity_kwargs.copy()
    lower_distributor_kwargs = gravity_kwargs.copy()

    # ## GET TRIP ENDS ## #
    hb_productions, hb_attractions, nhb_productions, nhb_attractions = build_trip_ends(
        use_tram=use_tram,
        zoning_system=upper_zoning_system,
        mode=mode,
        hb_agg_seg=hb_agg_seg,
        hb_running_seg=hb_running_seg,
        nhb_agg_seg=nhb_agg_seg,
        nhb_running_seg=nhb_running_seg,
    )

    # ## BUILD ARGUMENTS ## #
    if lower_zoning_system is not None:
        lower_running_zones = lower_zoning_system.internal_zones
    else:
        lower_running_zones = None

    # arg builder
    dmab_kwargs = {
        'year': base_year,
        'import_home': dm_import_home,
        'running_mode': mode,
        'upper_zoning_system': upper_zoning_system,
        'upper_running_zones': upper_zoning_system.unique_zones,
        'upper_model_method': upper_model_method,
        'upper_distributor_kwargs': upper_distributor_kwargs,
        'upper_calibration_zones_fname': upper_calibration_zones_fname,
        'upper_calibration_areas': upper_calibration_areas,
        'upper_calibration_naming': upper_calibration_naming,
        'lower_zoning_system': lower_zoning_system,
        'lower_running_zones': lower_running_zones,
        'lower_model_method': lower_model_method,
        'lower_distributor_kwargs': lower_distributor_kwargs,
        'lower_calibration_zones_fname': lower_calibration_zones_fname,
        'lower_calibration_areas': lower_calibration_areas,
        'lower_calibration_naming': lower_calibration_naming,
        'init_params_cols': gm_cost_function.parameter_names,
        'intrazonal_cost_infill': intrazonal_cost_infill,
        'cache_path': cache_path,
        'overwrite_cache': overwrite_cache,
    }

    # Distribution model
    dm_kwargs = {
        'iteration_name': dm_iteration_name,
        'upper_model_method': upper_model_method,
        'upper_model_kwargs': None,
        'lower_model_method': lower_model_method,
        'lower_model_kwargs': None,
        'export_home': dm_export_home,
        'process_count': -2,
    }

    # Init params fnames
    upper_kwargs = {'zoning': upper_zoning_system.name, 'area': upper_calibration_area}
    hb_kwargs = {'trip_origin': 'hb', 'seg': hb_seg_name}
    nhb_kwargs = {'trip_origin': 'nhb', 'seg': nhb_seg_name}

    hb_upper_init_params_fname = INIT_PARAMS_BASE.format(**hb_kwargs, **upper_kwargs)
    nhb_upper_init_params_fname = INIT_PARAMS_BASE.format(**nhb_kwargs, **upper_kwargs)

    if lower_zoning_system is not None:
        lower_kwargs = {'zoning': lower_zoning_system.name, 'area': lower_calibration_area}
        hb_lower_init_params_fname = INIT_PARAMS_BASE.format(**hb_kwargs, **lower_kwargs)
        nhb_lower_init_params_fname = INIT_PARAMS_BASE.format(**nhb_kwargs, **lower_kwargs)
    else:
        lower_kwargs = None
        hb_lower_init_params_fname = None
        nhb_lower_init_params_fname = None

    # ## RUN THE MODEL ## #
    if run_hb:
        trip_origin = 'hb'

        arg_builder = DistributionModelArgumentBuilder(
            trip_origin=trip_origin,
            productions=hb_productions,
            attractions=hb_attractions,
            running_segmentation=hb_running_seg,
            upper_init_params_fname=hb_upper_init_params_fname,
            lower_init_params_fname=hb_lower_init_params_fname,
            target_tld_dir=hb_seg_name,
            **dmab_kwargs,
        )

        hb_distributor = DistributionModel(
            arg_builder=arg_builder,
            compile_zoning_system=compile_zoning_system,
            **dm_kwargs,
            **arg_builder.build_distribution_model_init_args(),
        )

        hb_distributor.run(
            run_all=run_all,
            run_upper_model=run_upper_model,
            run_lower_model=run_lower_model,
            run_pa_matrix_reports=run_pa_matrix_reports,
            run_pa_to_od=run_pa_to_od,
            run_od_matrix_reports=run_od_matrix_reports,
        )

    if run_nhb:
        trip_origin = 'nhb'

        arg_builder = DistributionModelArgumentBuilder(
            trip_origin=trip_origin,
            productions=nhb_productions,
            attractions=nhb_attractions,
            running_segmentation=nhb_running_seg,
            upper_init_params_fname=nhb_upper_init_params_fname,
            lower_init_params_fname=nhb_lower_init_params_fname,
            target_tld_dir=nhb_seg_name,
            **dmab_kwargs,
        )

        nhb_distributor = DistributionModel(
            arg_builder=arg_builder,
            compile_zoning_system=compile_zoning_system,
            **dm_kwargs,
            **arg_builder.build_distribution_model_init_args(),
        )

        nhb_distributor.run(
            run_all=run_all,
            run_upper_model=run_upper_model,
            run_lower_model=run_lower_model,
            run_pa_matrix_reports=run_pa_matrix_reports,
            run_pa_to_od=run_pa_to_od,
            run_od_matrix_reports=run_od_matrix_reports,
        )

    # TODO(BT): Move this into Matrix tools!
    #  Fudged to get this to work for now. Handle this better!
    if compile_to_assignment:
        if 'hb_distributor' in locals():
            hb_distributor.compile_to_assignment_format()
        elif 'nhb_distributor' in locals():
            nhb_distributor.compile_to_assignment_format()
        else:
            trip_origin = 'hb'
            arg_builder = DistributionModelArgumentBuilder(
                trip_origin=trip_origin,
                productions=hb_productions,
                attractions=hb_attractions,
                running_segmentation=hb_running_seg,
                upper_init_params_fname=hb_upper_init_params_fname,
                lower_init_params_fname=hb_lower_init_params_fname,
                target_tld_dir=os.path.join(upper_calibration_area, hb_seg_name),
                **dmab_kwargs,
            )

            hb_distributor = DistributionModel(
                arg_builder=arg_builder,
                compile_zoning_system=compile_zoning_system,
                **dm_kwargs,
                **arg_builder.build_distribution_model_init_args(),
            )

            hb_distributor.compile_to_assignment_format()


def build_trip_ends(use_tram,
                    zoning_system,
                    mode,
                    hb_agg_seg,
                    hb_running_seg,
                    nhb_agg_seg,
                    nhb_running_seg,
                    ):
    if use_tram:
        tram = nd.pathing.TramExportPaths(
            path_years=[base_year],
            scenario=scenario,
            iteration_name=notem_iteration_name,
            export_home=tram_export_home,
        )
        hb_productions_path = tram.hb_production.export_paths.notem_segmented[base_year]
        hb_attractions_path = tram.hb_attraction.export_paths.notem_segmented[base_year]
        nhb_productions_path = tram.nhb_production.export_paths.notem_segmented[base_year]
        nhb_attractions_path = tram.nhb_attraction.export_paths.notem_segmented[base_year]

        base_fname = "%s_%s_%s.pkl"
        hbp_path = os.path.join(cache_path, base_fname % ('hbp_tram', zoning_system.name, mode.value))
        hba_path = os.path.join(cache_path, base_fname % ('hba_tram', zoning_system.name, mode.value))
        nhbp_path = os.path.join(cache_path, base_fname % ('nhbp_tram', zoning_system.name, mode.value))
        nhba_path = os.path.join(cache_path, base_fname % ('nhba_tram', zoning_system.name, mode.value))

    else:
        notem = nd.pathing.NoTEMExportPaths(
            path_years=[base_year],
            scenario=scenario,
            iteration_name=notem_iteration_name,
            export_home=notem_export_home,
        )
        hb_productions_path = notem.hb_production.export_paths.notem_segmented[base_year]
        hb_attractions_path = notem.hb_attraction.export_paths.notem_segmented[base_year]
        nhb_productions_path = notem.nhb_production.export_paths.notem_segmented[base_year]
        nhb_attractions_path = notem.nhb_attraction.export_paths.notem_segmented[base_year]

        # TODO(BT): Should we make this a NoTEM output tool?
        base_fname = "%s_%s_%s.pkl"
        hbp_path = os.path.join(cache_path, base_fname % ('hbp', zoning_system.name, mode.value))
        hba_path = os.path.join(cache_path, base_fname % ('hba', zoning_system.name, mode.value))
        nhbp_path = os.path.join(cache_path, base_fname % ('nhbp', zoning_system.name, mode.value))
        nhba_path = os.path.join(cache_path, base_fname % ('nhba', zoning_system.name, mode.value))

    print("Getting the Production/Attraction Vectors...")
    if not os.path.exists(hbp_path) or not os.path.exists(hba_path):
        hb_productions, hb_attractions = import_pa(
            production_import_path=hb_productions_path,
            attraction_import_path=hb_attractions_path,
            agg_segmentation=hb_agg_seg,
            out_segmentation=hb_running_seg,
            zoning_system=zoning_system,
            trip_origin='hb',
            use_tram=use_tram,
        )
        hb_productions.save(hbp_path)
        hb_attractions.save(hba_path)
    else:
        hb_productions = nd.DVector.load(hbp_path)
        hb_attractions = nd.DVector.load(hba_path)

    if not os.path.exists(nhbp_path) or not os.path.exists(nhba_path):
        nhb_productions, nhb_attractions = import_pa(
            production_import_path=nhb_productions_path,
            attraction_import_path=nhb_attractions_path,
            agg_segmentation=nhb_agg_seg,
            out_segmentation=nhb_running_seg,
            zoning_system=zoning_system,
            trip_origin='nhb',
            use_tram=use_tram,
        )
        nhb_productions.save(nhbp_path)
        nhb_attractions.save(nhba_path)
    else:
        nhb_productions = nd.DVector.load(nhbp_path)
        nhb_attractions = nd.DVector.load(nhba_path)

    return (
        hb_productions,
        hb_attractions,
        nhb_productions,
        nhb_attractions,
    )


def import_pa(production_import_path,
              attraction_import_path,
              agg_segmentation,
              out_segmentation,
              zoning_system,
              trip_origin,
              use_tram,
              ) -> Tuple[nd.DVector, nd.DVector]:

    model_name = 'tram' if use_tram else 'notem'

    # Determine the required segmentation
    if trip_origin == 'hb':
        reduce_seg = None
        subset_name = '%s_hb_output_wday'
        subset_seg = nd.get_segmentation_level(subset_name % model_name)
    elif trip_origin == 'nhb':
        reduce_name = '%s_nhb_output_reduced'
        reduce_seg = nd.get_segmentation_level(reduce_name % model_name)
        subset_name = '%s_nhb_output_reduced_wday'
        subset_seg = nd.get_segmentation_level(subset_name % model_name)
    else:
        raise ValueError("Invalid trip origin")

    # Reading pickled Dvector
    prod_dvec = nd.DVector.load(production_import_path)

    # Reduce nhb 11 into 12 if needed
    if reduce_seg is not None:
        prod_dvec = prod_dvec.reduce(out_segmentation=reduce_seg)

    # Convert from ave_week to ave_day
    prod_dvec = prod_dvec.subset(out_segmentation=subset_seg)
    prod_dvec = prod_dvec.convert_time_format('avg_day')

    # Convert zoning and segmentation to desired
    prod_dvec = prod_dvec.aggregate(agg_segmentation)
    prod_dvec = prod_dvec.subset(out_segmentation)
    prod_dvec = prod_dvec.translate_zoning(zoning_system, "population")

    # Reading pickled Dvector
    attr_dvec = nd.DVector.load(attraction_import_path)

    # Reduce nhb 11 into 12 if needed
    if reduce_seg is not None:
        attr_dvec = attr_dvec.reduce(out_segmentation=reduce_seg)

    # Convert from ave_week to ave_day
    attr_dvec = attr_dvec.subset(out_segmentation=subset_seg)
    attr_dvec = attr_dvec.convert_time_format('avg_day')

    # Convert zoning and segmentation to desired
    attr_dvec = attr_dvec.aggregate(agg_segmentation)
    attr_dvec = attr_dvec.subset(out_segmentation)
    attr_dvec = attr_dvec.translate_zoning(zoning_system, "employment")

    return prod_dvec, attr_dvec


if __name__ == '__main__':
    main()
