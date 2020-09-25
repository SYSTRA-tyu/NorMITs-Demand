# -*- coding: utf-8 -*-
"""
Created on: Fri September 11 12:46:25 2020
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:
Collection of functions for translating PA matrices into OD matrices.
TODO: After integrations with TMS, combine with pa_to_od.py
  to create a general pa_to_od.py file

"""

import numpy as np
import pandas as pd

from typing import List

import efs_constants as consts

from demand_utilities import utils as du

# Can call tms pa_to_od.py functions from here
from old_tms.pa_to_od import *


def simplify_time_period_splits(time_period_splits: pd.DataFrame):
    """
    Simplifies time_period_splits to a case where the purpose_from_home
    is always the same as the purpose_to_home

    Parameters
    ----------
    time_period_splits:
        A time_period_splits dataframe extracted using get_time_period_splits()

    Returns
    -------
    time_period_splits only where the purpose_from_home
    is the same as purpose_to_home
    """
    time_period_splits = time_period_splits.copy()

    # Problem column doesn't exist in this case
    if 'purpose_to_home' not in time_period_splits.columns:
        return time_period_splits

    # Build a mask where purposes match
    unq_purpose = time_period_splits['purpose_from_home'].drop_duplicates()
    keep_rows = np.array([False] * len(time_period_splits))
    for p in unq_purpose:
        purpose_mask = (
            (time_period_splits['purpose_from_home'] == p)
            & (time_period_splits['purpose_to_home'] == p)
        )
        keep_rows = keep_rows | purpose_mask

    time_period_splits = time_period_splits.loc[keep_rows]

    # Filter down to just the needed col and return
    needed_cols = [
        'purpose_from_home',
        'time_from_home',
        'time_to_home',
        'direction_factor']
    return time_period_splits.reindex(needed_cols, axis='columns')

def _build_tp_pa_internal(pa_import,
                          pa_export,
                          trip_origin,
                          matrix_format,
                          year,
                          purpose,
                          mode,
                          segment,
                          car_availability,
                          model_zone,
                          tp_split):
    """
    The internals of build_tp_pa(). Useful for making the code more
    readable du to the number of nested loops needed

    Returns
    -------

    """
    # ## Read in 24hr matrix ## #
    productions_fname = du.get_dist_name(
        trip_origin,
        matrix_format,
        str(year),
        str(purpose),
        str(mode),
        str(segment),
        str(car_availability),
        csv=True
    )
    pa_24hr = pd.read_csv(os.path.join(pa_import, productions_fname))

    # Convert from wide to long format
    y_zone = 'a_zone' if model_zone == 'p_zone' else 'd_zone'
    pa_24hr = du.expand_distribution(
        pa_24hr,
        year,
        purpose,
        mode,
        segment,
        car_availability,
        id_vars=model_zone,
        var_name=y_zone,
        value_name='trips'
    )

    # ## Narrow tp_split down to just the segment here ## #
    segment_id = 'soc_id' if purpose in [1, 2] else 'ns_id'
    segmentation_mask = du.get_segmentation_mask(
        tp_split,
        col_vals={
            'purpose_id': purpose,
            'mode_id': mode,
            segment_id: str(segment),
            'car_availability_id': car_availability,
        },
        ignore_missing_cols=True
    )
    tp_split = tp_split.loc[segmentation_mask]
    tp_split = tp_split.reindex([model_zone, 'tp', 'trips'], axis=1)

    # ## Calculate the time split factors for each zone ## #
    unq_zone = tp_split[model_zone].drop_duplicates()
    for zone in unq_zone:
        zone_mask = (tp_split[model_zone] == zone)
        tp_split.loc[zone_mask, 'time_split'] = (
                tp_split[zone_mask]['trips'].values
                /
                tp_split[zone_mask]['trips'].sum()
        )
    time_splits = tp_split.reindex(
        [model_zone, 'tp', 'time_split'],
        axis=1
    )

    # ## Apply tp-split factors to total pa_24hr ## #
    unq_time = time_splits['tp'].drop_duplicates()
    for time in unq_time:
        # Need to do a left join, and set any missing vals. Ensures
        # zones don't go missing if there's an issue with tp_split input
        # NOTE: tp3 is missing for p2, m1, soc0, ca1
        time_factors = time_splits.loc[time_splits['tp'] == time]
        gb_tp = pd.merge(
            pa_24hr,
            time_factors,
            on=[model_zone],
            how='left'
        ).rename(columns={'trips': 'dt'})
        gb_tp['time_split'] = gb_tp['time_split'].fillna(0)
        gb_tp['tp'] = gb_tp['tp'].fillna(time).astype(int)

        # Calculate the number of trips for this time_period
        gb_tp['dt'] = gb_tp['dt'] * gb_tp['time_split']

        # ## Aggregate back up to our segmentation ## #
        all_seg_cols = [
            model_zone,
            y_zone,
            "purpose_id",
            "mode_id",
            "soc_id",
            "ns_id",
            "car_availability_id",
            "tp"
        ]

        # Get rid of cols we're not using
        seg_cols = [x for x in all_seg_cols if x in gb_tp.columns]
        gb_tp = gb_tp.groupby(seg_cols)["dt"].sum().reset_index()

        # Build write path
        tp_pa_name = du.get_dist_name(
            str(trip_origin),
            str(matrix_format),
            str(year),
            str(purpose),
            str(mode),
            str(segment),
            str(car_availability),
            tp=str(time)
        )
        tp_pa_fname = tp_pa_name + '.csv'
        out_tp_pa_path = os.path.join(
            pa_export,
            tp_pa_fname
        )

        # Convert table from long to wide format and save
        # TODO: Generate header based on mode used
        du.long_to_wide_out(
            gb_tp.rename(columns={model_zone: 'norms_zone_id'}),
            v_heading='norms_zone_id',
            h_heading=y_zone,
            values='dt',
            out_path=out_tp_pa_path
        )


def efs_build_tp_pa(tp_import: str,
                    pa_import: str,
                    pa_export: str,
                    year_string_list: List[str],
                    required_purposes: List[int],
                    required_modes: List[int],
                    required_soc: List[int] = None,
                    required_ns: List[int] = None,
                    required_ca: List[int] = None,
                    matrix_format: str = 'pa'
                    ) -> None:
    """
    Converts the 24hr matrices in pa_import into time_period segmented
    matrices - outputting to pa_export

    Parameters
    ----------
    tp_import:
        Path to the dir containing the seed values to use for splitting
        pa_import matrices by tp

    pa_import:
        Path to the dir containing the 24hr matrices

    pa_export:
        Path to the dir to export the tp split matrices

    year_string_list:
        A list of which years of 24hr Matrices to convert.

    required_purposes:
        A list of which purposes of 24hr Matrices to convert.

    required_modes:
        A list of which modes of 24hr Matrices to convert.

    required_soc:
        A list of which soc of 24hr Matrices to convert.

    required_ns:
        A list of which ns of 24hr Matrices to convert.

    required_ca:
        A list of which car availabilities of 24hr Matrices to convert.

    matrix_format:
        Which format the matrix is in. Either 'pa' or 'od'

    Returns
    -------
        None

    """
    # Arg init
    if matrix_format not in consts.VALID_MATRIX_FORMATS:
        raise ValueError("'%s' is not a valid matrix format."
                         % str(matrix_format))

    # TODO: Infer these arguments based on pa_import
    #  Along with yr, p, m
    required_soc = [None] if required_soc is None else required_soc
    required_ns = [None] if required_ns is None else required_ns
    required_ca = [None] if required_ca is None else required_ca

    # Loop Init
    if matrix_format == 'pa':
        model_zone = 'p_zone'
    elif matrix_format == 'od':
        model_zone = 'o_zone'
    else:
        # Shouldn't be able to get here
        raise ValueError("'%s' seems to be a valid matrix format, "
                         "but build_tp_pa() cannot handle it. Sorry :(")

    # For every: Year, purpose, mode, segment, ca
    for year in year_string_list:
        for purpose in required_purposes:
            # Purpose specific set-up
            # Do it here to avoid repeats in inner loops
            if purpose in consts.ALL_NHB_P:
                trip_origin = 'nhb'
                required_segments = [None]
                tp_split_fname = 'export_nhb_productions_norms.csv'
                tp_split_path = os.path.join(tp_import, tp_split_fname)

            elif purpose in consts.ALL_HB_P:
                trip_origin = 'hb'
                tp_split_fname = 'export_productions_norms.csv'
                tp_split_path = os.path.join(tp_import, tp_split_fname)
                if purpose in [1, 2]:
                    required_segments = required_soc
                else:
                    required_segments = required_ns

            else:
                raise ValueError("%s is not a valid purpose."
                                 % str(purpose))

            # Read in the seed values for tp splits
            tp_split = pd.read_csv(tp_split_path).rename(
                columns={
                    'norms_zone_id': model_zone,
                    'p': 'purpose_id',
                    'm': 'mode_id',
                    'soc': 'soc_id',
                    'ns': 'ns_id',
                    'ca': 'car_availability_id',
                    'time': 'tp'
                }
            )
            tp_split[model_zone] = tp_split[model_zone].astype(int)

            # Compile aggregate to p/m if NHB
            if trip_origin == 'nhb':
                tp_split = tp_split.groupby(
                    [model_zone, 'purpose_id', 'mode_id', 'tp']
                )['trips'].sum().reset_index()

            for mode in required_modes:
                print("Working on yr%s_p%s_m%s..."
                      % (str(year), str(purpose), str(mode)))
                for segment in required_segments:
                    for car_availability in required_ca:
                        _build_tp_pa_internal(
                            pa_import,
                            pa_export,
                            trip_origin,
                            matrix_format,
                            year,
                            purpose,
                            mode,
                            segment,
                            car_availability,
                            model_zone,
                            tp_split
                        )
    return


def _build_od_internal(pa_import,
                       od_export,
                       calib_params,
                       phi_lookup_folder,
                       phi_type,
                       aggregate_to_wday,
                       full_od_out=False,
                       echo=True):
    """
    The internals of build_od(). Useful for making the code more
    readable du to the number of nested loops needed

    TODO: merge with TMS - NOTE:
    All this code below has been mostly copied from TMS pa_to_od.py
    function of the same name. A few filenames etc have been changed
    to make sure it properly works with NorMITs demand files (This is
    du to NorMITs demand needing moving in entirety over to the Y drive)

    Returns
    -------

    """
    # Init
    tps = ['tp1', 'tp2', 'tp3', 'tp4']
    matrix_totals = list()
    dir_contents = os.listdir(pa_import)
    mode = calib_params['m']
    purpose = calib_params['p']

    model_name = du.get_model_name(mode)
    model_zone_col = model_name + '_zone_id'

    # Get appropriate phis and filter
    phi_factors = get_time_period_splits(
        mode,
        phi_type,
        aggregate_to_wday=aggregate_to_wday,
        lookup_folder=phi_lookup_folder)
    phi_factors = simplify_time_period_splits(phi_factors)
    phi_factors = phi_factors[phi_factors['purpose_from_home'] == purpose]

    # Get the relevant filenames from the dir
    dir_subset = dir_contents.copy()
    for name, param in calib_params.items():
        # Work around for 'p2' clashing with 'tp2'
        if name == 'p':
            dir_subset = [x for x in dir_subset if '_' + name + str(param) in x]
        else:
            dir_subset = [x for x in dir_subset if (name + str(param)) in x]

    # Build dict of tp names to filenames
    tp_names = {}
    for tp in tps:
        tp_names.update({tp: [x for x in dir_subset if tp in x][0]})

    # ## Build from_home dict from imported from_home PA ## #
    frh_dist = {}
    for tp, path in tp_names.items():
        dist_df = pd.read_csv(os.path.join(pa_import, path))
        zone_nums = dist_df[model_zone_col]     # Save to re-attach later
        dist_df = dist_df.drop(model_zone_col, axis=1)
        frh_dist.update({tp: dist_df})

    # ## Build to_home matrices from the from_home PA ## #
    frh_ph = {}
    for tp_frh in tps:
        du.print_w_toggle('From from_h ' + str(tp_frh), echo=echo)
        frh_int = int(tp_frh.replace('tp', ''))
        phi_frh = phi_factors[phi_factors['time_from_home'] == frh_int]

        # Transpose to flip P & A
        frh_base = frh_dist[tp_frh].copy()
        frh_base = frh_base.values.T

        toh_dists = {}
        for tp_toh in tps:
            # Get phi
            du.print_w_toggle('\tBuilding to_h ' + str(tp_toh), echo=echo)
            toh_int = int(tp_toh.replace('tp', ''))
            phi_toh = phi_frh[phi_frh['time_to_home'] == toh_int]
            phi_toh = phi_toh['direction_factor']

            # Cast phi toh
            phi_mat = np.broadcast_to(phi_toh,
                                      (len(frh_base),
                                       len(frh_base)))
            tp_toh_mat = frh_base * phi_mat
            toh_dists.update({tp_toh: tp_toh_mat})
        frh_ph.update({tp_frh: toh_dists})

    # ## Aggregate to_home matrices by time period ## #
    # removes the from_home splits
    tp1_list = list()
    tp2_list = list()
    tp3_list = list()
    tp4_list = list()
    for item, toh_dict in frh_ph.items():
        for toh_tp, toh_dat in toh_dict.items():
            if toh_tp == 'tp1':
                tp1_list.append(toh_dat)
            elif toh_tp == 'tp2':
                tp2_list.append(toh_dat)
            elif toh_tp == 'tp3':
                tp3_list.append(toh_dat)
            elif toh_tp == 'tp4':
                tp4_list.append(toh_dat)

    toh_dist = {
        'tp1': np.sum(tp1_list, axis=0),
        'tp2': np.sum(tp2_list, axis=0),
        'tp3': np.sum(tp3_list, axis=0),
        'tp4': np.sum(tp4_list, axis=0)
    }

    # ## Output the from_home and to_home matrices ## #
    for tp in tps:
        # Get output matrices
        output_name = tp_names[tp]

        output_from = frh_dist[tp]
        from_total = output_from.sum().sum()
        output_from_name = output_name.replace('pa', 'od_from')

        output_to = toh_dist[tp]
        to_total = output_to.sum().sum()
        output_to_name = output_name.replace('pa', 'od_to')

        # ## Gotta fudge the row/column names ## #
        # Add the zone_nums back on
        output_from = pd.DataFrame(output_from).reset_index()
        # noinspection PyUnboundLocalVariable
        output_from['index'] = zone_nums
        output_from.columns = [model_zone_col] + zone_nums.tolist()
        output_from = output_from.set_index(model_zone_col)

        output_to = pd.DataFrame(output_to).reset_index()
        output_to['index'] = zone_nums
        output_to.columns = [model_zone_col] + zone_nums.tolist()
        output_to = output_to.set_index(model_zone_col)

        # With columns fixed, created full OD output
        output_od = output_from + output_to
        output_od_name = output_name.replace('pa', 'od')

        du.print_w_toggle('Exporting ' + output_from_name, echo=echo)
        du.print_w_toggle('& ' + output_to_name, echo=echo)
        if full_od_out:
            du.print_w_toggle('& ' + output_od_name, echo=echo)
        du.print_w_toggle('To ' + od_export, echo=echo)

        # Output from_home, to_home and full OD matrices
        output_from_path = os.path.join(od_export, output_from_name)
        output_to_path = os.path.join(od_export, output_to_name)
        output_od_path = os.path.join(od_export, output_od_name)

        # Auditing checks - tidality
        # OD from = PA
        # OD to = if it leaves it should come back
        # OD = 2(PA)
        output_from.to_csv(output_from_path)
        output_to.to_csv(output_to_path)
        if full_od_out:
            output_od.to_csv(output_od_path)

        matrix_totals.append([output_name, from_total, to_total])

    dist_name = du.calib_params_to_dist_name('hb', 'od', calib_params)
    print("INFO: OD Matrices for %s written to file." % dist_name)
    return matrix_totals


def efs_build_od(pa_import,
                 od_export,
                 required_purposes,
                 required_modes,
                 required_soc,
                 required_ns,
                 required_car_availabilities,
                 year_string_list,
                 phi_lookup_folder=None,
                 phi_type='fhp_tp',
                 aggregate_to_wday=True,
                 echo=True):
    """
    This function imports time period split factors from a given path.W
    """
    # Init
    if phi_lookup_folder is None:
        phi_lookup_folder = 'Y:/NorMITs Demand/import/phi_factors'

    # For every: Year, purpose, mode, segment, ca
    matrix_totals = list()
    for year in year_string_list:
        for purpose in required_purposes:
            required_segments = required_soc if purpose in [1, 2] else required_ns
            for mode in required_modes:
                for segment in required_segments:
                    for ca in required_car_availabilities:
                        calib_params = du.generate_calib_params(
                            year,
                            purpose,
                            mode,
                            segment,
                            ca
                        )
                        segmented_matrix_totals = _build_od_internal(
                            pa_import,
                            od_export,
                            calib_params,
                            phi_lookup_folder,
                            phi_type,
                            aggregate_to_wday,
                            echo=echo)
                        matrix_totals += segmented_matrix_totals
    return matrix_totals
