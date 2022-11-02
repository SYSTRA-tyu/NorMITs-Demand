# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 15:04:16 2022

@author: mishtaiwi - Systra
"""
# ## IMPORTS ## #
# Standard imports
import os
import sys
import logging
import itertools
from datetime import datetime

# Third party imports
import pandas as pd
from tqdm import tqdm

# Local imports
# pylint: disable=import-error,wrong-import-position
# pylint: enable=import-error,wrong-import-position

# ## CONSTANTS ## #
LOG = logging.getLogger(__name__)

# ## CLASSES ## #

# ## FUNCTIONS ## #
def check_file_exist(file):
    """
    Parameters
    ----------
    file : str
        full path to the file.

    Function
    ---------
    function checks if the file doesn't exist and report when it doesn't
    the function will force quit when a file doesn;t exist

    Returns
    -------
    None.

    """
    if not os.path.isfile(file):
        print(f" -- File not found - {file}")
        logging.info(f" -- File not found - {file}")
        sys.exit()


def add_tls2stations_matrix(mx, stnTLC):
    """
    Parameters
    ----------
    mx : pandas dataframe
        stn 2 stn matrix dataframe.
    stnTLC : TYPE
        sttion zone ID to TLC lookup.

    Function
    ---------
    Adds TLC to stn2stn matrix based on stn zone ID

    Returns
    -------
    mx : pandas dataframe
        matrix with TLCs.

    """
    # add TLCs
    mx = mx.merge(stnTLC, how="left", left_on=["from_stn_zone_id"], right_on=["stn_zone_id"])
    # keep needed columns
    mx = mx[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "to_stn_zone_id",
            "userclass",
            "Distance",
            "Demand",
            "STATIONCODE",
            "STATIONNAME",
        ]
    ]
    # rename column
    mx = mx.rename(columns={"STATIONCODE": "O_TLC", "STATIONNAME": "O_StnName"})
    # add TLCs
    mx = mx.merge(stnTLC, how="left", left_on=["to_stn_zone_id"], right_on=["stn_zone_id"])
    # keep needed columns
    mx = mx[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "O_TLC",
            "O_StnName",
            "to_stn_zone_id",
            "STATIONCODE",
            "STATIONNAME",
            "userclass",
            "Distance",
            "Demand",
        ]
    ]
    # rename column
    mx = mx.rename(columns={"STATIONCODE": "D_TLC", "STATIONNAME": "D_StnName"})

    return mx


def prepare_stn2stn_matrix(demandMX, iRSjProps, distMX, stnTLC):
    """
    Parameters
    ----------
    demandMX : pandas dataframe
        demand matrix dataframe
    iRSjProps : pandas dataframe
        iRSj split probabilities dataframe
    distMX : pandas dataframe
        stn2stn distance matrix
    stnTLC : pandas dataframe
        station zone id to TLC lookup dataframe

    Function
    ---------
    produce stn2stn demand matrix with distance and TLC codes

    Returns
    -------
    df : pandas dataframe
        demand matrix with added attributes of Distacne and TLCs

    """
    # merge demand matrix to iRSj probabilities
    df = demandMX.merge(
        iRSjProps, how="left", on=["from_model_zone_id", "to_model_zone_id", "userclass"]
    )
    # fill nans if any
    # df = df.fillna(0)
    # calculate movement demand proportion
    df["Demand"] = df["Demand"] * df["proportion"]
    # group by stn2stn
    df = (
        df.groupby(
            [
                "from_model_zone_id",
                "to_model_zone_id",
                "from_stn_zone_id",
                "to_stn_zone_id",
                "userclass",
            ]
        )["Demand"]
        .sum()
        .reset_index()
    )
    # remove records of zero stations
    df = df.loc[df["from_stn_zone_id"] != 0].reset_index()
    # add distance matrix to get stn2stn distance
    df = df.merge(distMX, how="left", on=["from_stn_zone_id", "to_stn_zone_id"])
    # keep needed columns
    df = df[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "to_stn_zone_id",
            "userclass",
            "tran_distance",
            "Demand",
        ]
    ]
    # rename column
    df = df.rename(columns={"tran_distance": "Distance"})
    # add TLCs
    df = add_tls2stations_matrix(df, stnTLC)

    return df


def prepare_stn2stn_matrix_tohome(demandMX, iRSjProps, distMX, stnTLC):
    """
    Parameters
    ----------
    demandMX : pandas dataframe
        demand matrix dataframe
    iRSjProps : pandas dataframe
        iRSj split probabilities dataframe
    distMX : pandas dataframe
        stn2stn distance matrix
    stnTLC : pandas dataframe
        station zone id to TLC lookup dataframe

    Function
    ---------
    produce stn2stn demand matrix with distance and TLC codes for 'To Home' segments
    i.e. transpose the matrix into OD matrix

    Returns
    -------
    df : pandas dataframe
        demand matrix with added attributes of Distacne and TLCs

    """
    # transpose matrix
    demandMX = transpose_matrix(demandMX)
    # merge demand matrix to iRSj probabilities
    df = demandMX.merge(
        iRSjProps,
        how="left",
        left_on=["from_model_zone_id", "to_model_zone_id", "userclass"],
        right_on=["to_model_zone_id", "from_model_zone_id", "userclass"],
    )
    # rename column
    df = df.rename(
        columns={
            "from_model_zone_id_x": "from_model_zone_id",
            "to_model_zone_id_x": "to_model_zone_id",
        }
    )
    # fill nans if any
    # df = df.fillna(0)
    # calculate movement demand proportion
    df["Demand"] = df["Demand"] * df["proportion"]
    # group by stn2stn
    df = (
        df.groupby(
            [
                "from_model_zone_id",
                "to_model_zone_id",
                "from_stn_zone_id",
                "to_stn_zone_id",
                "userclass",
            ]
        )["Demand"]
        .sum()
        .reset_index()
    )
    # remove records of zer ostations
    df = df.loc[df["from_stn_zone_id"] != 0].reset_index()
    # add distance matrix to get stn2stn distance
    df = df.merge(distMX, how="left", on=["from_stn_zone_id", "to_stn_zone_id"])
    # keep needed columns
    df = df[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "to_stn_zone_id",
            "userclass",
            "tran_distance",
            "Demand",
        ]
    ]
    # rename column
    df = df.rename(columns={"tran_distance": "Distance"})
    # add TLCs
    df = add_tls2stations_matrix(df, stnTLC)

    return df


def assign_edge_flow(flows_file, flows_lookup, mx):
    """
    Parameters
    ----------
    flows_file : pandas dataframe
        EDGE flows lookup dataframe
    flows_lookup: pandas dataframe
        lookup dataframe bwteen EDGE flows and TAG nondistance flows
    mx : pandas dataframe
        stn2stn matrix to assign flows to

    Function
    ---------
    assign EDGE flow based on stations movement, and match it to TAG non-distance flow

    Returns
    -------
    df : pandas dataframe
        demand matrix with added EDGE flows
    """
    # rename column
    edge_flows = flows_file.rename(
        columns={"FromCaseZoneID": "O_TLC", "ToCaseZoneID": "D_TLC"}
    )
    # keep needed columns
    edge_flows = edge_flows[["O_TLC", "D_TLC", "FlowCatID", "FlowCatName"]]
    # merge to matrix
    mx = mx.merge(edge_flows, how="left", on=["O_TLC", "D_TLC"])
    # add TAG flows
    mx = mx.merge(flows_lookup, how="left", on=["FlowCatName"])

    return mx


def assign_purposes(df):
    """
    Parameters
    ----------
    df : pandas dataframe
        dataframe with userclass info to add purpose info to


    Function
    ---------
    add purpose category to a dataframe based on userclass as below:
        1-3: EB
        4-6: Com
        7-9: Oth

    Returns
    -------
    df : pandas dataframe
        dataframe with added userclass info
    """
    # create empty vector
    df["Purpose"] = ""
    # EB
    df["Purpose"].loc[df["userclass"] == 1] = "Business"
    df["Purpose"].loc[df["userclass"] == 2] = "Business"
    df["Purpose"].loc[df["userclass"] == 3] = "Business"
    # Com
    df["Purpose"].loc[df["userclass"] == 4] = "Commuting"
    df["Purpose"].loc[df["userclass"] == 5] = "Commuting"
    df["Purpose"].loc[df["userclass"] == 6] = "Commuting"
    # Oth
    df["Purpose"].loc[df["userclass"] == 7] = "Leisure"
    df["Purpose"].loc[df["userclass"] == 8] = "Leisure"
    df["Purpose"].loc[df["userclass"] == 9] = "Leisure"

    return df


def add_distance_band_tag_flow(df):
    """
    Parameters
    ----------
    df : pandas dataframe
        prepared matrix with flows


    Function
    ---------
    function creates new flow category vector that matches TAGs ticket/purpose split

    Returns
    -------
    df : pandas dataframe
        dataframe with added new TAG flow
    """
    # set new flow to match the non-distance flow to begin with
    df["TAG_Flow"] = df["TAG_NonDist"]
    # Outside South East
    df["TAG_Flow"].loc[
        (df["TAG_NonDist"] == "Outside South East") & (df["Distance"] < 25)
    ] = "Outside South East <25 miles"
    df["TAG_Flow"].loc[
        (df["TAG_NonDist"] == "Outside South East")
        & (df["Distance"] >= 25)
        & (df["Distance"] < 100)
    ] = "Outside South East 25 to 100 miles"
    df["TAG_Flow"].loc[
        (df["TAG_NonDist"] == "Outside South East") & (df["Distance"] >= 100)
    ] = "Outside South East  100 + miles - adjusted"
    # Outside South East to/from London
    df["TAG_Flow"].loc[
        (df["TAG_NonDist"] == "Outside South East to/from London") & (df["Distance"] < 100)
    ] = "Outside South East to/from London < 100 miles"
    df["TAG_Flow"].loc[
        (df["TAG_NonDist"] == "Outside South East to/from London") & (df["Distance"] >= 100)
    ] = "Outside South East to/from London 100 + miles"

    return df


def apply_ticket_splits(df):
    """
    Parameters
    ----------
    df : pandas dataframe
        prepared demand matrix with ticket split proportions


    Function
    ---------
    function applies ticket split proportions to calculate the demand by ticket/prupose

    Returns
    -------
    df : pandas dataframe
        demand matrix by flow, ticket type and purpose
    """
    # apply split proportions by ticket type
    df["F"] = df["F"] * df["Demand"]
    df["R"] = df["R"] * df["Demand"]
    df["S"] = df["S"] * df["Demand"]

    # keep needed columns
    df = df[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "O_TLC",
            "to_stn_zone_id",
            "D_TLC",
            "userclass",
            "Purpose",
            "F",
            "R",
            "S",
        ]
    ]

    return df


def create_factors_for_missing_moira_movements(mx, edgeFactors, other_tickets_df, no_factors_df):
    """
    Parameters
    ----------
    mx : pandas dataframe
        prepared stn2stn matrix by flow, ticket type and purpose
    edge_factors : pandas dataframe
        EDGE gowth factors by flow, ticket type and purpose
    other_tickets_df : pandas dataframe
        dataframe to record movements where other ticket types where used to fill
        for missing ticket types
    no_factors_df : pandas dataframe
        dataframe to record movemetns where no factor was found at all

    Function
    ---------
    some movements don't have factors for specific ticket types or purposes given these do not
    appear in MOIRA, hence this function populate growth factor records for missing
    movments/ticket types with available ticket types/purposes for the same movement

    Returns
    -------
    edge_factors : pandas dataframe
        updated edge factors daatframe with added records for missing movements/tickets
    """

    # keep needed columns
    edgeFactors = edgeFactors[
        ["ZoneCodeFrom", "ZoneCodeTo", "purpose", "TicketType", "Demand_rate", "Flag"]
    ]
    # remove records with growth of nan
    edgeFactors = edgeFactors[~edgeFactors["Demand_rate"].isna()].reset_index(drop=True)
    # copy of factors dataframe
    upd_edge_factors = edgeFactors.copy()
    # melt Matrix
    mx = pd.melt(
        mx,
        id_vars=[
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "O_TLC",
            "to_stn_zone_id",
            "D_TLC",
            "userclass",
            "Purpose",
        ],
        value_vars=["F", "R", "S"],
    )
    # rename column
    mx = mx.rename(
        columns={
            "value": "T_Demand",
            "variable": "TicketType",
            "O_TLC": "ZoneCodeFrom",
            "D_TLC": "ZoneCodeTo",
            "Purpose": "purpose",
        }
    )
    # merge factors to matrix
    mx = mx.merge(
        edgeFactors, how="left", on=["ZoneCodeFrom", "ZoneCodeTo", "TicketType", "purpose"]
    )

    # get records of movements missing in MOIRA
    missing_moira = mx[mx["Demand_rate"].isna()].reset_index(drop=True)
    # copy with zonal info
    missing_moira_zonal = missing_moira.copy()
    # add internal flag
    missing_moira_zonal["Internal"] = 0
    missing_moira_zonal["Internal"][
        (missing_moira_zonal["from_model_zone_id"] < 1158)
        & (missing_moira_zonal["to_model_zone_id"] < 1158)
    ]
    # group to stn2stn
    missing_moira = (
        missing_moira.groupby(["ZoneCodeFrom", "ZoneCodeTo", "TicketType", "purpose"])[
            "T_Demand"
        ]
        .sum()
        .reset_index()
    )

    # merge factors
    missing_moira = missing_moira.merge(
        edgeFactors, how="left", on=["ZoneCodeFrom", "ZoneCodeTo", "purpose"]
    )
    missing_moira_zonal = missing_moira_zonal.merge(
        edgeFactors, how="left", on=["ZoneCodeFrom", "ZoneCodeTo", "purpose"]
    )
    # rename column
    missing_moira = missing_moira.rename(
        columns={"TicketType_y": "Available_TicketType", "TicketType_x": "Missing_TicketType"}
    )
    missing_moira_zonal = missing_moira_zonal.rename(
        columns={"TicketType_y": "Available_TicketType", "TicketType_x": "Missing_TicketType"}
    )
    # keep those that have available records
    available_ticket = missing_moira[
        ~missing_moira["Available_TicketType"].isna()
    ].reset_index(drop=True)
    # keep one ticket type
    available_ticket = available_ticket.drop_duplicates(
        subset=["ZoneCodeFrom", "ZoneCodeTo", "Missing_TicketType", "purpose"]
    )
    # keep needed columns
    available_ticket = available_ticket[
        [
            "ZoneCodeFrom",
            "ZoneCodeTo",
            "purpose",
            "Missing_TicketType",
            "Available_TicketType",
            "Demand_rate",
        ]
    ]

    # keep needed columns
    available_ticket = available_ticket[
        ["ZoneCodeFrom", "ZoneCodeTo", "purpose", "Missing_TicketType", "Demand_rate"]
    ]
    # rename columns
    available_ticket = available_ticket.rename(columns={"Missing_TicketType": "TicketType"})
    # create flag with '0' indicating it's a ppopulated factor
    available_ticket["Flag"] = 0
    # add to EDGE factors
    upd_edge_factors = pd.concat([upd_edge_factors, available_ticket], axis=0)

    # logging DFs
    # keep those that have available records
    available_ticket_log = missing_moira_zonal[
        ~missing_moira_zonal["Available_TicketType"].isna()
    ].reset_index(drop=True)
    available_ticket_log = (
        available_ticket_log.groupby(
            [
                "ZoneCodeFrom",
                "ZoneCodeTo",
                "purpose",
                "Missing_TicketType",
                "Available_TicketType",
                "Internal",
            ]
        )["T_Demand"]
        .sum()
        .reset_index()
    )
    # log these movments to main dataframe
    other_tickets_df = pd.concat([other_tickets_df, available_ticket_log], axis=0)

    # check missing tickets for current purpsoe in different purposes
    missing_ticket = missing_moira_zonal[
        missing_moira_zonal["Available_TicketType"].isna()
    ].reset_index(drop=True)
    # keep needed columns
    missing_ticket = (
        missing_ticket.groupby(["ZoneCodeFrom", "ZoneCodeTo", "Internal"])["T_Demand"]
        .sum()
        .reset_index()
    )
    # log these movments to main dataframe
    no_factors_df = pd.concat([no_factors_df, missing_ticket], axis=0)

    return upd_edge_factors, other_tickets_df, no_factors_df


def apply_edge_growth_method1(mx, edgeFactors):
    """
    Parameters
    ----------
    mx : pandas dataframe
        prepared stn2stn matrix by flow, ticket type and purpose
    edgeFactors : pandas dataframe
        EDGE gowth factors by flow, ticket type and purpose


    Function
    ---------
    applies EDGE growth to the stn2stn demand by flow, ticekt type and purpose
    using method 1 where the factors are applied on P=O and A=D level

    Returns
    -------
    mx : pandas dataframe
        grown stn2stn demand matrix
    """
    # melt Matrix
    mx = pd.melt(
        mx,
        id_vars=[
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "O_TLC",
            "to_stn_zone_id",
            "D_TLC",
            "userclass",
            "Purpose",
        ],
        value_vars=["F", "R", "S"],
    )
    # rename column
    mx = mx.rename(
        columns={
            "value": "T_Demand",
            "variable": "TicketType",
            "O_TLC": "ZoneCodeFrom",
            "D_TLC": "ZoneCodeTo",
            "Purpose": "purpose",
        }
    )
    # merge new factors file to matrix
    mx = mx.merge(
        edgeFactors, how="left", on=["ZoneCodeFrom", "ZoneCodeTo", "TicketType", "purpose"]
    )
    # Records with nan means no factor was found hence no frowth therfore fill nan with 1
    mx["Demand_rate"] = mx[["Demand_rate"]].fillna(1)
    # fill nan flag with zero as it doesn;t exist in the inut EDGE factors
    mx["Flag"] = mx[["Flag"]].fillna(0)
    # apply growth
    mx["N_Demand"] = mx["T_Demand"] * mx["Demand_rate"]
    # keep needed columns
    mx = mx[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "ZoneCodeFrom",
            "to_stn_zone_id",
            "ZoneCodeTo",
            "userclass",
            "purpose",
            "TicketType",
            "T_Demand",
            "N_Demand",
        ]
    ]

    return mx


def apply_edge_growth_method2(mx, edgeFactors):
    """
    Parameters
    ----------
    mx : pandas dataframe
        prepared stn2stn matrix by flow, ticket type and purpose
    edgeFactors : pandas dataframe
        EDGE gowth factors by flow, ticket type and purpose


    Function
    ---------
    applies EDGE growth to the stn2stn demand by flow, ticekt type and purpose
    using method 2 where an avergae factor of the two directions is applied

    Returns
    -------
    mx : pandas dataframe
        grown stn2stn demand matrix
    """
    # melt Matrix
    mx = pd.melt(
        mx,
        id_vars=[
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "O_TLC",
            "to_stn_zone_id",
            "D_TLC",
            "userclass",
            "Purpose",
        ],
        value_vars=["F", "R", "S"],
    )
    # rename column
    mx = mx.rename(
        columns={
            "value": "T_Demand",
            "variable": "TicketType",
            "O_TLC": "ZoneCodeFrom",
            "D_TLC": "ZoneCodeTo",
            "Purpose": "purpose",
        }
    )
    # merge new factors file to matrix on first direction O>D
    mx = mx.merge(
        edgeFactors, how="left", on=["ZoneCodeFrom", "ZoneCodeTo", "TicketType", "purpose"]
    )
    # rename growt column to indicate first merge
    # rename column
    mx = mx.rename(columns={"Demand_rate": "1st_Dir_Growth"})
    # merge new factors file to matrix on second direction O>D
    mx = mx.merge(
        edgeFactors,
        how="left",
        left_on=["ZoneCodeFrom", "ZoneCodeTo", "TicketType", "purpose"],
        right_on=["ZoneCodeTo", "ZoneCodeFrom", "TicketType", "purpose"],
    )
    # rename growt column to indicate first merge
    # rename column
    mx = mx.rename(
        columns={
            "ZoneCodeFrom_x": "ZoneCodeFrom",
            "ZoneCodeTo_x": "ZoneCodeTo",
            "Demand_rate": "2nd_Dir_Growth",
        }
    )
    # get average growth for both directions
    mx["Demand_rate"] = mx[["1st_Dir_Growth", "2nd_Dir_Growth"]].mean(axis=1)
    # Records with nan means no factor was found hence no growth therfore fill nan with 1
    mx["Demand_rate"] = mx[["Demand_rate"]].fillna(1)
    # fill nan flag with zero as it doesn;t exist in the inut EDGE factors
    mx["Flag_x"] = mx[["Flag_x"]].fillna(0)
    mx["Flag_y"] = mx[["Flag_y"]].fillna(0)
    mx["Flag"] = 0
    mx["Flag"].loc[(mx["Flag_x"] == 1) & (mx["Flag_y"] == 1)] = 1
    # apply growth
    mx["N_Demand"] = mx["T_Demand"] * mx["Demand_rate"]
    # keep needed columns
    mx = mx[
        [
            "from_model_zone_id",
            "to_model_zone_id",
            "from_stn_zone_id",
            "ZoneCodeFrom",
            "to_stn_zone_id",
            "ZoneCodeTo",
            "userclass",
            "purpose",
            "TicketType",
            "T_Demand",
            "N_Demand",
        ]
    ]

    return mx


def prepare_logging_info(other_tickets_df, no_factors_df, demand_total):
    """
    Parameters
    ----------
    other_tickets_df : pandas dataframe
        dataframe with movements that has used factors for other ticket types
    no_factors_df : pandas dataframe
        dataframe with movements that has no factor at all
    demand_total : float
        sum of all input demand

    Function
    ---------
    function calculates logging stats of the proportion of the demand each category
    has and the proportion of theat demand that is internal
    the fucntion also prepare the dataframe in a format ready to print to the logfile

    Returns
    -------
    other_tickets_df : pandas dataframe
        dataframe with movements that has used factors for other ticket types
    no_factors_df : pandas dataframe
        dataframe with movements that has no factor at all
    no_factor_demand_prop: float
        proportion of total demand with no factors proportion
    tickets_demand_prop: float
        proportion of total demand where factors for other ticket types were used
    tickets_internal_prop: float
        internal demand proportion out of tickets_demand_prop
    no_factor_demand_prop: float
        internal demand proportion out of no_factor_demand_prop
    """
    # log warning info
    # get demand total by movement/ticekt
    other_tickets_df = (
        other_tickets_df.groupby(
            [
                "ZoneCodeFrom",
                "ZoneCodeTo",
                "purpose",
                "Missing_TicketType",
                "Available_TicketType",
                "Internal",
            ]
        )["T_Demand"]
        .sum()
        .reset_index()
    )
    # get demand totals for movements where different ticket type was used
    demand_total_ticket = other_tickets_df["T_Demand"].sum()
    # demand total for internals
    demand_total_ticket_internal = other_tickets_df["T_Demand"][
        other_tickets_df["Internal"] == 1
    ].sum()
    # movements with no factor at all
    no_factors_df = (
        no_factors_df.groupby(["ZoneCodeFrom", "ZoneCodeTo", "Internal"])["T_Demand"]
        .sum()
        .reset_index()
    )
    # get demand totals for movements where different ticket type was used
    demand_total_factors = no_factors_df["T_Demand"].sum()
    # demand total for internals
    demand_total_factors_internal = no_factors_df["T_Demand"][
        no_factors_df["Internal"] == 1
    ].sum()
    # check proportion of unfactored demand to total demand and other tickets demand to total demand
    #   as well as internal proportion of that demand
    tickets_internal_prop = round(demand_total_ticket_internal / demand_total_ticket * 100, 3)
    factors_internal_prop = round(
        demand_total_factors_internal / demand_total_factors * 100, 3
    )
    # total proportions
    no_factor_demand_prop = round(demand_total_factors / demand_total * 100, 3)
    tickets_demand_prop = round(demand_total_ticket / demand_total * 100, 3)
    # regroup dataframes for logging
    other_tickets_df = (
        other_tickets_df.groupby(
            [
                "ZoneCodeFrom",
                "ZoneCodeTo",
                "purpose",
                "Missing_TicketType",
                "Available_TicketType",
            ]
        )["T_Demand"]
        .sum()
        .reset_index()
    )
    no_factors_df = (
        no_factors_df.groupby(["ZoneCodeFrom", "ZoneCodeTo"])["T_Demand"].sum().reset_index()
    )

    return (
        other_tickets_df,
        no_factors_df,
        no_factor_demand_prop,
        tickets_demand_prop,
        tickets_internal_prop,
        factors_internal_prop,
    )


def sum_periods_demand(am, ip, pm, op):
    """
    Parameters
    ----------
    am : pandas dataframe
        demand matrix for the AM period
    ip : pandas dataframe
        demand matrix for the IP period
    pm : pandas dataframe
        demand matrix for the PM period
    op : pandas dataframe
        demand matrix for the OP period

    Function
    ---------
    sums time periods demand to 24Hr demand matrix

    Returns
    -------
    comb : pandas dataframe
        24Hr demand matrix
    """
    comb = am.merge(ip, how="outer", on=["from_model_zone_id", "to_model_zone_id"])
    comb = comb.merge(pm, how="outer", on=["from_model_zone_id", "to_model_zone_id"])
    comb = comb.merge(op, how="outer", on=["from_model_zone_id", "to_model_zone_id"])
    # fill nans with zeros
    comb = comb.fillna(0)
    # sum 24Hr demand
    comb["Demand"] = (
        comb["AM_Demand"] + comb["IP_Demand"] + comb["PM_Demand"] + comb["OP_Demand"]
    )
    # keep needed columns
    comb = comb[["from_model_zone_id", "to_model_zone_id", "Demand"]]

    return comb


def average_two_matrices(mx1, mx2, zones=1300):
    """
    Parameters
    ----------
    mx1 : pandas dataframe
        first matrix
    mx2 : pandas dataframe
        second matrix
    zones: int
        number of model zones, default = 1300
    Function
    ---------
    average two matrices while expanding to full matrix dimensions

    Returns
    -------
    mx : pandas dataframe
        averaged matrix
    """
    # create empty dataframe
    mx = pd.DataFrame(
        list(itertools.product(range(1, zones + 1), range(1, zones + 1))),
        columns=["from_model_zone_id", "to_model_zone_id"],
    )
    # get first matrix
    mx = mx.merge(mx1, how="outer", on=["from_model_zone_id", "to_model_zone_id"]).fillna(0)
    # get second matrix
    mx = mx.merge(mx2, how="outer", on=["from_model_zone_id", "to_model_zone_id"]).fillna(0)
    # sum demand
    mx["Demand"] = (mx["Demand_x"] + mx["Demand_y"]) / 2
    # keep needed columns
    mx = mx[["from_model_zone_id", "to_model_zone_id", "Demand"]].fillna(0)
    return mx


def expand_matrix(mx, zones=1300):
    """
    Parameters
    ----------
    mx : pandas dataframe
        matrix
    zones: int
        number of model zones, default = 1300
    Function
    ---------
    expand a matrix to the full dimensions

    Returns
    -------
    eMx : pandas dataframe
        expanded matrix
    """
    # create empty dataframe
    eMx = pd.DataFrame(
        list(itertools.product(range(1, zones + 1), range(1, zones + 1))),
        columns=["from_model_zone_id", "to_model_zone_id"],
    )
    # get first matrix
    eMx = eMx.merge(mx, how="outer", on=["from_model_zone_id", "to_model_zone_id"]).fillna(0)
    return eMx


def fromto_2_from_by_averaging(matrices_dict):
    """
    Parameters
    ----------
    matrices_dict : dictionary
        24Hr demand matrices dictionary
    Function
    ---------
    Function combines From/To by averaging the two directions to produce the 19
    segments needed by NoRMS

    Returns
    -------
    matrices : dictionary
        dictionary of matrices
    """
    # empty dictionary
    matrices = {}
    # HBEBCA_Int
    mx1 = average_two_matrices(
        matrices_dict["HBEBCA_Int"], transpose_matrix(matrices_dict["HBEBCA_Int_T"])
    )
    matrices["HBEBCA_Int"] = mx1
    # HBEBNCA_Int
    mx2 = average_two_matrices(
        matrices_dict["HBEBNCA_Int"], transpose_matrix(matrices_dict["HBEBNCA_Int_T"])
    )
    matrices["HBEBNCA_Int"] = mx2
    # NHBEBCA_Int
    mx3 = expand_matrix(matrices_dict["NHBEBCA_Int"])
    matrices["NHBEBCA_Int"] = mx3
    # NHBEBNCA_Int
    mx4 = expand_matrix(matrices_dict["NHBEBNCA_Int"])
    matrices["NHBEBNCA_Int"] = mx4
    # HBWCA_Int
    mx5 = average_two_matrices(
        matrices_dict["HBWCA_Int"], transpose_matrix(matrices_dict["HBWCA_Int_T"])
    )
    matrices["HBWCA_Int"] = mx5
    # HBWNCA_Int
    mx6 = average_two_matrices(
        matrices_dict["HBWNCA_Int"], transpose_matrix(matrices_dict["HBWNCA_Int_T"])
    )
    matrices["HBWNCA_Int"] = mx6
    # HBOCA_Int
    mx7 = average_two_matrices(
        matrices_dict["HBOCA_Int"], transpose_matrix(matrices_dict["HBOCA_Int_T"])
    )
    matrices["HBOCA_Int"] = mx7
    # HBONCA_Int
    mx8 = average_two_matrices(
        matrices_dict["HBONCA_Int"], transpose_matrix(matrices_dict["HBONCA_Int_T"])
    )
    matrices["HBONCA_Int"] = mx8
    # NHBOCA_Int
    mx9 = expand_matrix(matrices_dict["NHBOCA_Int"])
    matrices["NHBOCA_Int"] = mx9
    # NHBONCA_Int
    mx10 = expand_matrix(matrices_dict["NHBONCA_Int"])
    matrices["NHBONCA_Int"] = mx10
    # EBCA_Ext_FM
    mx11 = expand_matrix(matrices_dict["EBCA_Ext_FM"])
    matrices["EBCA_Ext_FM"] = mx11
    # EBCA_Ext_TO
    mx12 = expand_matrix(matrices_dict["EBCA_Ext_TO"])
    matrices["EBCA_Ext_TO"] = mx12
    # EBNCA_Ext
    mx13 = expand_matrix(matrices_dict["EBNCA_Ext"])
    matrices["EBNCA_Ext"] = mx13
    # HBWCA_Ext_FM
    mx14 = expand_matrix(matrices_dict["HBWCA_Ext_FM"])
    matrices["HBWCA_Ext_FM"] = mx14
    # HBWCA_Ext_TO
    mx15 = expand_matrix(matrices_dict["HBWCA_Ext_TO"])
    matrices["HBWCA_Ext_TO"] = mx15
    # HBWNCA_Ext
    mx16 = expand_matrix(matrices_dict["HBWNCA_Ext"])
    matrices["HBWNCA_Ext"] = mx16
    # OCA_Ext_FM
    mx17 = expand_matrix(matrices_dict["OCA_Ext_FM"])
    matrices["OCA_Ext_FM"] = mx17
    # OCA_Ext_TO
    mx18 = expand_matrix(matrices_dict["OCA_Ext_TO"])
    matrices["OCA_Ext_TO"] = mx18
    # ONCA_Ext
    mx19 = expand_matrix(matrices_dict["ONCA_Ext"])
    matrices["ONCA_Ext"] = mx19

    return matrices


def fromto_2_from_by_from(matrices_dict):
    """
    Parameters
    ----------
    matrices_dict : dictionary
        24Hr demand matrices dictionary
    Function
    ---------
    Function keeps the From home only when moving back to NoRMS segments for the
    internal From/To

    Returns
    -------
    matrices : dictionary
        dictionary of matrices
    """
    # empty dictionary
    matrices = {}
    # HBEBCA_Int
    mx1 = expand_matrix(matrices_dict["HBEBCA_Int"])
    matrices["HBEBCA_Int"] = mx1
    # HBEBNCA_Int
    mx2 = expand_matrix(matrices_dict["HBEBNCA_Int"])
    matrices["HBEBNCA_Int"] = mx2
    # NHBEBCA_Int
    mx3 = expand_matrix(matrices_dict["NHBEBCA_Int"])
    matrices["NHBEBCA_Int"] = mx3
    # NHBEBNCA_Int
    mx4 = expand_matrix(matrices_dict["NHBEBNCA_Int"])
    matrices["NHBEBNCA_Int"] = mx4
    # HBWCA_Int
    mx5 = expand_matrix(matrices_dict["HBWCA_Int"])
    matrices["HBWCA_Int"] = mx5
    # HBWNCA_Int
    mx6 = expand_matrix(matrices_dict["HBWNCA_Int"])
    matrices["HBWNCA_Int"] = mx6
    # HBOCA_Int
    mx7 = expand_matrix(matrices_dict["HBOCA_Int"])
    matrices["HBOCA_Int"] = mx7
    # HBONCA_Int
    mx8 = expand_matrix(matrices_dict["HBONCA_Int"])
    matrices["HBONCA_Int"] = mx8
    # NHBOCA_Int
    mx9 = expand_matrix(matrices_dict["NHBOCA_Int"])
    matrices["NHBOCA_Int"] = mx9
    # NHBONCA_Int
    mx10 = expand_matrix(matrices_dict["NHBONCA_Int"])
    matrices["NHBONCA_Int"] = mx10
    # EBCA_Ext_FM
    mx11 = expand_matrix(matrices_dict["EBCA_Ext_FM"])
    matrices["EBCA_Ext_FM"] = mx11
    # EBCA_Ext_TO
    mx12 = expand_matrix(matrices_dict["EBCA_Ext_TO"])
    matrices["EBCA_Ext_TO"] = mx12
    # EBNCA_Ext
    mx13 = expand_matrix(matrices_dict["EBNCA_Ext"])
    matrices["EBNCA_Ext"] = mx13
    # HBWCA_Ext_FM
    mx14 = expand_matrix(matrices_dict["HBWCA_Ext_FM"])
    matrices["HBWCA_Ext_FM"] = mx14
    # HBWCA_Ext_TO
    mx15 = expand_matrix(matrices_dict["HBWCA_Ext_TO"])
    matrices["HBWCA_Ext_TO"] = mx15
    # HBWNCA_Ext
    mx16 = expand_matrix(matrices_dict["HBWNCA_Ext"])
    matrices["HBWNCA_Ext"] = mx16
    # OCA_Ext_FM
    mx17 = expand_matrix(matrices_dict["OCA_Ext_FM"])
    matrices["OCA_Ext_FM"] = mx17
    # OCA_Ext_TO
    mx18 = expand_matrix(matrices_dict["OCA_Ext_TO"])
    matrices["OCA_Ext_TO"] = mx18
    # ONCA_Ext
    mx19 = expand_matrix(matrices_dict["ONCA_Ext"])
    matrices["ONCA_Ext"] = mx19

    return matrices


def transpose_matrix(mx):
    """
    Parameters
    ----------
    mx : pandas dataframe
        input matrix to transpose
    Function
    ---------
    Function Transposes the matrix O<>D/P<>A

    Returns
    -------
    mx : pandas dataframe
        transposed matrix
    """
    # transpose to-home PA to OD
    # assign to to temp column
    mx["from"] = mx["to_model_zone_id"]
    mx["to"] = mx["from_model_zone_id"]
    # transpose
    mx["from_model_zone_id"] = mx["from"]
    mx["to_model_zone_id"] = mx["to"]
    # drop columns
    mx = mx.drop(["from", "to"], axis=1)

    return mx


def run_edge_growth(params):
    # create new logfile
    if os.path.exists(f"{params.export_path}/EDGE_Factoring_{params.forecast_year}.Log"):
        os.remove(f"{params.export_path}/EDGE_Factoring_{params.forecast_year}.Log")
    logging.basicConfig(
        filename=f"{params.export_path}/EDGE_Factoring_{params.forecast_year}.Log",
        format="%(levelname)s:%(message)s",
        level=logging.INFO,
    )
    logging.info("#" * 80)
    logging.info("Started Process @ " + datetime.now().strftime("%d-%m-%Y,,,%H:%M:%S.%f"))
    logging.info("#" * 80)

    """Process Fixed objects"""
    periods = ["AM", "IP", "PM", "OP"]

    # ## READ INPUT FILES ## #
    # Custom input files
    segments_to_uc = pd.read_csv(params.segments_to_uc_path)
    ticket_type_splits = pd.read_csv(params.ticket_type_splits_path)
    flow_cats = pd.read_csv(params.flow_cat_path)
    norms_to_edge_stns = pd.read_csv(params.norms_to_edge_stns_path)

    # EDGE files
    edge_flows_file = pd.read_csv(params.edge_flows_path)
    edge_growth_factors = pd.read_csv(params.edge_factors_path)

    # Add Flag = 1 for all input factors in EDGE
    #    i.e. Flag = 1 if the factor comes directly from EDGE
    edge_growth_factors["Flag"] = 1

    # demand segment list groups
    # NoRMS demand segments
    norms_segments = [
        "HBEBCA_Int",
        "HBEBNCA_Int",
        "NHBEBCA_Int",
        "NHBEBNCA_Int",
        "HBWCA_Int",
        "HBWNCA_Int",
        "HBOCA_Int",
        "HBONCA_Int",
        "NHBOCA_Int",
        "NHBONCA_Int",
        "EBCA_Ext_FM",
        "EBCA_Ext_TO",
        "EBNCA_Ext",
        "HBWCA_Ext_FM",
        "HBWCA_Ext_TO",
        "HBWNCA_Ext",
        "OCA_Ext_FM",
        "OCA_Ext_TO",
        "ONCA_Ext",
    ]

    # these demand segments need to have the iRSj probabilities transposed
    internal_to_home = [
        "HBEBCA_Int_T",
        "HBEBNCA_Int_T",
        "NHBEBCA_Int_T",
        "NHBEBNCA_Int_T",
        "HBWCA_Int_T",
        "HBWNCA_Int_T",
        "HBOCA_Int_T",
        "HBONCA_Int_T",
        "NHBOCA_Int_T",
        "NHBONCA_Int_T",
    ]

    # below dictionary sets out the factoring method for each demand segment where:
    #           1: Apply P=O and A=D (i.e. PA factoring as it is)
    #           2: Apply Average of both directions
    segments_method = {
        "HBEBCA_Int": 1,
        "HBEBNCA_Int": 1,
        "NHBEBCA_Int": 2,
        "NHBEBNCA_Int": 2,
        "HBWCA_Int": 1,
        "HBWNCA_Int": 2,
        "HBOCA_Int": 1,
        "HBONCA_Int": 2,
        "NHBOCA_Int": 2,
        "NHBONCA_Int": 2,
        "HBEBCA_Int_T": 1,
        "HBEBNCA_Int_T": 1,
        "NHBEBCA_Int_T": 2,
        "NHBEBNCA_Int_T": 2,
        "HBWCA_Int_T": 1,
        "HBWNCA_Int_T": 2,
        "HBOCA_Int_T": 1,
        "HBONCA_Int_T": 2,
        "NHBOCA_Int_T": 2,
        "NHBONCA_Int_T": 2,
        "EBCA_Ext_FM": 1,
        "EBNCA_Ext": 2,
        "HBWCA_Ext_FM": 1,
        "HBWNCA_Ext": 2,
        "OCA_Ext_FM": 1,
        "ONCA_Ext": 2,
        "EBCA_Ext_TO": 1,
        "HBWCA_Ext_TO": 1,
        "OCA_Ext_TO": 1,
    }

    # get list of demand segments
    demand_segment_list = segments_to_uc["MX"].tolist()
    # demand_segment_list = ['HBEBCA_Int_T']

    # factored matricies dictionary
    factored_matrices = {}
    factored_24hr_matrices = {}

    # empty DFs for recording missing factors
    other_tickets_df = pd.DataFrame()
    no_factors_df = pd.DataFrame()

    # set demand total to 0
    demand_total = 0

    # loop over periods
    for period in tqdm(periods, desc="Time Periods Loop ", unit=" Period", colour="cyan"):
        logging.info(
            f'-- Processing Time Period {period} @ {datetime.now().strftime("%d-%m-%Y,,,%H:%M:%S.%f")}'
        )
        # read distance matrix
        check_file_exist(f"{params.matrices_to_grow_dir}/{period}_stn2stn_costs.csv")
        distMX = pd.read_csv(f"{params.matrices_to_grow_dir}/{period}_stn2stn_costs.csv")
        # read iRSj props
        check_file_exist(f"{params.matrices_to_grow_dir}/{period}_iRSj_probabilities.h5")
        iRSjProps = pd.read_hdf(
            f"{params.matrices_to_grow_dir}/{period}_iRSj_probabilities.h5", key="iRSj"
        )
        # period dictionary
        factored_matrices[period] = {}
        logging.info(
            f"Demand Segment           Base_Demand             {params.forecast_year}_Demand"
        )
        # loop over demand segments
        for segment in tqdm(
            demand_segment_list,
            desc="    Demand Segments Loop ",
            unit=" Segment",
            colour="cyan",
        ):

            # demand matrices
            check_file_exist(f"{params.matrices_to_grow_dir}/{period}_{segment}.csv")
            demandMX = pd.read_csv(f"{params.matrices_to_grow_dir}/{period}_{segment}.csv")
            tot_input_demand = round(demandMX["Demand"].sum())
            # sum total demand
            demand_total = demand_total + tot_input_demand
            # add UCs to demand based on demand segment
            demandMX["userclass"] = (
                segments_to_uc[segments_to_uc["MX"] == segment]
                .reset_index()
                .iloc[0]["userclass"]
            )
            # keep needed columns
            demandMX = demandMX[
                ["from_model_zone_id", "to_model_zone_id", "userclass", "Demand"]
            ]
            # keep non-zero demand records
            demandMX = demandMX.loc[demandMX["Demand"] > 0].reset_index(drop=True)
            # prepare demand matrix
            if segment in internal_to_home:
                demandMX = prepare_stn2stn_matrix_tohome(demandMX, iRSjProps, distMX, norms_to_edge_stns)
            else:
                demandMX = prepare_stn2stn_matrix(demandMX, iRSjProps, distMX, norms_to_edge_stns)
            # assign EDGE flows
            demandMX = assign_edge_flow(edge_flows_file, flow_cats, demandMX)
            # add TAG flows
            demandMX = add_distance_band_tag_flow(demandMX)
            # add prupsoes to matrix
            demandMX = assign_purposes(demandMX)
            # add ticket splits props
            demandMX = demandMX.merge(
                ticket_type_splits, how="left", on=["TAG_Flow", "Purpose"]
            )
            # apply Ticket Splits
            demandMX = apply_ticket_splits(demandMX)
            # Get factors for missing movements if any
            (
                edge_growth_factors,
                other_tickets_df,
                no_factors_df,
            ) = create_factors_for_missing_moira_movements(
                demandMX, edge_growth_factors, other_tickets_df, no_factors_df
            )
            # get factoring method
            method = segments_method[segment]
            # apply factoring based on demand segment
            if method == 1:
                demandMX = apply_edge_growth_method1(demandMX, edge_growth_factors)
            else:
                demandMX = apply_edge_growth_method2(demandMX, edge_growth_factors)

            # move back to zone2zone matrix
            demandMX = (
                demandMX.groupby(["from_model_zone_id", "to_model_zone_id"])[
                    ["T_Demand", "N_Demand"]
                ]
                .sum()
                .reset_index()
            )
            tot_output_demand = round(demandMX["N_Demand"].sum())
            logging.info(
                f"{segment}                 {tot_input_demand}                   {tot_output_demand}"
            )

            # ammend forecast matrix to main dictionary
            demandMX = demandMX[["from_model_zone_id", "to_model_zone_id", "N_Demand"]]
            demandMX = demandMX.rename(columns={"N_Demand": f"{period}_Demand"})
            factored_matrices[period][segment] = demandMX

    # get logging stats
    (
        other_tickets_df,
        no_factors_df,
        no_factor_demand_prop,
        tickets_demand_prop,
        tickets_internal_prop,
        factors_internal_prop,
    ) = prepare_logging_info(other_tickets_df, no_factors_df, demand_total)

    # if the proportion of the demand that has no factor at all in EDGE exceeds 1%
    #        then report these movements and quit the program
    #        user MUST look into these movements and check why these have no factor
    #        and act accordingly
    if no_factor_demand_prop > 1:
        logging.warning(
            f"          Demand with no factors  = {no_factor_demand_prop}% exceeding the 1% threshold of the total demand hence the process terminated"
        )
        logging.warning("           Table Below lists all movements with no factors:")
        logging.warning("          {}".format(no_factors_df.to_string(index=False)))
        logging.info(
            "Process was interrupted @ " + datetime.now().strftime("%d-%m-%Y,,,%H:%M:%S.%f")
        )
        print("Process was interrupted - Check the logfile for more details", "red")
        # quit
        sys.exit()

    # else: keep records with no factor as they are (i.e. no growth) and continue with
    #       writing out the mtrices but still report where factors for other ticket
    #       types have been used as well as movements with no factors at all
    else:
        logging.info(
            "          Records below have missing factors for -Missing_TicketType- and therefore growth factors for"
        )
        logging.info("          Tickets from Availabkle_TicektType- have been used")
        logging.info(
            f"          Total demand proportion for these movements = {tickets_demand_prop}% of which {tickets_internal_prop}% is Internal"
        )
        logging.info("          -----------------------------------")
        logging.info("{}".format(other_tickets_df.to_string(index=False)))
        # log info
        logging.warning(
            "          Records below have no factors at all for these movements hence no growth have been applied:"
        )
        logging.warning(
            f"          Total demand proportion for these movements = {no_factor_demand_prop}% of which {factors_internal_prop}% is Internal"
        )
        logging.warning("          -----------------------------------")
        logging.warning("{}".format(no_factors_df.to_string(index=False)))

        # write out matrices
        for segment in segments_method:
            # get demand for each period
            am = factored_matrices["AM"][segment]
            ip = factored_matrices["IP"][segment]
            pm = factored_matrices["PM"][segment]
            op = factored_matrices["OP"][segment]
            # get 24Hr demand amtrix
            demandMX = sum_periods_demand(am, ip, pm, op)
            # add to 24Hr matrices dict
            factored_24hr_matrices[segment] = demandMX

        # Combine matrices into NoRMS segments
        norms_matrices1 = fromto_2_from_by_averaging(factored_24hr_matrices)
        # norms_matrices2 = pFunc.FromTo2FromByFrom(factored_24Hr_matrices)
        # plot matrices
        for segment in norms_segments:
            # write out demand matrix
            norms_matrices1[segment].to_csv(
                f"{params.export_path}/{params.forecast_year}_24Hr_{segment}.csv", index=False
            )
            # norms_matrices2[segment].to_csv(f'{export_path}/{forecast_year}/{forecast_year}_24Hr_{segment}.csv', index=False)

        print("Process finished successfully!", "green")
        logging.info(
            "Process finished successfully @ "
            + datetime.now().strftime("%d-%m-%Y,,,%H:%M:%S.%f")
        )
