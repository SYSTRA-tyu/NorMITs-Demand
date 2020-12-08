# -*- coding: utf-8 -*-
"""
    Module containing the utility functions for the elasticity calculations.
"""

##### IMPORTS #####
# Standard imports
from pathlib import Path
from typing import Union, Dict, List

# Third party imports
import numpy as np
import pandas as pd

# Local imports
from demand_utilities.utils import safe_read_csv


##### FUNCTIONS #####
def read_segments_file(path: Path) -> pd.DataFrame:
    """Read the segments CSV file containing all the TfN segmentation info.

    Parameters
    ----------
    path : Path
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        The TfN segment information to be used for the
        elasticity calculations.
    """
    dtypes = {
        "EFS_Seg": "int16",
        "EFS_PurpBase": str,
        "EFS_MainPurp": str,
        "EFS_SubPurp": str,
        "EFS_SubPurpID": "int8",
        "EFS_TimePeriod": str,
        "EFS_SkillLevel": "float16",
        "EFS_IncLevel": "float16",
        "Elast_Purp": str,
        "Elast_MarketShare": str,
    }
    return safe_read_csv(path, dtype=dtypes, usecols=dtypes.keys())


def read_elasticity_file(
    data: Union[Path, pd.DataFrame],
    elasticity_type: str = None,
    purpose: str = None,
    market_share: str = None,
) -> pd.DataFrame:
    """Reads and filters the elasticity file.

    Can be given a DataFrame instead of a path for just filtering.

    Parameters
    ----------
    data : Union[Path, pd.DataFrame]
        Path to the elasticity file or a DataFrame containing the information.
    elasticity_type : str
        Value to filter on the 'ElasticityType' column, if None (default) then
        does not filter on this column.
    purpose : str
        Value to filter on the 'Purp' column, if None (default) then
        does not filter on this column.
    market_share : str
        Value to filter on the 'MarketShare' column, if None (default) then
        does not filter on this column.

    Returns
    -------
    pd.DataFrame
        The filtered elasticity data.

    Raises
    ------
    ValueError
        If the combination of filters leads to no remaining rows in
        the DataFrame.
    """
    dtypes = {
        "ElasticityType": str,
        "Purp": str,
        "MarketShare": str,
        "AffectedMode": str,
        "ModeCostChg": str,
        "OwnElast": "float32",
        "CstrMatrixName": str,
    }
    if isinstance(data, pd.DataFrame):
        df = data.copy()[dtypes.keys()]
    else:
        df = safe_read_csv(data, dtype=dtypes, usecols=dtypes.keys())

    # Filter required values
    for col, val in [
        ("ElasticityType", elasticity_type),
        ("Purp", purpose),
        ("MarketShare", market_share),
    ]:
        if val is None:
            continue
        df = df.loc[df[col] == val]
        if df.empty:
            raise ValueError(f"Value '{val}' not found in column '{col}'")

    return df


def get_constraint_matrices(
    folder: Path, get_files: List[str] = None
) -> Dict[str, Union[Path, np.array]]:
    """Search the given folder for any CSV files.

    Parameters
    ----------
    folder : Path
        Folder containing the constraint matrices as CSV files.
    get_files : List[str], optional
        The names of the matrices to read, if None (default) then
        the paths of all CSVs found will be returned.

    Returns
    -------
    Dict[str, Union[Path, np.array]]
        The lowercase name of the file and the absolute path to it
        (if get_files is None). If get_files is a list of names then
        those files will be read and returned.

    Raises
    ------
    FileNotFoundError
        If the folder given doesn't exist or isn't a folder.
    """
    matrices = {}
    if not folder.is_dir():
        raise FileNotFoundError(f"Not a folder, or doesn't exist: {folder}")
    get_files = [i.lower() for i in get_files] if get_files is not None else get_files

    for path in folder.iterdir():
        if path.suffix.lower() != ".csv":
            continue
        nm = path.stem.lower()
        if get_files is None:
            matrices[nm] = path.absolute()
        elif nm in get_files:
            matrices[nm] = np.loadtxt(path)
    return matrices
