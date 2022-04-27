# -*- coding: utf-8 -*-
"""
    Module for running the NTEM forecast.
"""

##### IMPORTS #####
from __future__ import annotations

# Standard imports
import configparser
import dataclasses
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Third party imports

# Add parent folder to path
sys.path.append("..")
# Local imports
# pylint: disable=import-error,wrong-import-position
from normits_demand import efs_constants as efs_consts
from normits_demand import logging as nd_log
from normits_demand.models import ntem_forecast, tempro_trip_ends
from normits_demand.reports import ntem_forecast_checks
from normits_demand.utils import timing

# pylint: enable=import-error,wrong-import-position

##### CONSTANTS #####
LOG_FILE = "NTEM_forecast.log"
LOG = nd_log.get_logger(nd_log.get_package_logger_name() + ".run_models.run_ntem_forecast")


##### CLASSES #####
@dataclasses.dataclass(repr=False)
class NTEMForecastParameters:
    """Class for storing the parameters for running NTEM forecasting.

    Attributes
    ----------
    import_path : Path
        Path to the NorMITs demand imports.
    tempro_data_path : Path
        Path to the CSV containing the TEMPro data.
    model_name : str
        Name of the model.
    iteration : str
        Iteration number.
    base_year : int
        Base year for the model.
    future_years: list[int]
        All future years to run the model for.
    export_path_fmt: str
        Format for the export path, used for building
        `export_path` property.
    export_path_params : Dict[str, Any]
        Dictionary containing any additional parameters
        for building the `export_path`.
    export_path : Path
        Read-only path to export folder, this is built from
        the `export_path_fmt` with variables filled in from
        the class attributes, and additional optional values
        from `export_path_params`.
    """

    import_path: Path
    tempro_data_path: Path
    model_name: str
    iteration: str
    base_year: int
    future_years: list[int]
    export_path_fmt: str = "I:/NorMITs Demand/{model_name}/NTEM/iter{iteration}"
    export_path_params: Optional[Dict[str, Any]] = None
    _export_path: Optional[Path] = dataclasses.field(default=None, init=False, repr=False)

    @property
    def export_path(self) -> Path:
        """
        Read-only path to export folder, this is built from
        the `export_path_fmt` with variables filled in from
        the class attributes, and additional optional values
        from `export_path_params`.
        """
        if self._export_path is None:
            fmt_params = dataclasses.asdict(self)
            if self.export_path_params is not None:
                fmt_params.update(self.export_path_params)
            self._export_path = Path(self.export_path_fmt.format(**fmt_params))
        return self._export_path

    def __repr__(self) -> str:
        params = (
            "import_path",
            "tempro_data_path",
            "export_path",
            "model_name",
            "iteration",
            "car_occupancies_path",
            "base_year",
            "future_years",
        )
        msg = f"{self.__class__.__name__}("
        for p in params:
            msg += f"\n\t{p}={getattr(self, p)!s}"
        for p, val in self.pa_to_od_factors.items():
            msg += f"\n\t{p}={val!s}"
        msg += "\n)"
        return msg

    @property
    def pa_to_od_factors(self) -> Dict[str, Path]:
        """Dict[str, Path]
        Paths to the PA to OD tour proportions, has
        keys `post_me_tours` and `post_me_fh_th_factors`.
        """
        tour_prop_home = self.import_path / self.model_name / "post_me_tour_proportions"
        paths = {
            "post_me_tours": tour_prop_home,
            "post_me_fh_th_factors": tour_prop_home / "fh_th_factors",
        }
        for nm, p in paths.items():
            if not p.is_dir():
                raise NotADirectoryError(f"cannot find {nm} folder: {p}")
        return paths

    @property
    def car_occupancies_path(self) -> Path:
        """Path
        Path to the vehicle occupancies CSV file.
        """
        path = self.import_path / "vehicle_occupancies/car_vehicle_occupancies.csv"
        if not path.exists():
            raise FileNotFoundError(f"cannot find vehicle occupancies CSV: {path}")
        return path

    def save(self, output_path: Optional[Path] = None):
        """Save current parameters to config file.

        Parameters
        ----------
        output_path : Path, optional
            Path to the file to save, if not given saves to
            'NTEM_forecast_parameters.txt' in `export_folder`.
        """
        if output_path is None:
            output_path = self.export_path / "NTEM_forecast_parameters.txt"
        config = configparser.ConfigParser()
        config["parameters"] = {
            "import_path": self.import_path,
            "tempro_data_path": self.tempro_data_path,
            "model_name": self.model_name,
            "iteration": self.iteration,
            "base_year": self.base_year,
            "future_years": " ".join(str(i) for i in self.future_years),
            "export_path_fmt": self.export_path_fmt,
        }
        if self.export_path_params is not None:
            config["export_path_params"] = self.export_path_params
        with open(output_path, "wt") as f:
            config.write(f)
        LOG.info("Written input parameters to: %s", output_path)

    @staticmethod
    def load(path: Path) -> NTEMForecastParameters:
        """Load parameters from config file.

        Parameters
        ----------
        path : Path
            Path to config file.

        Returns
        -------
        NTEMForecastParameters
            New instance of the class with all parameters
            filled in from the config file.

        Raises
        ------
        FileNotFoundError
            If `path` isn't a file.
        tempro_trip_ends.NTEMForecastError
            If the config file doesn't contain a
            'parameters' section.
        """
        if not path.is_file():
            raise FileNotFoundError(f"cannot find config file: {path}")

        config = configparser.ConfigParser()
        config.read(path)
        param_sec = "parameters"
        if not config.has_section(param_sec):
            raise tempro_trip_ends.NTEMForecastError(
                f"config file doesn't contain '{param_sec}' section"
            )

        params = dict(config[param_sec])
        for p in ("import_path", "tempro_data_path"):
            if p in params:
                params[p] = Path(params[p])

        params["base_year"] = int(params["base_year"])
        params["future_years"] = [int(i.strip()) for i in params["future_years".split(" ")]]

        export_params = "export_path_params"
        if config.has_section(export_params):
            params[export_params] = dict(config[export_params])

        return NTEMForecastParameters(**params)


##### FUNCTIONS #####
def get_tempro_data(data_path: Path, years: list[int]) -> tempro_trip_ends.TEMProTripEnds:
    """Read TEMPro data and convert it to DVectors.

    Parameters
    ----------
    data_path : Path
        Path to TEMPro data CSV.
    years : list[int]
        List of year columns to read from input file,
        should include base and forecast years.

    Returns
    -------
    tempro_trip_ends.TEMProTripEnds
        TEMPro trip end data as DVectors stored in class
        attributes for base and all future years.
    """
    tempro_data = tempro_trip_ends.TEMProData(data_path, years)
    # Read data and convert to DVectors
    trip_ends = tempro_data.produce_dvectors()
    # Aggregate DVector to required segmentation
    segmentation = ntem_forecast.NTEMImportMatrices.SEGMENTATION
    return trip_ends.aggregate(
        {
            "hb_attractions": segmentation["hb"],
            "hb_productions": segmentation["hb"],
            "nhb_attractions": segmentation["nhb"],
            "nhb_productions": segmentation["nhb"],
        }
    )


def model_mode_subset(
    trip_ends: tempro_trip_ends.TEMProTripEnds,
    model_name: str,
) -> tempro_trip_ends.TEMProTripEnds:
    """Get subset of `trip_ends` segmentation for specific `model_name`.

    Parameters
    ----------
    trip_ends : tempro_trip_ends.TEMProTripEnds
        Trip end data, which has segmentation split by
        mode.
    model_name : str
        Name of the model being ran, currently only
        works for 'noham'.

    Returns
    -------
    tempro_trip_ends.TEMProTripEnds
        Trip end data at new segmentation.

    Raises
    ------
    NotImplementedError
        If any `model_name` other than 'noham' is
        given.
    """
    model_name = model_name.lower().strip()
    if model_name == "noham":
        segmentation = {
            "hb_attractions": "hb_p_m_car",
            "hb_productions": "hb_p_m_car",
            "nhb_attractions": "nhb_p_m_car",
            "nhb_productions": "nhb_p_m_car",
        }
    else:
        raise NotImplementedError(
            f"NTEM forecasting only not implemented for model {model_name!r}"
        )
    return trip_ends.subset(segmentation)


def main(params: NTEMForecastParameters, init_logger: bool = True):
    """Main function for running the NTEM forecasting.

    Parameters
    ----------
    params : NTEMForecastParameters
        Parameters for running NTEM forecasting.
    init_logger : bool, default True
        If True initialises logger with log file
        in the export folder.

    See Also
    --------
    normits_demand.models.ntem_forecast
    """
    start = timing.current_milli_time()
    if params.export_path.exists():
        msg = "export folder already exists: %s"
    else:
        params.export_path.mkdir(parents=True)
        msg = "created export folder: %s"
    if init_logger:
        # Add log file output to main package logger
        nd_log.get_logger(
            nd_log.get_package_logger_name(),
            params.export_path / LOG_FILE,
            "Running NTEM forecast",
        )
    LOG.info(msg, params.export_path)
    LOG.info("Input parameters: %r", params)
    params.save()

    tempro_data = get_tempro_data(
        params.tempro_data_path, [params.base_year, *params.future_years]
    )
    tempro_data = model_mode_subset(tempro_data, params.model_name)
    tempro_growth = ntem_forecast.tempro_growth(tempro_data, params.model_name)
    tempro_growth.save(params.export_path / "TEMPro Growth Factors")

    ntem_inputs = ntem_forecast.NTEMImportMatrices(
        params.import_path,
        params.base_year,
        params.model_name,
    )
    pa_folder = params.export_path / "Matrices" / "PA"
    ntem_forecast.grow_all_matrices(ntem_inputs, tempro_growth, pa_folder)
    ntem_forecast_checks.pa_matrix_comparison(ntem_inputs, pa_folder, tempro_data)
    od_folder = pa_folder.with_name("OD")
    ntem_forecast.convert_to_od(
        pa_folder,
        od_folder,
        params.future_years,
        [ntem_inputs.mode],
        {
            "hb": efs_consts.HB_PURPOSES_NEEDED,
            "nhb": efs_consts.NHB_PURPOSES_NEEDED,
        },
        params.model_name,
        params.pa_to_od_factors,
    )

    # Compile to output formats
    ntem_forecast.compile_noham_for_norms(pa_folder, params.future_years)
    compiled_od_path = ntem_forecast.compile_noham(
        od_folder,
        params.future_years,
        params.car_occupancies_path,
    )
    ntem_forecast_checks.od_matrix_comparison(
        ntem_inputs.od_matrix_folder,
        compiled_od_path / "PCU",
        params.model_name,
        ntem_forecast_checks.COMPARISON_ZONE_SYSTEMS["matrix 1"],
    )

    LOG.info(
        "NTEM forecasting completed in %s",
        timing.time_taken(start, timing.current_milli_time()),
    )


##### MAIN #####
if __name__ == "__main__":
    try:
        ntem_parameters = NTEMForecastParameters(
            import_path=Path("I:/NorMITs Demand/import"),
            tempro_data_path=Path(r"I:\Data\TEMPRO\tempro_pa_data_6_mode_exc_mode_4.csv"),
            model_name="noham",
            iteration="1d",
            base_year=2018,
            future_years=[2033],
        )
        main(ntem_parameters)

    except Exception as err:
        LOG.critical("NTEM forecasting error:", exc_info=True)
        raise
