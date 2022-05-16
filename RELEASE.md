# Release Notes

The NorMITs Demand codebase follows [Semantic Versioning](https://semver.org/); the convention
for most software products. In Summary this means the version numbers should be read in the
following way.

Given a version number MAJOR.MINOR.PATCH (e.g. 1.0.0), increment the:

- MAJOR version when you make incompatible API changes,
- MINOR version when you add functionality in a backwards compatible manner, and
- PATCH version when you make backwards compatible bug fixes.

Note that the master branch of this repository contains a work in progress, and  may **not**
contain a stable version of the codebase. We aim to keep the master branch stable, but for the
most stable versions, please see the
[releases](https://github.com/Transport-for-the-North/NorMITs-Demand/releases)
page on GitHub. A log of all patches made between versions can also be found
there.

Below, a brief summary of patches made since the previous version can be found.

### V0.5.0
- Core
  - Added 'save()' and 'load()' functions (to remove implicit pandas
    dependencies when using pickles) to:
    - DVector
    - SegmentationLevel
    - ZoningSystem
  - Added Enumerations for scenarios
- NoTEM
  - Updated Attraction Model to accept a new form of Land Use data and attraction
    trip weights. This should lead to more accurate attraction trip ends.
  - Updated the Tram Model to balance at different zoning systems for different
    modes - similar to how NoTEM now balances bus trips.
  - Applied a fix to the tram model where negative train trips were being
    predicted before balancing.
  - Applied a fix to the tram model where output attractions always had a 
    tiny infill.
- Updated NoTEM and Distribution Model to read in DVectors using the new
  `Dvector.load()` method. This makes loads faster and safer.
- Distribution Model
  - Reporting
    - Automatically generates vector reports on the production and attraction
      vectors generated when converting the upper model outputs for the
      lower model.
    - Updated the gravity model reports output format. More standardised.
    - Added optional cost naming to make reports more flexible in names.
  - Multi-Area Gravity Model
    - Initial implementation of a multi-area gravity model. Each area calibrates
      its own cost params, and aims for its own target cost distribution. All
      areas share the same Furness and Jacobian matrices via threading.
  - Added a built-in cache method for trip end inputs
  - If the gravity model fails to converge, and option has now been added
    to allow it to run again using default parameters. This often solves
    most problems.
- Concurrency
  - Multi-threading framework added to make multi-threading simpler in codebase
  - `SharedNumpyArrayHelper` added to make communication of large numpy 
    arrays between threads/processes faster and easier, at the cost of memory
- Converters
  - Added a converter module to convert I/O between Demand Models and for
    inputs and outputs into NorMITs Demand
  - Added classes to convert demand from NoTEM/Tram Models into Distribution 
    Model.
- Tools
  - Updated the Trip Length Distribution (TLD) code to become a tool for 
    generating TLDs for Demand Models
  - Created a NoRMS output converter tool which takes outputs from NoRMS
    CUBE model and uses them to create OD matrices which can be used by
    other Analytical Framework tools. Note that this is a work in progress
    and isn't a fully developed tool yet. It requires better integration with
    NoRMS.
- Bug Fixes
  - Updated the TfGM translations and zoning definitions
  - Fixed some overflow handling in the `doubly_contstrained_furness()`
  - Clip non-zero values being returned from cost functions to prevent overflow
    errors in the gravity model
  - Fixed a bug where a DVector would fail to initialise when a zoning_system
    wasn't given
  - Fixed a bug where the cost matrices were being replaces with 0s in the lower
    gravity model
  - Allow some reporting in the distribution model to be optional when the
    translation files don't exist
  - Fixed an infill 0 bug in `DVector.balance_at_segments()`
  - Fixed a divide by 0 bug in `DVector.split_segment_like()`
  - Added error checking into cost read in. Will now report if/where np.nan 
    values have been found
  - Fixed an issue with PA to OD conversion where not all arguments were being
    passed in
