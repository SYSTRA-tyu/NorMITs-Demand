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
  - Multi-Area Gravity Model
    - Initial implementation of a multi-area gravity model. Each area calibrates
      its own cost params, and aims for its own target cost distribution. All
      areas share the same Furness and Jacobian matrices via threading. 
- Concurrency
  - Multi-threading framework added to make multi-threading simpler in codebase
  - `SharedNumpyArrayHelper` added to make communication of large numpy 
    arrays between threads/processes faster and easier, at the cost of memory.  
