package PlotGeneration

import PlotGeneration.Core.PlotGenerator

/**
 * Entry point for `PlotGeneration`.
 *
 * Delegates to `PlotGeneration.Core.PlotGenerator.generate()` which runs the
 * end-to-end pipeline (`Stations`, `Climograph`, `SingleParamStudies`, `InterestingStudies`),
 * reading precomputed `JSON`, formatting `DTO`s and `POST`ing them to the plotting service,
 * while logging progress and measuring elapsed time.
 *
 */
object Main extends App {
  PlotGenerator.generate()
}
