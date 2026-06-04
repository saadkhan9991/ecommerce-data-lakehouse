"""
Dagster Definitions
====================
The entry point Dagster looks for. Lists every asset, schedule, sensor,
and resource the project exposes.

When you run `dagster dev`, Dagster imports this file and reads `defs`.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import bronze

# Auto-discover every @asset defined in the bronze module
all_assets = load_assets_from_modules([bronze])

defs = Definitions(
    assets=all_assets,
)