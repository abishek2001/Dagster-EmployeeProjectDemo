from dagster import load_assets_from_package_module, repository

from etl import assets


@repository
def etl():
    return [load_assets_from_package_module(assets)]
