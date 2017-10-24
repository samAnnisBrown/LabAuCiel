import json

import yaml


def update_config_item(key, value):
    with open("core/resources/config.yaml") as file:
        config_yaml = yaml.load(file)

    if key in config_yaml:
        config_yaml[key] = value

    with open("core/resources/config.yaml", "w") as file:
        yaml.dump(config_yaml, file, default_flow_style=False)

    return 1


def add_config_item(key, value):
    with open("core/resources/config.yaml") as file:
        config_yaml = yaml.load(file)

    if key in config_yaml:
        return "Key Exists"

    config_yaml[key] = value

    with open("core/resources/config.yaml", "w") as file:
        yaml.dump(config_yaml, file, default_flow_style=False)

    return 1


def get_config_item(key):
    with open("core/resources/config.yaml") as file:
        config_yaml = yaml.load(file)

    if key in config_yaml:
        return config_yaml[key]
    else:
        return "Key doesn't exist."


def get_region_shortname(region):
    with open('core/resources/regions.json') as region_file:
        regions = json.load(region_file)

    for r in regions['Regions']:
        if r['RegionName'] == region:
            return r['RegionShortName']

    return None


def get_region_friendlyname(region):
    with open('core/resources/regions.json') as region_file:
        regions = json.load(region_file)

    for r in regions['Regions']:
        if r['RegionName'] == region:
            return r['RegionFriendlyName']

    return None