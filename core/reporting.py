import json

import dateutil

from .ddb import scan_items_reverse
from .config import get_region_shortname


def report_all():
    # Gather Inputs
    with open('core/resources/regions.json') as region_file:
        regions = json.load(region_file)

    input = scan_items_reverse()

    # -------- Region Graph --------
    region_y = []
    region_x = []
    r_counter = 0

    for region in regions['Regions']:
        region_x.append(region['RegionShortName'])
        for i in range(input.__len__()):
            if region['RegionName'] == input[i]['Region']:
                r_counter += 1
        region_y.append(r_counter)
        r_counter = 0

    # -------- Cost Graph --------
    cost_x = []
    cost_y = []
    y_loc = -1

    for i in range(input.__len__()):
        intime = dateutil.parser.parse(input[i].get('StartTime'))
        timestr = str(intime.strftime('%b') + " " + str(intime.day))
        cost = input[i].get('Cost')

        if timestr not in cost_x:
            cost_x.append(timestr)
            y_loc = y_loc + 1
            cost_y.append(0)

        if cost is not None:
            cost_y[y_loc] = round(cost_y[y_loc] + float(cost), 2)

    # -------- Total Cost --------
    totalcost = 0

    for i in range(input.__len__()):
        cost = input[i].get('Cost')
        if cost is not None:
            totalcost = totalcost + float(cost)

    # --------  Cost Per Region --------
    reg_costs_dict = dict()
    region_cost_x = []
    region_cost_y = []

    # Build Dictionary
    for i in range(input.__len__()):
        region = input[i].get('Region')
        if region not in reg_costs_dict:
            reg_costs_dict.update({region:0})

    # Add Region Cost
    regcost = 0
    for key, value in reg_costs_dict.items():
        for i in range(input.__len__()):
            if input[i]['Region'] == key:
                regcost = regcost + float(input[i].get('Cost'))
        reg_costs_dict[key] = regcost
        region_cost_x.append(get_region_shortname(key))
        region_cost_y.append(round(regcost, 2))
        regcost = 0

    # -------- Instance Graph --------
    inst_x = []
    inst_y = []
    i_counter = 0

    # Build Dictionary
    for i in range(input.__len__()):
        instance = input[i].get('InstanceSize')
        if instance not in inst_x:
            inst_x.append(instance)

    for instance in inst_x:
        for i in range(input.__len__()):
            if instance == input[i]['InstanceSize']:
                i_counter += 1
        inst_y.append(i_counter)
        i_counter = 0

    return json.dumps(region_x), json.dumps(region_y), json.dumps(cost_x), json.dumps(cost_y), json.dumps(region_cost_x), json.dumps(region_cost_y), json.dumps(inst_x), json.dumps(inst_y), json.dumps(round(totalcost, 2))


def report_region_frequency():
    with open('core/resources/regions.json') as region_file:
        regions = json.load(region_file)

    input = scan_items_reverse()
    output = []
    counter = 0

    for region in regions['Regions']:
        for i in range(input.__len__()):
            if region['RegionName'] == input[i]['Region']:
                counter += 1
        output.append(counter)
        counter = 0

    return json.dumps(output)


def report_cost():
    input = scan_items_reverse()
    x_axis = []
    y_axis = []
    y_loc = -1

    for i in range(input.__len__()):
        intime = dateutil.parser.parse(input[i].get('StartTime'))
        timestr = str(intime.strftime('%b') + " " + str(intime.day))
        cost = input[i].get('Cost')

        if timestr not in x_axis:
            x_axis.append(timestr)
            y_loc = y_loc + 1
            y_axis.append(0)

        if cost is not None:
            y_axis[y_loc] = round(y_axis[y_loc] + float(cost), 2)

    return json.dumps(x_axis), json.dumps(y_axis)


def report_cost_per_region():

    input = scan_items_reverse()
    dictionary = dict()
    region_cost_x = []
    region_cost_y = []

    # Build Dictionary
    for i in range(input.__len__()):
        region = input[i].get('Region')
        if region not in dictionary:
            dictionary.update({region:0})

    # Add Region Cost
    regcost = 0
    for key, value in dictionary.items():
        for i in range(input.__len__()):
            if input[i]['Region'] == key:
                regcost = regcost + float(input[i].get('Cost'))
        dictionary[key] = regcost
        region_cost_x.append(get_region_shortname(key))
        region_cost_y.append(round(regcost, 2))
        regcost = 0

    return json.dumps(region_cost_x), json.dumps(region_cost_y)


def report_total_cost():
    input = scan_items_reverse()
    totalcost = 0

    for i in range(input.__len__()):
        cost = input[i].get('Cost')
        if cost is not None:
            totalcost = totalcost + float(cost)

    return round(totalcost, 2)


def report_instance_sizes():
    with open('core/resources/instances.json') as instance_file:
        instances = json.load(instance_file)

    input = scan_items_reverse()
    inst_x = []
    inst_y = []
    i_counter = 0

    for instance in instances['Instances']:
        inst_x.append(instance['InstanceName'])
        for i in range(input.__len__()):
            if instance['InstanceName'] == input[i]['InstanceSize']:
                i_counter += 1
        inst_y.append(i_counter)
        i_counter = 0

    return json.dumps(inst_x), json.dumps(inst_y)
