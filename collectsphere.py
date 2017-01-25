"""
This is the code that needs to be integrated into collectd when run in
production. It contains the python code that integrates into the python module
for collectd. It connects to one or more vCenter Servers and gathers the configured
metrics from ESXi hosts and Virtual Machines.

The file is organized in multiple sections. The first section implements the
callback functions executed be collectd which is followed be a couple of helper
functions that separate out some code to make the rest more readable. The
helper classes section provides threads that are used to parallelize things and
make the plugin a lot faster.
"""
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import datetime
import re
import ssl
import time

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

import collectd


################################################################################
# CONFIGURE ME
################################################################################
INTERVAL = 300

################################################################################
# DO NOT CHANGE BEYOND THIS POINT!
################################################################################
CONFIGS = []  # Stores the configuration as passed from collectd
ENVIRONMENT = {}  # Runtime data and object cache


################################################################################
# IMPLEMENTATION OF COLLECTD CALLBACK FUNCTIONS
################################################################################

def convert_folder_tree_to_list(folder_tree):
    result = list()
    for leaf in folder_tree:
        if leaf._wsdlName == "Folder":
            result.extend(convert_folder_tree_to_list(leaf.childEntity))
        else:
            result.append(leaf)
    return result

def configure_callback(conf):
    """Receive configuration block. This is called by collectd for every
    configuration block it finds for this module."""

    # Set some sensible default values
    name = None
    host = None
    port = 443
    verbose = None
    verify_cert = None
    username = 'root'
    password = 'vmware'
    host_counters = []
    vm_counters = []
    inventory_refresh_interval = 600

    for node in conf.children:
        key = node.key.lower()
        val = node.values

        if key == 'name':
            name = val[0]
        elif key == 'host':
            host = val[0]
        elif key == 'port':
            port = int(val[0])
        elif key == 'verbose':
            verbose = bool(val)
        elif key == 'verifycertificate':
            verify_cert = bool(val)
        elif key == 'username':
            username = val[0]
        elif key == 'password':
            password = val[0]
        elif key == 'host_counters':
            str = val[0]
            if not str == "all":
                values = str.split(',')
                for value in values:
                    if len(value) > 0:
                        host_counters.append(value.strip())
        elif key == 'vm_counters':
            str = val[0]
            if not str == "all":
                values = str.split(',')
                for value in values:
                    if len(value) > 0:
                        vm_counters.append(value.strip())
        elif key == 'inventory_refresh_interval':
            inventory_refresh_interval = int(val[0])
        else:
            collectd.warning('collectsphere plugin: Unknown config key: %s.'
                             % key)
            continue

    collectd.info(
        'configure_callback: Loaded config: name=%s, host=%s, port=%s, verbose=%s, username=%s, password=%s, host_metrics=%s, vm_metrics=%s, inventory_refresh_interval=%s' % (
            name, host, port, verbose, username, "******", len(host_counters),
            len(vm_counters), inventory_refresh_interval))

    CONFIGS.append({
        'name': name,
        'host': host,
        'port': port,
        'verbose': verbose,
        'verify_cert': verify_cert,
        'username': username,
        'password': password,
        'host_counters': host_counters,
        'vm_counters': vm_counters,
        'inventory_refresh_interval': inventory_refresh_interval
    })


def init_callback():
    """ In this method we create environments for every configured vCenter
    Server. This includes creating the connection, reading in counter ID
    mapping tables """

    # For every set of configuration received from collectd, a environment must
    # be created.
    for config in CONFIGS:
        env = create_environment(config)

        # The environment is stored under the name of the config block
        ENVIRONMENT[config.get("name")] = env


def read_callback():
    """ This function is regularly executed by collectd. It is important to
    minimize the execution time of the function which is why a lot of caching
    is performed using the environment objects. """

    # Walk through the existing environments
    for name in ENVIRONMENT:
        env = ENVIRONMENT[name]
        collectd.info("read_callback: entering environment: " + name)

        # Connects to vCenter Server
        service_instance = SmartConnect(host=env["host"], user=env["username"], pwd=env["password"])
        performance_manager = service_instance.RetrieveServiceContent().perfManager

        # Walk through all Clusters of Datacenter
        for datacenter in service_instance.RetrieveServiceContent().rootFolder.childEntity:
            if datacenter._wsdlName == "Datacenter":
                for compute_resource in datacenter.hostFolder.childEntity:
                    if compute_resource._wsdlName == "ComputeResource" or \
                            compute_resource._wsdlName == "ClusterComputeResource":

                        # Walk throug all hosts in cluster, collect its metrics and dispatch them
                        collectd.info(
                            "read_callback: found %d hosts in cluster %s" % (
                                len(compute_resource.host), compute_resource.name))
                        collet_metrics_for_entities(performance_manager,
                                                    env['host_counter_ids'],
                                                    compute_resource.host, compute_resource._moId)

                        # Walk throug all vms in host, collect its metrics and dispatch them
                        for host in compute_resource.host:
                            if host._wsdlName == "HostSystem":
                                collectd.info(
                                    "read_callback: found %d vms in host %s" % (
                                        len(host.vm), host.name))
                                collet_metrics_for_entities(performance_manager,
                                                            env['vm_counter_ids'],
                                                            host.vm, compute_resource._moId)
        Disconnect(service_instance)


def collet_metrics_for_entities(performance_manager, filtered_metric_ids, entities,
                                cluster_name):
    # Definition of the queries for getting performance data from vCenter
    query_specs = []
    query_spec = vim.PerformanceManager.QuerySpec()
    query_spec.metricId = filtered_metric_ids
    query_spec.format = "normal"
    # Define the default time range in which the data should be collected (from
    # now to INTERVAL seconds)
    end_time = datetime.datetime.today()
    start_time = datetime.datetime.today() - datetime.timedelta(seconds=INTERVAL)
    query_spec.endTime = end_time
    query_spec.startTime = start_time
    # Define the interval, in seconds, for the performance statistics. This
    # means for any entity and any metric there will be
    # INTERVAL / query_spec.intervalId values collected. Leave blank or use
    # performanceManager.historicalInterval[i].samplingPeriod for
    # aggregated values
    query_spec.intervalId = 20

    # For any entity there has to be an own query.
    if len(entities) == 0:
        return
    for entity in entities:
        query_spec.entity = entity
        query_specs.append(query_spec)

    # Retrieves the performance metrics for the specified entity (or entities)
    # based on the properties specified in the query_specs
    collectd.info("GetMetricsForEntities: collecting its stats")
    metrics_of_entities = performance_manager.QueryPerf(query_specs)

    cd_value = collectd.Values(plugin="collectsphere")
    cd_value.type = "gauge"

    # Walk throug all entites of query
    for metrics_of_entities_number in range(len(metrics_of_entities)):
        metrics_of_entity = metrics_of_entities[metrics_of_entities_number].value

        # For every queried metric per entity, get an array consisting of
        # performance counter information for the specified counterIds.
        queried_counter_ids_per_entity = []
        for metric in metrics_of_entity:
            queried_counter_ids_per_entity.append(metric.id.counterId)
        perf_counter_info_list = performance_manager.QueryPerfCounter(
            queried_counter_ids_per_entity)

        # Walk thorug all queried metrics per entity
        for metrics_of_entity_number in range(len(metrics_of_entity)):
            metric = metrics_of_entity[metrics_of_entity_number]

            perf_counter_info = perf_counter_info_list[metrics_of_entity_number]
            counter = perf_counter_info.nameInfo.key
            group = perf_counter_info.groupInfo.key
            instance = metric.id.instance
            unit = perf_counter_info.unitInfo.key
            rollup_type = perf_counter_info.rollupType

            # Walk throug all values of a metric (INTERVAL / query_spec.intervalId values)
            for metric_value_number in range(len(metric.value)):
                value = float(metric.value[metric_value_number])

                # Get the timestamp of value. Because of an issue by VMware the
                # has to be add an hour if you're at DST
                timestamp = float(time.mktime(
                    metrics_of_entities[metrics_of_entities_number]
                    .sampleInfo[metric_value_number]
                    .timestamp.timetuple()
                ))
                timestamp += time.localtime().tm_isdst * (3600)
                cd_value.time = timestamp

                # truncate
                instance = truncate(instance)
                # When the instance value is empty, the vSphere API references a
                # total. Example: A host has multiple cores for each of which we
                # get a single stat object. An additional stat object will be
                # returned by the vSphere API with an empty string for "instance".
                # This is the overall value accross all logical CPUs.
                # if(len(stat.instance.strip()) == 0):
                #   instance = 'all'
                instance = "all" if instance == "" else instance
                unit = truncate(unit)
                group = truncate(group)
                if rollup_type == vim.PerformanceManager.CounterInfo.RollupType.maximum:
                    print ""
                rollup_type = truncate(rollup_type)
                type_instance_str = \
                    cluster_name + "." \
                    + entities[metrics_of_entities_number]._wsdlName + "." \
                    + entities[metrics_of_entities_number]._moId + "." \
                    + group + "." \
                    + instance + "." \
                    + rollup_type + "." \
                    + counter + "." \
                    + unit
                type_instance_str = type_instance_str.replace(' ', '_')

                # now dispatch to collectd
                cd_value.dispatch(time=timestamp,
                                  type_instance=type_instance_str,
                                  values=[value])


def shutdown_callback():
    """ Called by collectd on shutdown. """
    pass


################################################################################
# HELPER FUNCTIONS
################################################################################


def truncate(string):
    """ We are limited to 63 characters for the type_instance field. This
    function truncates names in a sensible way """

    # NAA/T10 Canonical Names
    match = re.match('(naa|t10)\.(.+)', string, re.IGNORECASE)
    if match:
        id_type = match.group(1).lower()
        identifier = match.group(2).lower()
        if identifier.startswith('ATA'):
            second_match = re.match('ATA_+(.+?)_+(.+?)_+', identifier, re.IGNORECASE)
            identifier = second_match.group(1) + second_match.group(2)
        else:
            string = id_type + identifier[-12:]

    # vCloud Director naming pattern
    match = re.match(
        '^(.*)\s\(([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\)(.*)$',
        string, re.IGNORECASE)
    if match:
        vm_name = match.group(1).lower()
        uuid = match.group(2).lower()
        suffix = match.group(3).lower()
        short_vm_name = vm_name[:6]
        short_uuid = uuid[:6]
        string = short_vm_name + '-' + short_uuid + suffix

    # VMFS UUIDs: e.g. 541822a1-d2dcad52-129a-0025909ac654
    match = re.match('^(.*)([0-9a-f]{8}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{12})(.*)$',
                     string, re.IGNORECASE)
    if match:
        before = match.group(1).lower()
        uuid = match.group(2).lower()
        after = match.group(3).lower()
        short_uuid = uuid[:12]
        string = before + short_uuid + after

    # truncate units
    string = string.replace('millisecond', 'ms')
    string = string.replace('percent', 'perc')
    string = string.replace('number', 'num')
    string = string.replace('kiloBytesPerSecond', 'KBps')
    string = string.replace('kiloBytes', 'KB')
    string = string.replace('megaBytes', 'MB')

    # truncate groups
    string = string.replace('datastore', 'ds')

    return string


def create_environment(config):
    """
    Creates a runtime environment from a given configuration block. As the
    structure of an environment is a bit complicates, this is the time to
    document it:

    A single environment is a dictionary that stores runtime information about
    the connection, metrics, etc for a single vCenter Server. This is the
    structure pattern:

        {
            'host': <FQDN OR IP ADDRESS OF VCENTER SERVER>,
            'username': <USER FOR LOGIN IN VCENTER SERVER>,
            'password': <PASSWORD FOR LOGIN IN VCENTER SERVER>,

            # This is a dictionary that stores mappings of performance counter
            # names to their respective IDs in vCenter.
            'lookup_host': {
                'NAME': <ID>,       # Example: 'cpu.usage': 2
                ...
            },

            # The same lookup dictionary must be available for virtual machines:
            'lookup_vm': {
                'NAME': <ID>,
                ...
            },

            # This stores the IDs of the counter names passed via the
            # configuration block. We used the lookup tables above to fill in
            # the IDs.
            'host_counter_ids': [<ID>, <ID>, ...],
            'vm_counter_ids': [<ID>, <ID>, ...],
        }
    """

    if config.get('verify_cert'):
        ssl._create_default_https_context = ssl._create_unverified_context
    # Connect to vCenter Server
    service_instance = SmartConnect(host=config.get("host"), user=config.get("username"),
                                    pwd=config.get("password"))

    # If we could not connect abort here
    if not service_instance:
        print("Could not connect to the specified host using specified "
              "username and password")
        return -1

    # Set up the environment. We fill in the rest afterwards.
    env = {}
    env["host"] = config.get("host")
    env["username"] = config.get("username")
    env["password"] = config.get("password")

    performance_manager = service_instance.RetrieveServiceContent().perfManager

    # We need at least one host and one virtual machine, which are poweredOn, in
    # the vCenter to be able to fetch the Counter IDs and establish the lookup table.

    # Fetch the Counter IDs
    filtered_counter_ids = []
    for perf_counter in performance_manager.perfCounter:
        counter_key = perf_counter.groupInfo.key + "." + perf_counter.nameInfo.key
        if counter_key in config['vm_counters'] + config['host_counters']:
            filtered_counter_ids.append(perf_counter.key)

    host = None
    virtual_machine = None
    for child in service_instance.RetrieveServiceContent().rootFolder.childEntity:
        if child._wsdlName == "Datacenter":
            for host_folder_child in convert_folder_tree_to_list(child.hostFolder.childEntity):
                host = host_folder_child.host[0] if (
                    (len(host_folder_child.host) != 0) and host_folder_child.host[
                        0].summary.runtime.powerState == vim.HostSystem.PowerState.poweredOn
                ) else host
                if virtual_machine is not None and host is None:
                    break
            vm_list = child.vmFolder.childEntity
            for tmp in vm_list:
                if tmp._wsdlName == "VirtualMachine":
                    if tmp.summary.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
                        virtual_machine = tmp
                        if virtual_machine is not None and host is not None:
                            break
                elif tmp._wsdlName == "Folder":
                    vm_list += tmp.childEntity
                elif tmp._wsdlName == "VirtualApp":
                    vm_list += tmp.vm
    if host is None:
        collectd.info("create_environment: vCenter " + config.get(
            "name") + " does not contain any hosts. Cannot continue")
        return
    if virtual_machine is None:
        collectd.info("create_environment: vCenter " + config.get(
            "name") + " does not contain any VMs. Cannot continue")
        return

    # Get all queryable aggregated and realtime metrics for an entity
    env['lookup_host'] = []
    env['lookup_vm'] = []
    performance_interval = performance_manager.historicalInterval[0]
    performance_interval.level = 2

    # Update performance interval to get all rolluptypes
    performance_manager.UpdatePerfInterval(performance_interval)

    # Query aggregated qureyable mertics for host and virtual_machine
    env['lookup_host'] += performance_manager.QueryAvailablePerfMetric(
        host,
        None,
        None,
        performance_interval.samplingPeriod
    )
    env['lookup_vm'] += performance_manager.QueryAvailablePerfMetric(
        virtual_machine,
        None,
        None,
        performance_interval.samplingPeriod
    )
    # Query aggregated realtime mertics for host and virtual_machine
    env['lookup_host'] += performance_manager.QueryAvailablePerfMetric(
        host,
        None,
        None,
        20
    )
    env['lookup_vm'] += performance_manager.QueryAvailablePerfMetric(
        virtual_machine,
        None,
        None,
        20
    )

    # Now use the lookup tables to find out the IDs of the counter names given
    # via the configuration and store them as an array in the environment.
    # If host_counters or vm_counters is empty, select all.
    env['host_counter_ids'] = []
    if len(config['host_counters']) == 0:
        collectd.info(
            "create_environment: configured to grab all host counters")
        env['host_counter_ids'] = env['lookup_host']
    else:
        for metric in env['lookup_host']:
            if metric.counterId in filtered_counter_ids:
                env['host_counter_ids'].append(metric)

    collectd.info("create_environment: configured to grab %d host counters" % (
        len(env['host_counter_ids'])))

    env['vm_counter_ids'] = []
    if len(config['vm_counters']) == 0:
        env['vm_counter_ids'] = env['lookup_vm']
    else:
        for metric in env['lookup_vm']:
            if metric.counterId in filtered_counter_ids:
                env['vm_counter_ids'].append(metric)

    collectd.info("create_environment: configured to grab %d virtual_machine counters" % (
        len(env['vm_counter_ids'])))

    Disconnect(service_instance)

    return env


################################################################################
# COLLECTD REGISTRATION
################################################################################

collectd.register_config(configure_callback)
collectd.register_init(init_callback)
collectd.register_read(callback=read_callback, interval=INTERVAL)
collectd.register_shutdown(shutdown_callback)
