"""
This is the code that needs to be integrated into collectd when run in
production. It contains the python code that integrates into the python module
for collectd. It connects to one or more vCenter Servers and gathers the
configured metrics from ESXi hosts and Virtual Machines.

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
import tzlocal

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
    use_friendly_name = None
    username = None
    password = None
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
            verbose = bool(val[0])
        elif key == 'verifycertificate':
            verify_cert = bool(val[0])
        elif key == 'usefriendlyname':
            use_friendly_name = bool(val[0])
        elif key == 'username':
            username = val[0]
        elif key == 'password':
            password = val[0]
        elif key == 'host_counters':
            counters = val[0]
            if not counters == "all":
                values = counters.split(',')
                for value in values:
                    if len(value) > 0:
                        host_counters.append(value.strip())
        elif key == 'vm_counters':
            counters = val[0]
            if not counters == "all":
                values = counters.split(',')
                for value in values:
                    if len(value) > 0:
                        vm_counters.append(value.strip())
        elif key == 'inventory_refresh_interval':
            inventory_refresh_interval = int(val[0])
        else:
            collectd.warning('collectsphere plugin: Unknown config key: %s.'
                             % key)
            continue

    log_message = \
        'configure_callback: Loaded config: name=%s, host=%s, port=%s, ' \
        'verbose=%s, username=%s, password=%s, host_metrics=%s, ' \
        'vm_metrics=%s, inventory_refresh_interval=%s' % (
            name, host, port, verbose, username, "******", len(host_counters),
            len(vm_counters), inventory_refresh_interval
        )
    collectd.info(
        log_message
    )

    CONFIGS.append({
        'name': name,
        'host': host,
        'port': port,
        'verbose': verbose,
        'verify_cert': verify_cert,
        'use_friendly_name': use_friendly_name,
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
        service_instance = SmartConnect(
            host=env["host"], user=env["username"], pwd=env["password"]
        )
        performance_manager = service_instance \
            .RetrieveServiceContent() \
            .perfManager

        # Walk through all Clusters of Datacenter
        for datacenter in service_instance \
                .RetrieveServiceContent() \
                .rootFolder.childEntity:
            if datacenter._wsdlName == "Datacenter":
                for compute_resource in datacenter.hostFolder.childEntity:
                    if compute_resource._wsdlName == \
                            "ComputeResource" \
                            or compute_resource._wsdlName == \
                                    "ClusterComputeResource":
                        cluster_name = \
                            compute_resource.name if env['use_friendly_name'] \
                                else compute_resource._moId
                        # Walk throug all hosts in cluster, collect its metrics
                        # and dispatch them
                        collectd.info(
                            "read_callback: found %d hosts in cluster %s" % (
                                len(compute_resource.host),
                                compute_resource.name
                            )
                        )
                        collet_metrics_for_entities(
                            service_instance,
                            performance_manager,
                            env['host_counter_ids'],
                            compute_resource.host,
                            cluster_name,
                            env
                        )

                        # Walk throug all vms in host, collect its metrics and
                        # dispatch them
                        for host in compute_resource.host:
                            if host._wsdlName == "HostSystem":
                                collectd.info(
                                    "read_callback: found %d vms in host %s" % (
                                        len(host.vm), host.name
                                    )
                                )
                                collet_metrics_for_entities(
                                    service_instance,
                                    performance_manager,
                                    env['vm_counter_ids'],
                                    host.vm,
                                    cluster_name,
                                    env
                                )
        Disconnect(service_instance)


def shutdown_callback():
    """ Called by collectd on shutdown. """
    pass


################################################################################
# HELPER FUNCTIONS
################################################################################

def collet_metrics_for_entities(service_instance, performance_manager,
                                filtered_metric_ids, entities, cluster_name,
                                env):
    # Definition of the queries for getting performance data from vCenter
    query_specs = []

    # Define the default time range in which the data should be collected (from
    # now to INTERVAL seconds)
    end_time = service_instance.CurrentTime()
    start_time = end_time - datetime.timedelta(seconds=INTERVAL)

    # For any entity there has to be an own query.
    if len(entities) == 0:
        return
    for entity in entities:
        query_spec = vim.PerformanceManager.QuerySpec()
        query_spec.metricId = filtered_metric_ids
        query_spec.format = "normal"
        query_spec.endTime = end_time
        query_spec.startTime = start_time
        # Define the interval, in seconds, for the performance statistics. This
        # means for any entity and any metric there will be
        # INTERVAL / query_spec.intervalId values collected. Leave blank or use
        # performanceManager.historicalInterval[i].samplingPeriod for
        # aggregated values
        query_spec.intervalId = 20
        query_spec.entity = entity
        query_specs.append(query_spec)

    # Retrieves the performance metrics for the specified entity (or entities)
    # based on the properties specified in the query_specs
    collectd.info("GetMetricsForEntities: collecting its stats")
    perf_entity_metrics = performance_manager.QueryPerf(query_specs)

    cd_value = collectd.Values(plugin="collectsphere")
    # Walk throug all entites of query

    # for perf_entity_metric in perf_entity_metrics:
    for perf_entity_metric_count in range(len(perf_entity_metrics)):
        perf_entity_metric = perf_entity_metrics[perf_entity_metric_count]
        perf_sample_infos = perf_entity_metric.sampleInfo
        perf_metric_series_list = perf_entity_metric.value

        # For every queried metric per entity, get an array consisting of
        # performance counter information for the specified counterIds.
        queried_counter_ids_per_entity = []
        for perf_metric_series in perf_metric_series_list:
            queried_counter_ids_per_entity.append(
                perf_metric_series.id.counterId)
        perf_counter_info_list = performance_manager.QueryPerfCounter(
            queried_counter_ids_per_entity)

        instances = list()

        # for perf_metric_series in perf_metric_series_list:
        for perf_metric_series_count in range(len(perf_metric_series_list)):
            perf_metric_series = perf_metric_series_list[
                perf_metric_series_count]
            perf_counter_info = perf_counter_info_list[perf_metric_series_count]
            counter = perf_counter_info.nameInfo.key
            group = perf_counter_info.groupInfo.key

            unit = perf_counter_info.unitInfo.key
            rollup_type = perf_counter_info.rollupType

            instance = perf_metric_series.id.instance
            if instance in instances:
                continue
            else:
                instances.append(instance)

            # for perf_metric in perf_metric_series.value:
            for perf_metric_count in range(len(perf_metric_series.value)):
                perf_metric = perf_metric_series.value[perf_metric_count]

                timestamp = float(time.mktime(
                    perf_sample_infos[perf_metric_count]
                        .timestamp.astimezone(
                        tzlocal.get_localzone()
                    ).timetuple()
                ))

                entity = entities[perf_entity_metric_count]

                # When the instance value is empty, the vSphere API references a
                # total. Example: A host has multiple cores for each of which we
                # get a single stat object. An additional stat object will be
                # returned by the vSphere API with an empty string for
                # "instance".
                # This is the overall value accross all logical CPUs.
                # if(len(stat.instance.strip()) == 0):
                #   instance = 'all'
                instance = "all" if instance == "" else instance
                type_instance_str = \
                    cluster_name + "." + re.sub(
                        pattern=r'[^A-Za-z0-9_]',
                        repl='_',
                        string=
                        (
                            entity.name
                            if env['use_friendly_name']
                            else
                            entity._moId
                        )
                    ) + "." + instance

                # now dispatch to collectd
                collectd.info("dispatch " + str(
                    timestamp) + "\t" + type_instance_str + "\t" + str(
                    long(perf_metric)))
                cd_value.type = re.sub(
                    pattern=r'[^A-Za-z0-9_]',
                    repl='_',
                    string=
                    entity._wsdlName
                ) + "." + group + "." + rollup_type + "." + counter + "." + unit
                cd_value.dispatch(time=timestamp,
                                  type_instance=type_instance_str,
                                  values=[long(perf_metric)])


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

    if not config.get('verify_cert'):
        ssl._create_default_https_context = ssl._create_unverified_context
    # Connect to vCenter Server
    service_instance = SmartConnect(host=config.get("host"),
                                    user=config.get("username"),
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
    env['use_friendly_name'] = config.get('use_friendly_name')
    performance_manager = service_instance.RetrieveServiceContent().perfManager

    # We need at least one host and one virtual machine, which are poweredOn, in
    # the vCenter to be able to fetch the Counter IDs and establish the lookup
    # table.

    # Fetch the Counter IDs
    filtered_counter_ids = []
    for perf_counter in performance_manager.perfCounter:
        counter_key = \
            perf_counter.groupInfo.key + "." + perf_counter.nameInfo.key
        if counter_key in config['vm_counters'] + config['host_counters']:
            filtered_counter_ids.append(perf_counter.key)

    host = None
    virtual_machine = None
    for child in service_instance \
            .RetrieveServiceContent() \
            .rootFolder.childEntity:
        if child._wsdlName == "Datacenter":
            for host_folder_child in convert_folder_tree_to_list(
                    child.hostFolder.childEntity
            ):
                host = host_folder_child.host[0] if (
                    (len(host_folder_child.host) != 0) and
                    host_folder_child
                    .host[0]
                    .summary
                    .runtime
                    .powerState
                    == vim.HostSystem.PowerState.poweredOn
                ) else host
                if virtual_machine is not None and host is None:
                    break
            vm_list = child.vmFolder.childEntity
            for tmp in vm_list:
                if tmp._wsdlName == "VirtualMachine":
                    if tmp.summary.runtime.powerState == \
                            vim.VirtualMachine.PowerState.poweredOn:
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
    if len(performance_manager.historicalInterval) is not 0:
        performance_interval = performance_manager.historicalInterval[0]
        samplingPeriod = performance_interval.samplingPeriod
        performance_interval.level = 2

        # Update performance interval to get all rolluptypes
        performance_manager.UpdatePerfInterval(performance_interval)

    else:
        samplingPeriod = None

    # Query aggregated qureyable mertics for host and virtual_machine
    env['lookup_host'] += performance_manager.QueryAvailablePerfMetric(
        host,
        None,
        None,
        samplingPeriod
    )
    env['lookup_vm'] += performance_manager.QueryAvailablePerfMetric(
        virtual_machine,
        None,
        None,
        samplingPeriod
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

    collectd.info(
        "create_environment: configured to grab %d virtual_machine counters" % (
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
