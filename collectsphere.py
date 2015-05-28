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

import collectd
import logging
import threading
import time
import ssl
import re
import datetime

from pyVmomi import *
from pyVim import connect

#####################################################################################
# CONFIGURE ME
#####################################################################################
INTERVAL = 120

#####################################################################################
# DO NOT CHANGE BEYOND THIS POINT!
#####################################################################################
# Stores the configuration as passed from collectd
CONFIGS = []                                        

# Runtime data and object cache
ENVIRONMENT = {}                                    

# Used to control the access to SHUTDOWN_SIGNAL by the threads
SHUTDOWN_SIGNAL_CONDITION = threading.Condition()   

# Used to signal the shutdown to the spawned threads
SHUTDOWN_SIGNAL = False; 		                    

#####################################################################################
# IMPLEMENTATION OF COLLECTD CALLBACK FUNCTIONS 
#####################################################################################

def configure_callback(conf):
    """Receive configuration block. This is called by collectd for every
    configuration block it finds for this module."""

    # Set some sensible default values
    name = None
    host = None
    port = 443 
    verbose = None
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
        elif key == 'username':
            username = val[0]
        elif key == 'password':
            password = val[0]
        elif key == 'host_counters':
            str = val[0]
            if not str == "all": 
                values = str.split(',')
                for m in values:
                    if len(m) > 0:
                        host_counters.append(m.strip())
        elif key == 'vm_counters':
            str = val[0]
            if not str == "all": 
                values = str.split(',')
                for m in values:
                    if len(m) > 0:
                	vm_counters.append(m.strip())
        elif key == 'inventory_refresh_interval':
            inventory_refresh_interval = int(val[0])
        else:
            collectd.warning('collectsphere plugin: Unknown config key: %s.' % key )
            continue

    collectd.info('configure_callback: Loaded config: name=%s, host=%s, port=%s, verbose=%s, username=%s, password=%s, host_metrics=%s, vm_metrics=%s, inventory_refresh_interval=%s' % (name, host, port, verbose, username, "******", len(host_counters), len(vm_counters), inventory_refresh_interval))

    CONFIGS.append({
        'name': name,
        'host': host,
        'port': port,
        'verbose': verbose,
        'username': username,
        'password': password,
        'host_counters': host_counters,
        'vm_counters': vm_counters,
        'inventory_refresh_interval': inventory_refresh_interval
    })

def init_callback():
    """ In this method we create environments for every configured vCenter
    Server. This includes creating the connection, reading in counter ID
    mapping tables and spawning inventory watch dogs. """

    # For every set of configuration received from collectd, a environment must
    # be created. Further, a inventory watch dog thread has to be spawned.
    for config in CONFIGS:
        # create environment
        env = create_environment(config)
      
        # spawn watch dog thread
        thread = InventoryWatchDog(env.get('service_instance'), env, config.get('inventory_refresh_interval'))
        thread.start()

        # save to environment
        env['inventory'] = {}
        env['watchdog'] = thread
        ENVIRONMENT[config.get("name")] = env
 
def read_callback():
    """ This function is regularly executed by collectd. It is important to
    minimize the execution time of the function which is why a lot of caching
    is performed using the environment objects. """

    # keep track of own execution time
    start_time = time.time()

    threads = []
    
    # Walk through the existing environments
    for name in ENVIRONMENT.keys():
        env = ENVIRONMENT[name]
        collectd.info("read_callback: entering environment: " + name)
        service_instance = env['service_instance']

        # fetch the instance of perf manager from the object cache
        pm = env['pm']

        # Host Metrics: Walk the _host_ inventory tree created by the inventory
        # watch dog 
        
        host_counter_ids = env.get('host_counter_ids')
        vm_counter_ids = env.get('vm_counter_ids')
        inventory = env.get('inventory')

        collectd.info("Found %d host counter IDs" % (len(host_counter_ids)))
        collectd.info("Found %d vm counter IDs" % (len(vm_counter_ids)))

        # See if there is something to monitor in the environment
        host_count = 0
        vm_count = 0
        for cluster_name in inventory.keys():
            host_count += len(inventory.get(cluster_name).get('hosts'))
            vm_count += len(inventory.get(cluster_name).get('vms'))
    
        collectd.info("Found %d hosts in the intentory" % (host_count))
        collectd.info("Found %d vms in the intentory" % (vm_count))

        # If 0 clusters or no vms and hosts where discovered, skip to the next environment
        if (host_count == 0 and vm_count == 0):
            collectd.info("read_callback: Inventory is empty. Skipping to next environment")
            continue
        else:
            collectd.info("read_callback: found %d hosts and %d VMs in the inventory cache" % (host_count, vm_count))

        # Only do this if the inventory tree is not empty.
        if inventory:
            for cluster_name in inventory.keys():
                collectd.info("read_callback: Cluster %s: Spawning %d threads to fetch host metrics." % (cluster_name, len(inventory.get(cluster_name).get('hosts').keys())))

                # Spawn threads for every host
                for host_name in inventory.get(cluster_name).get('hosts').keys():
                    
                    # fetch the host MOR from the inventory tree
                    host = inventory.get(cluster_name).get('hosts').get(host_name)
                   
                    # create thread and execute
                    thread = GetMetricsThread(pm, host, host_counter_ids, name, cluster_name, 'host')
                    thread.start()
    
                    # append the thread to the list of threads
                    threads.append(thread)
                
                collectd.info("read_callback: Cluster %s: Spawning %d threads to fetch VM metrics." % (cluster_name, len(inventory.get(cluster_name).get('vms').keys())))
                
                # Spawn threads for every VM
                for vm_name in inventory.get(cluster_name).get('vms').keys():
                    
                    # fetch the VM
                    vm = inventory.get(cluster_name).get('vms').get(vm_name)

                    # create thread and execute
                    thread = GetMetricsThread(pm, vm, vm_counter_ids, name, cluster_name, 'vm')
                    thread.start()

                    # append the thread to the list of threads
                    threads.append(thread)

    collectd.info("read_callback: Spawned a total of " + str(len(threads)) + " threads to fetch Host and VM metrics.")

    #TODO
    return
    
    # prepare Value
    cd_value = collectd.Values(plugin="collectsphere")
    cd_value.type = "gauge" 
    cd_value.time = float(datetime.datetime.now().strftime('%s'))

    stats_count = 0

    # Wait for threads to finish in FIFO order which should distribute the load
    # when dispatching values a bit better.
    for th in threads:
        th.join()

        # The thread was spawned with some information that we now need to
        # properly dispatch the values. Fetch that information now.
        vc_name = th.vc_name
        cluster_name = th.cluster_name
        entity_type = th.entity_type
        entity_name = th.entity_name
        stats = th.stats

        # Sometimes it seems to happen that no stats object is returned.
        # GetMetricsThread sets self.stats to None in this case. 
        if not stats:
            continue

        cluster_name = truncate(cluster_name)
        entity_name = truncate(entity_name)

        stats_count += len(stats)

        # For every stat object work up the data, then dispatch
        for stat in stats:

            counter = stat.counter
            group = stat.group
            instance = stat.instance
            dtime = stat.time
            unit = stat.unit
            value = float(stat.value)

            # When the instance value is empty, the vSphere API references a
            # total. Example: A host has multiple cores for each of which we
            # get a single stat object. An additional stat object will be
            # returned by the vSphere API with an empty string for "instance".
            # This is the overall value accross all logical CPUs.
            if(len(stat.instance.strip()) == 0):
                instance = 'all'
            
            # truncate
            instance = truncate(instance)
            unit = truncate(unit)
            group = truncate(group)

            type_instance_str = cluster_name + "." + entity_type + "." + entity_name + "." + group + "." + instance + "." + counter + "." + unit
            type_instance_str = type_instance_str.replace(' ', '_')

            # now dispatch to collectd
            cd_value.dispatch(type_instance = type_instance_str, values = [value])

    # keep track of own execution time
    elapsed = time.time() - start_time    
        
    # dispatch execution time to collectd
    cd_value.dispatch(type_instance = "exec.time.ms", values = [elapsed])

    collectd.info("read_callback: Dispatched a total of %d values in %f seconds." % (stats_count, elapsed))


def shutdown_callback():
    """ Called by collectd on shutdown. """

    # Shutdown the Watchdogs
    SHUTDOWN_SIGNAL_CONDITION.acquire()
    SHUTDOWN_SIGNAL = True;
    SHUTDOWN_SIGNAL_CONDITION.notifyAll();
    SHUTDOWN_SIGNAL_CONDITION.release();

    # Disconnect all existing vCenter connections
    for vc_name in ENVIRONMENT.keys():
		env = ENVIRONMENT.get(vc_name)
		conn = env.get('conn')
		conn.disconnect()
	

#####################################################################################
# HELPER FUNCTIONS 
#####################################################################################

def truncate(str):
    """ We are limited to 63 characters for the type_instance field. This
    function truncates names in a sensible way """

    # NAA/T10 Canonical Names
    m = re.match('(naa|t10)\.(.+)', str, re.IGNORECASE)
    if m:
        id_type = m.group(1).lower()
        identifier = m.group(2).lower()
        if identifier.startswith('ATA'):
            m2 = re.match('ATA_+(.+?)_+(.+?)_+', identifier, re.IGNORECASE)
            identifier = m2.group(1) + m2.group(2)
        else:
            str = id_type + identifier[-12:]

    # vCloud Director naming pattern
    m = re.match('^(.*)\s\(([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\)(.*)$', str, re.IGNORECASE)
    if m:
        vm_name = m.group(1).lower()
        uuid = m.group(2).lower()
        suffix = m.group(3).lower()
        short_vm_name = vm_name[:6]
        short_uuid = uuid[:6]
        str = short_vm_name + '-' + short_uuid + suffix

    # VMFS UUIDs: e.g. 541822a1-d2dcad52-129a-0025909ac654
    m = re.match('^(.*)([0-9a-f]{8}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{12})(.*)$', str, re.IGNORECASE)
    if m:
        before = m.group(1).lower()
        uuid = m.group(2).lower()
        after = m.group(3).lower()
        short_uuid = uuid[:12]
        str = before + short_uuid + after
   
    # truncate units       
    str = str.replace('millisecond', 'ms')
    str = str.replace('percent', 'perc')
    str = str.replace('number', 'num')
    str = str.replace('kiloBytesPerSecond', 'KBps')
    str = str.replace('kiloBytes', 'KB')
    str = str.replace('megaBytes', 'MB')

    # truncate groups
    str = str.replace('datastore', 'ds')
    
    return str

def create_environment(config):
    """
    Creates a runtime environment from a given configuration block. As the
    structure of an environment is a bit complicates, this is the time to
    document it:
   
    A single environment is a dictionary that stores runtime information about
    the connection, metrics, etc for a single vCenter Server. This is the
    structure pattern:

        {
            'service_instance': <INSTANCE OF VIServer FROM THE PYSPHERE API>,
            'pm': <INSTANCE OF THE PERFORMANCE MANAGER>,
                
            # This is a dictionary that stores mappings of performance counter
            # names to their respective IDs in vCenter.
            'lookup_host': {    
                'NAME': <ID>,       # Example: 'cpu.usage': <instance of vim.MetricId>
                ...
            },

            # The same lookup dictionary must be available for virtual machines:
            'lookup_vm': {
                'NAME': <ID>,
                ...
            },
            
            # This stores the IDs of the counter names passed via the
            # configuration block. We used the lookup tables above to fill in
            # the IDs. IDs come as MetricId objects

            'host_counter_ids': [<ID>, <ID>, ...],
            'vm_counter_ids': [<ID>, <ID>, ...],

            # The inventory watch dog reponsible for this vCenter Server fills
            # in the contents here. The inventory contains a tree of clusters
            # and ESXi hosts as well as VMs as child objects. The important
            # part is that in addition to the names of clusters, hosts and VMs
            # we store the MOR of the objects. Like this we can easily make an
            # API call to fetch metrics.
            'inventory': {
                <CLUSTER_NAME>: {
                    'cluster': <CLUSTER>,
                    'hosts': {
                        <HOST_NAME>: <HOST>,
                        ...
                    },
                    'vms': {
                        <VM_NAME>: <VM>,
                        ...
                    }
                }
            },

            # The instance of the watchdog thread.
            'watchdog': <WATCH DOG THREAD>
        }
    """
    
    # Connect to vCenter Server
    try:
        service_instance = connect.SmartConnect(host=config.get("host"), user=config.get("username"), pwd=config.get("password"), port=config.get("port"))
    except:
        return

    # Set up the environment. We fill in the rest afterwards.
    env = {}
    env['service_instance'] = service_instance
    env['pm'] = service_instance.content.perfManager

    # Setup lookup tables. They map names like "cpu.usage" to instances of
    # MetricId which we will need later to query for the values.
    env['lookup_host'] = create_host_metric_lookup_table(service_instance)
    env['lookup_vm'] = create_vm_metric_lookup_table(service_instance)

    collectd.info("Lookup table for host metrics contains %d entries." % (len(env['lookup_host'].keys())))
    collectd.info("Lookup table for vm metrics contains %d entries." % (len(env['lookup_vm'].keys())))

    # Now use the lookup tables to find out the IDs of the counter names given
    # via the configuration and store them as an array in the environment.
    env['host_counter_ids'] = []
    for name in config['host_counters']:
        env['host_counter_ids'].append(env['lookup_host'].get(name))

    env['vm_counter_ids'] = []
    for name in config['vm_counters']:
        env['vm_counter_ids'].append(env['lookup_vm'].get(name))
    
    collectd.info("create_environment: configured to grab %d vm counters" % (len(env['vm_counter_ids'])))
    collectd.info("create_environment: configured to grab %d host counters" % (len(env['host_counter_ids'])))

    print env['vm_counter_ids']
    
    return env

def create_vm_metric_lookup_table(service_instance):
    objView = service_instance.content.viewManager.CreateContainerView(service_instance.content.rootFolder, [vim.VirtualMachine], True)
    host = objView.view[0]
    return create_metric_lookup_table(service_instance, host)

def create_host_metric_lookup_table(service_instance):
    objView = service_instance.content.viewManager.CreateContainerView(service_instance.content.rootFolder, [vim.HostSystem], True)
    host = objView.view[0]
    return create_metric_lookup_table(service_instance, host)

def create_metric_lookup_table(service_instance, entity):
    pm = service_instance.content.perfManager

    # List of vim.PerformanceManager.MetricId
    avail_metricsIds = pm.QueryAvailablePerfMetric(entity, intervalId=20)

    metricId_map = {}
    for metricId in avail_metricsIds:
        metricId_map[metricId.counterId] = metricId

    # List of vim.PerformanceManager.CounterInfo
    counter_infos = pm.QueryPerfCounter(counterId=metricId_map.keys())

    lookup_table = {}
    for info in counter_infos:
        key = info.key
        name = info.groupInfo.key + '.' + info.nameInfo.key
        lookup_table[name] = key

    return lookup_table

def get_cluster_list(service_instance, parent=None):
    if not parent:
        parent = service_instance.content.rootFolder

    cluster_list = []
    
    if isinstance(parent, vim.ClusterComputeResource):
        cluster_list.append(parent)

    if hasattr(parent, 'childEntity'):
        for child in parent.childEntity:
            cluster_list = cluster_list + get_cluster_list(service_instance, child)

    if hasattr(parent, 'hostFolder'):
        for child in parent.hostFolder.childEntity:
            cluster_list = cluster_list + get_cluster_list(service_instance, child)

    return cluster_list

#####################################################################################
# HELPER CLASSES 
#####################################################################################

class Metric

    def __init__(self, metric_name, value, timestamp, container, entity, instance)
        return

class GetMetricsThread(threading.Thread):
    """ This thread takes parameters necessary to fetch the metrics of a single
    host and later identify the source of the data once the thread is done. """

    def __init__(self, pm, entity, metric_ids, vc_name, cluster_name, entity_type):
        threading.Thread.__init__(self)
        self.pm = pm
        self.entity = entity
        self.metric_ids = metric_ids
        self.vc_name = vc_name
        self.cluster_name = cluster_name
        self.entity_type = entity_type
        self.data = None

    def run(self):
        refreshRate = self.pm.QueryPerfProviderSummary(entity=self.entity).refreshRate

        metricIds = []
        for id in self.metric_ids:
            metricIds.append(vim.PerformanceManager.MetricId(counterId=2, instance="*"))

        perfQuerySpec = vim.PerformanceManager.QuerySpec(entity=self.entity, metricId=metricIds, intervalId=refreshRate, maxSample=1)
        perf = self.pm.QueryPerf([perfQuerySpec])

        entityMetric = perf[0]
        timestamp = entityMetric.sampleInfo.timestamp



        print perf

        #data = {}
        #for sampleInfo in perf[0].sampleInfo:
        #    timestamp = sampleInfo.timestamp
        return

class InventoryWatchDog(threading.Thread):
    """ The Inventory Watch Dog is a thread that is spawned for every vCenter
    configured. It updates the the passed inventory object to reflect the
    current state of the environment. It repeats the inventorization every
    10min by default."""   

    service_instance = None
    environment = None
    sleepSeconds = None

    def __init__(self, service_instance, environment, sleepSeconds):
        threading.Thread.__init__(self)
        self.service_instance = service_instance
        self.environment = environment
        self.sleepSeconds = sleepSeconds

    def run(self):
    
        # In a infinite loop build the inventory tree
        while True:
            collectd.info("InventoryWatchDog: Running inventory refresh ...")
            start_time = time.time()

            # fetch or create the inventory
            inventory = self.environment.get('inventory')
            if not inventory:
                inventory = {}
                self.environment['inventory'] = inventory

            # For every cluster, list the hosts and vms and add them to the
            # inventory if necessary. Remove entires of entities which are not
            # there anymore.
            
            clusters = get_cluster_list(self.service_instance)
            for cluster in clusters:
                cluster_name = cluster.name
                collectd.info("InventoryWatchDog: Discovered cluster %s" % (cluster_name))
           
                # if not already there: store the cluster object itself
                if not cluster_name in inventory:
                    collectd.info("InventoryWatchDog:     Cluster %s never seen before" % (cluster_name))
                    inventory[cluster_name] = {
                        'cluster': cluster,
                        'hosts': {},
                        'vms': {},
                    }
                
                # HOST INVENTORY
                host_inventory = {}
                hostView = self.service_instance.content.viewManager.CreateContainerView(container=cluster, type=[vim.HostSystem], recursive=True) 
                host_list = hostView.view
                for host in host_list:
                    host_inventory[host.name] = host
                
                collectd.info("InventoryWatchDog:     Found %d hosts in cluster %s" % (len(host_inventory.keys()), cluster_name))

                # VM INVENTORY
                vm_inventory = {}
                vmView = self.service_instance.content.viewManager.CreateContainerView(container=cluster, type=[vim.VirtualMachine], recursive=True) 
                vm_list = vmView.view
                for vm in vm_list:
                    vm_inventory[vm.name] = vm
                
                collectd.info("InventoryWatchDog:     Found %d VMs in cluster %s" % (len(vm_inventory.keys()), cluster_name))

                # PUBLISH THE NEW INVENTORY
                inventory.get(cluster_name)['hosts'] = host_inventory
                inventory.get(cluster_name)['vms'] = vm_inventory

            end_time = time.time()
            elapsed_time = end_time - start_time

            collectd.info("InventoryWatchDog: Discovered %d hosts and %d VMs in %d clusters in %d seconds. Next refresh in %d seconds." % (
                len(inventory[cluster_name]['hosts'].keys()),
                len(inventory[cluster_name]['vms'].keys()),
                len(inventory.keys()),
                elapsed_time,
                self.sleepSeconds))

            SHUTDOWN_SIGNAL_CONDITION.acquire()
            SHUTDOWN_SIGNAL_CONDITION.wait(self.sleepSeconds)
            if SHUTDOWN_SIGNAL:
			    SHUTDOWN_SIGNAL_CONDITION.notifyAll()
			    SHUTDOWN_SIGNAL_CONDITION.release()
			    break

#####################################################################################
# COLLECTD REGISTRATION 
#####################################################################################

collectd.register_config(configure_callback)
collectd.register_init(init_callback)
collectd.register_read(callback=read_callback, interval=INTERVAL)
collectd.register_shutdown(shutdown_callback)
