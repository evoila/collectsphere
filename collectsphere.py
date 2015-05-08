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
import time
from pysphere import VIServer

CONFIGS = []                                        # Stores the configuration as passed from collectd
ENVIRONMENT = {}                                    # Runtime data and object cache
SHUTDOWN_SIGNAL_CONDITION = threading.Condition()   # Used to control the access to SHUTDOWN_SIGNAL by the threads
SHUTDOWN_SIGNAL = False; 		                    # Used to signal the shutdown to the spawned threads

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
    host_counters = ['cpu.usage', 'mem.usage']
    vm_counters = ['cpu.usage','mem.usage']
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
            values = val[0].split(',')
            for m in values:
                if len(m) > 0:
                    host_counters.append(m.strip())
        elif key == 'vm_counters':
            values = val[0].split(',')
            for m in values:
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
        env = create_environment(config)
      
        inventory = {}
        thread = InventoryWatchDog(env.get('conn'), env, config.get('inventory_refresh_interval'))
        thread.start()

        env['inventory'] = inventory
        env['watchdog'] = thread

        # The environment is stored under the name of the config block
        ENVIRONMENT[config.get("name")] = env
 
def read_callback():
    """ This function is regularly executed by collectd. It is important to
    minimize the execution time of the function which is why a lot of caching
    is performed using the environment objects. """

    # We are going to spawn a lot of threads soon to speed up metric fetching.
    # References to the threads are stored here.
    threads = []
    
    # Walk through the existing environments
    for name in ENVIRONMENT.keys():
        env = ENVIRONMENT[name]
        collectd.info("read_callback: entering environment: " + name)
        conn = env['conn']

        # Reconnect to vCenter Server if necessary and give up if a connection
        # cannot be established.
        if(conn.is_connected() == False):
            collectd.info("read_callback: reconnecting")
            reconnect(name)
            conn = env['conn']
            if(conn.is_connected() == False):
                collectd.info("read_callback: reconnect unsuccessful. Giving up.")
                continue
        
        # fetch the instance of perf manager from the object cache
        pm = env['pm']

        # Host Metrics: Walk the _host_ inventory tree created by the inventory
        # watch dog and spawn a thread for every host.
        
        host_counter_ids = env.get('host_counter_ids')
        vm_counter_ids = env.get('vm_counter_ids')
        inventory = env.get('inventory')

        # See if there is something to monitor in the environment
        host_count = 0
        vm_count = 0
        for cluster_name in inventory.keys():
            host_count += len(inventory.get(cluster_name).get('hosts'))
            vm_count += len(inventory.get(cluster_name).get('vms'))

        # If 0 clusters or no vms and hosts where discovered, skip to the next environment
        if (host_count == 0 and vm_count == 0):
            collectd.info("read_callback: Inventory is empty. Skipping to next environment")
            continue
        else:
            collectd.info("read_callback: found %d hosts and %d VMs in the inventory cache" % (host_count, vm_count))

        # Only do this if the inventory tree is not empty. If is isn't: for
        # every host in every cluster spawn a thread.
        if inventory:
            for cluster_name in inventory.keys():
               
                collectd.info("read_callback: Cluster %s: Spawning %d threads to fetch host metrics." % (cluster_name, len(inventory.get(cluster_name).get('hosts').keys())))

                # Spawn threads for every host
                for host_name in inventory.get(cluster_name).get('hosts').keys():
                    
                    # fetch the host MOR from the inventory tree
                    host = inventory.get(cluster_name).get('hosts').get(host_name)
                   
                    # create thread and execute
                    thread = GetMetricsThread(pm, host, host_counter_ids, name, cluster_name, 'host', host_name.split('.')[0])
                    thread.start()
        
                    # append the thread to the list of threads
                    threads.append(thread)
                
                collectd.info("read_callback: Cluster %s: Spawning %d threads to fetch VM metrics." % (cluster_name, len(inventory.get(cluster_name).get('vms').keys())))
                
                # Spawn threads for every VM
                for vm_path in inventory.get(cluster_name).get('vms').keys():
                    
                    # fetch the VM
                    vm = inventory.get(cluster_name).get('vms').get(vm_path)

                    # create thread and execute
                    thread = GetMetricsThread(pm, vm._mor, vm_counter_ids, name, cluster_name, 'vm', vm.get_properties().get('name'))
                    thread.start()

                    # append the thread to the list of threads
                    threads.append(thread)

    collectd.info("read_callback: Spawned a total of " + str(len(threads)) + " threads to fetch Host and VM metrics.")

    # Wait for all threads to finish. Then dispatch all gathered values to collectd.
    stats_count = 0
    for th in threads:
        th.join()

        # The thread was spawned with some information that we now need to
        # properly dispatch the values. Fetch that information now.
        vc_name = th.vc_name
        cluster_name = th.cluster_name
        entity_type = th.entity_type
        entity_name = th.entity_name
        stats = th.stats

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
            timestamp = time.mktime(dtime.timetuple())

            # When the instance value is empty, the vSphere API references a
            # total. Example: A host has multiple cores for each of which we
            # get a single stat object. An additional stat object will be
            # returned by the vSphere API with an empty string for "instance".
            # This is the overall value accross all logical CPUs.
            if(len(stat.instance.strip()) == 0):
                instance = 'all'
            
            # struncate
            instance = truncate(instance)
            unit = truncate(unit)
 
            # Now we used the collectd API for dispatch the information to
            # collectd which will then take care of sending it to rrdtool,
            # graphite or whereever.
            cd_value = collectd.Values(plugin="collectsphere")
            cd_value.type = "gauge"
            cd_value.type_instance = cluster_name + "." + entity_type + "." + entity_name + "." + group + "." + instance + "." + counter + "." + unit
            cd_value.values = [value]
            cd_value.dispatch()

    collectd.info("read_callback: Dispatched a total of %d values to collectd." % (stats_count))

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
    m = re.match('(naa|t10)\.([0-9a-f]{32})', str, re.IGNORECASE)
    if m:
        id_type = m.group(1).lower()
        identifier = m.group(2).lower()
        str = id_type + identifier[-6:]

    # UUIDs in general
    m = re.match('^(.*)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(.*)$', str, re.IGNORECASE)
    if m:
        before = m.group(1).lower()
        uuid = m.group(2).lower()
        after = m.group(3).lower()
        short_uuid = uuid[:6]
        str = before + short_uuid + after

    # VMFS UUIDs: e.g. 541822a1-d2dcad52-129a-0025909ac654
    m = re.match('^(.*)([0-9a-f]{8}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{12})(.*)$', str, re.IGNORECASE)
    if m:
        before = m.group(1).lower()
        uuid = m.group(2).lower()
        after = m.group(3).lower()
        short_uuid = uuid[:6]
        str = before + short_uuid + after
   
    # truncate units       
    str = str.replace('millisecond', 'ms')
    str = str.replace('percent', 'perc')
    str = str.replace('number', 'num')
    str = str.replace('kiloBytesPerSecond', 'KBps')
    str = str.replace('kiloBytes', 'KB')
    str = str.replace('megaBytes', 'MB')

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
            'conn': <INSTANCE OF VIServer FROM THE PYSPHERE API>,
            'pm': <INSTANCE OF THE PERFORMANCE MANAGER>,
                
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

            # The inventory watch dog reponsible for this vCenter Server fills
            # in the contents here. The inventory contains a tree of clusters
            # and ESXi hosts as well as VMs as child objects. The important
            # part is that in addition to the names of clusters, hosts and VMs
            # we store the MOR of the objects. Like this we can easily make an
            # API call to fetch metrics.
            'inventory': {
                <CLUSTER_NAME>: {
                    'cluster': <CLUSTER MOR>,
                    'hosts': {
                        <HOST_NAME>: <HOST MOR>,
                        ...
                    },
                    'vms': {
                        <VM_NAME>: <VM MOR>,
                        ...
                    }
                }
            },

            # The instance of the watchdog thread.
            'watchdog': <WATCH DOG THREAD>
        }
    """
    
    url = config.get("host") + ":" + str(config.get("port"))
    collectd.info("create_environment: URL: " + url)

    # Connect to vCenter Server
    viserver = VIServer()
    viserver.connect(url, config.get("username"), config.get("password"))

    # If we could not connect abort here
    if(viserver == None or viserver.is_connected() == False):
        return

    # Set up the environment. We fill in the rest afterwards.
    env = {}
    env['conn'] = viserver
    env['pm'] = viserver.get_performance_manager()

    # We need at least one host in the vCenter to be able to fetch the Counter
    # IDs and establish the lookup table.
    hosts = viserver.get_hosts().items()
    if(len(hosts) == 0):
        collectd.info("create_environment: vCenter " + config.get("name") + " does not contain any hosts. Cannot continue")
        return
    host_key = hosts[0][0]
    env['lookup_host'] = env['pm'].get_entity_counters(host_key)

    # The same is true for VMs: We need at least one VM to fetch the Counter IDs.
    vms = viserver.get_registered_vms()
    if(len(vms) == 0):
        collectd.info("create_environment: vCenter " + config.get("name") + " does not contain any VMs. Cannot continue")
        return
    vm = viserver.get_vm_by_path(vms[0])
    vm_mor = vm._mor
    env['lookup_vm'] = env['pm'].get_entity_counters(vm_mor)

    # Now use the lookup tables to find out the IDs of the counter names given
    # via the configuration and store them as an array in the environment.
    
    env['host_counter_ids'] = []
    if len(config['host_counters']) == 0:
        env['host_counter_ids'] = env['lookup_host'].values()
    else:
        for name in config['host_counters']:
            env['host_counter_ids'].append(env['lookup_host'].get(name))

    env['vm_counter_ids'] = []
    for name in config['vm_counters']:
        env['vm_counter_ids'].append(env['lookup_vm'].get(name))

    return env

def reconnect(name):
    """ This function is used to reconnect to vCenter by a given environment name. """

    global ENVIRONMENT

    env = ENVIRONMENT[name]

    if(env['conn'].is_connected()):
        return

    viserver = VIServer()
    viserver.connect(env['config']['url'], env['config']['login'], env['config']['passwd'])

    if(viserver.is_connected() == True):
        env['conn'] = viserver

#####################################################################################
# HELPER CLASSES 
#####################################################################################

class GetVMThread(threading.Thread):
    
    def __init__(self, vm_path, conn):
        threading.Thread.__init__(self)
        self.vm_path = vm_path
        self.conn = conn

    def run(self):
        self.vm = self.conn.get_vm_by_path(self.vm_path)

    def get_vm(self):
        return self.vm

class GetMetricsThread(threading.Thread):
    """ This thread takes parameters necessary to fetch the metrics of a single
    host and later identify the source of the data once the thread is done. """

    def __init__(self, pm, entity_key, metric_ids, vc_name, cluster_name, entity_type, entity_name):
        threading.Thread.__init__(self)
        self.pm = pm
        self.entity_key = entity_key
        self.metric_ids = metric_ids
        self.vc_name = vc_name
        self.cluster_name = cluster_name
        self.entity_type = entity_type
        self.entity_name = entity_name

    def run(self):
        # The API call is very simple thanks to pysphere :)
        self.stats = self.pm.get_entity_statistic(self.entity_key, self.metric_ids, None, False)

class InventoryWatchDog(threading.Thread):
    """ The Inventory Watch Dog is a thread that is spawned for every vCenter
    configured. It updates the the passed inventory object to reflect the
    current state of the environment. It repeats the inventorization every
    10min by default."""   

    conn = None
    environment = None
    sleepSeconds = None

    def __init__(self, conn, environment, sleepSeconds):
        threading.Thread.__init__(self)
        self.conn = conn
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
            for cluster_data in self.conn.get_clusters().items():
                cluster = cluster_data[0]
                cluster_name = cluster_data[1]

                collectd.info("InventoryWatchDog: Discovered cluster %s" % (cluster_name))

                # if not already there: store the cluster object itself
                if not cluster_name in inventory:
                    collectd.info("InventoryWatchDog: Cluster %s never seen before" % (cluster_name))
                    inventory[cluster_name] = {
                        'cluster': cluster,
                        'hosts': {},
                        'vms': {},
                    }

                # HOST INVENTORY
                host_inventory = {}
                
                # fill it with the hosts
                host_list = self.conn.get_hosts(cluster).items()

                if len(host_list) == 0:
                    collectd.info("InventoryWatchDog: Found 0 hosts in cluster %s. Skipping to next cluster" % (cluster_name))
                    continue

                for hData in self.conn.get_hosts(cluster).items():
                    host_mor = hData[0]
                    host_name = hData[1]
                    host_inventory[host_name] = host_mor
                
                collectd.info("InventoryWatchDog: Found %d hosts in cluster %s" % (len(host_inventory.keys()), cluster_name))

                # VM INVENTORY

                # As we are working on the inventory dictionary but dont want
                # the main thread to spawn threads to fetch metrics for object
                # that are not fully discovered yet, we have to work on a copy
                # of the vm_inventory and then swap it with the original
                vm_inventory = inventory.get(cluster_name).get('vms').copy()

                # fetch list of vm paths (this is very quick)
                vm_path_list = self.conn.get_registered_vms(cluster=cluster)

                collectd.info("InventoryWatchDog: Found %d registered VMs in cluster %s" % (len(vm_path_list), cluster_name))
                
                # Remove all VMs from the inventory cache that are not listed
                # in vSphere anymore
                removed = 0
                for vm_path in vm_inventory.keys():
                    if not vm_path in vm_path_list:
                        vm_inventory.pop(vm_path)
                        removed += 1

                collectd.info("InventoryWatchDog: Removed %d VMs from my inventory cache" % (removed))

                # Generate list of VM paths that need to be added to the
                # inventory
                new_vms = []
                for vm_path in vm_path_list:
                    if not vm_path in vm_inventory:
                        new_vms.append(vm_path)

                collectd.info("InventoryWatchDog: Adding %d VMs to cache. Spawning threads..." % (len(new_vms)))

                # Spawn threads to fetch the VM object of every VM that is in
                # the unknown list
                max_threads = 50
                start_index = 0
                end_index = -1

                while end_index < (len(new_vms)-1):

                    # calculate the end index
                    end_index = start_index + max_threads - 1
                    if(end_index > (len(new_vms)-1)):
                        end_index = len(new_vms)-1
                
                    # get the subset of vm paths
                    current_vm_paths = new_vms[start_index:end_index + 1]
                    
                    # spawn threads
                    threads = []
                    for vm_path in current_vm_paths:
                        thread = GetVMThread(vm_path, self.conn)
                        thread.start()
                        threads.append(thread)
                
                    collectd.info("InventoryWatchDog: Spawned %d threads (%d - %d) to fetch VM objects in cluster %s" % (len(threads), start_index, end_index, cluster_name))
                
                    # Wait for the threads to finish, the put the VM MORs into the
                    # inventory
                    for thread in threads:
                        thread.join()
                        if thread.get_vm:
                            vm_path = thread.vm_path
                            vm = thread.get_vm()
                            vm_inventory[vm_path] = vm
                   
                    # set start_index to the next element for the next run
                    start_index = end_index + 1

                collectd.info("InventoryWatchDog: All threads returned. Publishing inventory...")

                # PUBLISH THE NEW INVENTORY
                inventory.get(cluster_name)['hosts'] = host_inventory
                inventory.get(cluster_name)['vms'] = vm_inventory

            end_time = time.time()
            elapsed_time = end_time - start_time

            collectd.info("InventoryWatchDog: Discovered %d hosts and %d VMs in %d clusters in %d seconds. Next refresh in %d seonds." % (
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
collectd.register_read(read_callback)
collectd.register_shutdown(shutdown_callback)
