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
from pysphere import VIServer

CONFIGS = []            # Stores the configuration as passed from collectd
ENVIRONMENT = {}        # Runtime data and object cache

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
            inventory_refresh_interval = val[0]
        else:
            collectd.warning('collectsphere plugin: Unknown config key: %s.' % key )
            continue

    collectd.info('Loaded config: name=%s, host=%s, port=%s, verbose=%s, username=%s, password=%s, host_metrics=%s, vm_metrics=%s, inventory_refresh_interval=%s' % (name, host, port, verbose, username, "******", len(host_counters), len(vm_counters), inventory_refresh_interval))

    CONFIGS.append({'name': name,'host': host,'port': port,'verbose': verbose,'username': username,'password': password, 'host_counters': host_counters,'vm_counters': vm_counters})

def init_callback():
    """ In this method we create environments for every configured vCenter
    Server. This includes creating the connection, reading in counter ID
    mapping tables and spawning inventory watch dogs. """

    # For every set of configuration received from collectd, a environment must
    # be created. Further, a inventory watch dog thread has to be spawned.
    for config in CONFIGS:
        env = create_environment(config)
      
        inventory = {}
        thread = InventoryWatchDog(env.get('conn'), env, 600)
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
        collectd.info("Entering environment: " + name)
        conn = env['conn']

        # Reconnect to vCenter Server if necessary and give up if a connection
        # cannot be established.
        if(conn.is_connected() == False):
            collectd.info("Reconnecting")
            reconnect(name)
            conn = env['conn']
            if(conn.is_connected() == False):
                collectd.info("Reconnect unsuccessful. Giving up.")
                continue
        
        # fetch the instance of perf manager from the object cache
        pm = env['pm']

        # Host Metrics: Walk the _host_ inventory tree created by the inventory
        # watch dog and spawn a thread for every host.
        
        host_counter_ids = env.get('host_counter_ids')
        inventory = env.get('inventory')

        # Only do this if the inventory tree is not empty. If is isn't: for
        # every host in every cluster spawn a thread.
        if inventory:
            for cluster_name in inventory.keys():
                for host_name in inventory.get(cluster_name).get('hosts').keys():
                    
                    # fetch the host MOR from the inventory tree
                    host = inventory.get(cluster_name).get('hosts').get(host_name)
                   
                    # create thread and execute
                    thread = GetMetricsThread(pm, host, host_counter_ids, name, cluster_name, host_name.split('.')[0])
                    thread.start()
        
                    # append the thread to the list of threads
                    threads.append(thread)
        
        collectd.info("Spawned " + str(len(threads)) + " threads to fetch metrics.")
 
    # Wait for all threads to finish. Then dispatch all gathered values to collectd.
    for th in threads:
        th.join()

        # The thread was spawned with some information that we now need to
        # properly dispatch the values. Fetch that information now.
        vc_name = th.vc_name
        cluster_name = th.cluster_name
        host_name = th.host_name
        stats = th.stats

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
           
            # We are limited to 63 characters for the type_instance field. This
            # is why we need to shorten NAA canonical names and VMFS UUIDs. For
            # NAAs it makes the most sense to use the last 6 characters while
            # UUIDs should be unique with the first 6. Unfortunately, there is
            # a chance to collisions for VMFS UUIDs.
            if 'naa' in instance:
                values = instance.split('.')
                instance = values[0] + values[1][-6:]

            if re.search('^[0-9a-f-]+$', instance):
                instance = instance[:6] 
        
            # Now we used the collectd API for dispatch the information to
            # collectd which will then take care of sending it to rrdtool,
            # graphite or whereever.
            cd_value = collectd.Values(plugin="collectsphere")
            cd_value.type = "gauge"
            cd_value.type_instance = cluster_name + "." + host_name + "." + group + "." + instance + "." + counter + "." + unit
            cd_value.values = [value]
            cd_value.dispatch()

    collectd.info("All threads returned, all values dispatched.")

def shutdown_callback():
    """ Called by collectd on shutdown. """

    # Disconnect all existing vCenter connections
    for vc_name in ENVIRONMENT.keys():
		env = ENVIRONMENT.get(vc_name)
		conn = env.get('conn')
		conn.disconnect()
	

#####################################################################################
# HELPER FUNCTIONS 
#####################################################################################

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
    collectd.info("URL: " + url)

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
        collectd.info("vCenter " + config.get("name") + " does not contain any hosts. Cannot continue")
        return
    host_key = hosts[0][0]
    env['lookup_host'] = env['pm'].get_entity_counters(host_key)

    # The same is true for VMs: We need at least one VM to fetch the Counter IDs.
    vms = viserver.get_registered_vms()
    if(len(vms) == 0):
        collectd.info("vCenter " + config.get("name") + " does not contain any VMs. Cannot continue")
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

class GetMetricsThread(threading.Thread):
    """ This thread takes parameters necessary to fetch the metrics of a single
    host and later identify the source of the data once the thread is done. """

    def __init__(self, pm, entity_key, metric_ids, vc_name, cluster_name, host_name):
        threading.Thread.__init__(self)
        self.pm = pm
        self.entity_key = entity_key
        self.metric_ids = metric_ids
        self.vc_name = vc_name
        self.cluster_name = cluster_name
        self.host_name = host_name

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
    
        # Count the clusters and hosts discovered just to have some good logging output.
        host_count = 0
        cluster_count = 0

        # In a infinite loop build the inventory tree
        while True:
            collectd.info("Running inventory refresh ...")

            # Get the inventory and clear it. Create an empty one if none
            # exists.
            inventory = self.environment.get('inventory')
            if inventory:
                inventory.clear()
            else:
                inventory = {}
                self.environment['inventory'] = inventory
    
            for cluster_data in self.conn.get_clusters().items():
                cluster = cluster_data[0]
                cluster_name = cluster_data[1]
                cluster_count += 1

                inventory[cluster_name] = {
                    'cluster': cluster    
                }

                # Build the host list
                hosts = {}
                for hData in self.conn.get_hosts(cluster).items():
                    host = hData[0]
                    host_name = hData[1]
                    hosts[host_name] = host
                    host_count += 1

                inventory[cluster_name]['hosts'] = hosts                

                # Build the vm list
                vms = {}
                # TODO: build the VM list
                inventory[cluster_name]['vms'] = vms

            collectd.info("Found " + str(host_count) + " hosts in " + str(cluster_count) + " clusters. Next refresh in " + str(self.sleepSeconds) + "s")
            
            # Sleep a while (600s by default)
            time.sleep(self.sleepSeconds)

#####################################################################################
# COLLECTD REGISTRATION 
#####################################################################################

collectd.register_config(configure_callback)
collectd.register_init(init_callback)
collectd.register_read(read_callback)
collectd.register_shutdown(shutdown_callback)
