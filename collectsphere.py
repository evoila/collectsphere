# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import collectd
import logging
import threading
import time
import ssl
import re
from pysphere import VIServer

CONFIGS = []
ENVIRONMENT = {}

#####################################################################################
# IMPLEMENTATION OF COLLECTD CALLBACK FUNCTIONS 
#####################################################################################

def configure_callback(conf):
    """Receive configuration block. This is called by collectd for every
    configuration block it finds for this module."""

    name = None
    host = None
    port = None
    verbose = None
    username = None
    password = None
    host_counters = []
    vm_counters = []
    inventory_refresh_interval = None

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

    for config in CONFIGS:
        env = create_environment(config)
      
        inventory = {}
        thread = InventoryWatchDog(env.get('conn'), inventory, 600)
        thread.start()

        env['inventory'] = inventory
        env['watchdog'] = thread

        ENVIRONMENT[config.get("name")] = env
 
def read_callback():
    threads = []
    for name in ENVIRONMENT.keys():
        env = ENVIRONMENT[name]
        collectd.info("Entering environment: " + name)
        conn = env['conn']

        if(conn.is_connected() == False):
            collectd.info("Reconnecting")
            reconnect(name)
            conn = env['conn']
            if(conn.is_connected() == False):
                collectd.info("Reconnect unsuccessful. Giving up.")
                continue

        pm = env['pm']

        # HOST METRICS     
        host_counter_ids = env.get('host_counter_ids')
        inventory = env.get('inventory')
        host_inventory = inventory.get('host_inventory')

        if host_inventory:
            for cluster_name in host_inventory.keys():
                for host_name in host_inventory.get(cluster_name).get('hosts').keys():
                    host = host_inventory.get(cluster_name).get('hosts').get(host_name)
                    thread = GetMetricsThread(pm, host, host_counter_ids, name, cluster_name, host_name.split('.')[0])
                    threads.append(thread)
                    thread.start()
        
        collectd.info("Spawned " + str(len(threads)) + " threads to fetch metrics.")
 
    for th in threads:
        th.join()

        vc_name = th.vc_name
        cluster_name = th.cluster_name
        host_name = th.host_name
        stats = th.stats

        env = ENVIRONMENT.get(vc_name)

        for stat in stats:
            counter = stat.counter
            group = stat.group
            instance = stat.instance
            dtime = stat.time
            unit = stat.unit
            value = float(stat.value)
            timestamp = time.mktime(dtime.timetuple())

            if(len(stat.instance.strip()) == 0):
                instance = 'average'
            
            if 'naa' in instance:
                values = instance.split('.')
                instance = values[0] + values[1][-6:]

            if re.search('^[0-9a-f-]+$', instance):
                instance = instance[:6] 
        
            cd_value = collectd.Values(plugin="collectsphere")
            cd_value.type = "gauge"
            cd_value.type_instance = cluster_name + "." + host_name + "." + group + "." + instance + "." + counter + "." + unit
            cd_value.values = [value]
            cd_value.dispatch()

    collectd.info("All threads returned, all values dispatched.")

def shutdown_callback():
	""" Called by collectd on shutdown. """
	
	for vc_name in ENVIRONMENT.keys():
		env = ENVIRONMENT.get(vc_name)
		conn = env.get('conn')
		conn.disconnect()
	

#####################################################################################
# HELPER FUNCTIONS 
#####################################################################################

def create_environment(config):
    """ Creates environment in ENVIRONMENT from the passed configuration. """
    
    url = config.get("host") + ":" + str(config.get("port"))
    collectd.info("URL: " + url)

    viserver = VIServer()
    viserver.connect(url, config.get("username"), config.get("password"))

    if(viserver == None or viserver.is_connected() == False):
        return

    env = {}
    env['conn'] = viserver
    env['pm'] = viserver.get_performance_manager()

    hosts = viserver.get_hosts().items()
    if(len(hosts) == 0):
        collectd.info("vCenter " + config.get("name") + " does not contain any hosts. Cannot continue")
        return

    host_key = hosts[0][0]
    env['lookup_host'] = env['pm'].get_entity_counters(host_key)

    vms = viserver.get_registered_vms()
    if(len(vms) == 0):
        collectd.info("vCenter " + config.get("name") + " does not contain any VMs. Cannot continue")
        return

    vm = viserver.get_vm_by_path(vms[0])
    vm_mor = vm._mor
    env['lookup_vm'] = env['pm'].get_entity_counters(vm_mor)

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

    def __init__(self, pm, entity_key, metric_ids, vc_name, cluster_name, host_name):
        threading.Thread.__init__(self)
        self.pm = pm
        self.entity_key = entity_key
        self.metric_ids = metric_ids
        self.vc_name = vc_name
        self.cluster_name = cluster_name
        self.host_name = host_name

    def run(self):
        self.stats = self.pm.get_entity_statistic(self.entity_key, self.metric_ids, None, False)

class InventoryWatchDog(threading.Thread):
    """ The Inventory Watch Dog is a thread that is spawned for every vCenter
    configured. It updates the the passed inventory object to reflect the
    current state of the environment. It repeats the inventorization every
    10min by default."""   

    conn = None
    inventory = None
    sleepSeconds = None

    def __init__(self, conn, inventory, sleepSeconds):
        threading.Thread.__init__(self)
        self.conn = conn
        self.inventory = inventory
        self.sleepSeconds = sleepSeconds

    def run(self):

        host_count = 0
        cluster_count = 0

        while True:
            collectd.info("Running inventory refresh ...")

            host_inventory = {}
            for cluster_data in self.conn.get_clusters().items():
                cluster = cluster_data[0]
                cluster_name = cluster_data[1]
                cluster_count += 1

                hosts = {}
                for hData in self.conn.get_hosts(cluster).items():
                    host = hData[0]
                    host_name = hData[1]
                    hosts[host_name] = host
                    host_count += 1

                host_inventory[cluster_name] = {
                    'cluster': cluster,   
                    'hosts': hosts
                }

            self.inventory['host_inventory'] = host_inventory
            
            collectd.info("Found " + str(host_count) + " hosts in " + str(cluster_count) + " clusters. Next refresh in " + str(self.sleepSeconds) + "s")
            time.sleep(self.sleepSeconds)

#####################################################################################
# COLLECTD REGISTRATION 
#####################################################################################

try:
    collectd.register_config(configure_callback)
    collectd.register_init(init_callback)
    collectd.register_read(read_callback)
    collectd.register_shutdown(shutdown_callback)
except:
    pass
