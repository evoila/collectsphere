import sys

class Conf(object):
    key = None
    values = ()
    children = []
   
    def __init__(self, key, values, children=None):
        self.children = children
        self.key = key
        self.values = values

class PluginData(object):
    host = None
    plugin = None
    plugin_instance = None
    time = None
    type = None  # as specified in types.db
    type_instance = None
    
    def __init__(self, host=None, plugin=None, plugin_instance=None, time=None, type=None, type_instance=None):
        self.host = host
        self.plugin = plugin
        self.plugin_instance = plugin_instance
        self.time = time
        self.type = type
        self.type_instance = type_instance

class Values(PluginData):
    values = None
    meta = None

    def __init__(self, host=None, plugin=None, plugin_instance=None, time=None, type=None, type_instance=None, values=None, meta=None):
        PluginData.__init__(self, host, plugin, plugin_instance, time, type, type_instance)
        self.values = values
        self.meta = meta
 
    def dispatch(self, time=None, type=None, type_instance=None, values=None, meta=None):    
        if not time: time = self.time
        if not type: type = self.type
        if not type_instance: type_instance = self.type_instance
        if not values: values = self.values
        if not meta: meta = self.meta
        #print("host=%s, plugin=%s, plugin_instance=%s, time=%s, type=%s, type_instance=%s, values=%s, meta=%s" % (self.host, self.plugin, self.plugin_instance, time, type, type_instance, values, meta))    
    
class Notfication(PluginData):
    message = None
    severity = None
    
    def dispatch(self, type, values, plugin_instance, type_instance, plugin, host, time, interval):
        return
    
def info(message):
    sys.stderr.write("collectd [INFO]: " + message + "\n")

def warning(message):
    sys.stderr.write("collectd [WARNING]: " + message + "\n")
    
def error(message):
    sys.stderr.write("collectd [ERROR]: " + message + "\n")

def register_config(func):
    return

def register_read(func):
    return

def register_write(func):
    return
