"""
This is a mock of the collectd framework that will be available when the plugin
actually run as a plugin to collectd. It was written based on the information
given on https://collectd.org/documentation/manpages/collectd-python.5.shtml
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import sys


class Conf(object):
    """ Collectd create Conf objects from the config file (see example in
    collectsphere.conf) and passes a single Conf object tree to the plugin on
    configuration. """

    key = None
    values = ()
    children = []

    def __init__(self, key, values, children=None):
        self.children = children
        self.key = key
        self.values = values


class PluginData(object):
    """ This is the base class for Values and Notification classes. """
    host = None
    plugin = None
    plugin_instance = None
    time = None
    type = None  # as specified in types.db
    type_instance = None

    def __init__(
            self, host=None, plugin=None, plugin_instance=None, time=None,
            type=None, type_instance=None
    ):
        self.host = host
        self.plugin = plugin
        self.plugin_instance = plugin_instance
        self.time = time
        self.type = type
        self.type_instance = type_instance

data = dict()

class Values(PluginData):
    """ Represents values a plugin collected and provides a function to
    dispatch the information to collectd. """

    values = None
    meta = None

    def __init__(
            self, host=None, plugin=None, plugin_instance=None, time=None,
            type=None, type_instance=None, values=None, meta=None
    ):
        PluginData.__init__(
            self, host, plugin, plugin_instance, time, type, type_instance
        )
        self.values = values
        self.meta = meta


    def dispatch(
            self, time=None, type=None, type_instance=None, values=None,
            meta=None
    ):
        if not time:
            time = self.time
        if not type:
            type = self.type
        if not type_instance:
            type_instance = self.type_instance
        if not values:
            values = self.values
        if not meta:
            meta = self.meta
        if type + '-' + type_instance not in data:
            data[type + '-' + type_instance] = dict()
            data[type + '-' + type_instance][time] = values
        elif time in data[type + '-' + type_instance]:
            print "Value too old"

        print(
            "host=%s, plugin=%s, plugin_instance=%s, time=%s, type=%s, type_instance=%s, values=%s, meta=%s" % (
                self.host, self.plugin, self.plugin_instance, time, type,
                type_instance, values, meta
            )
        )


class Notfication(PluginData):
    message = None
    severity = None

    def dispatch(self, type, values, plugin_instance, type_instance, plugin,
                 host, time, interval):
        return


################################################################################
# Logging functions provided by collectd. Messages passed like this run through
# collectd's logging mechanisms.
################################################################################

def info(message):
    sys.stderr.write("collectd [INFO]: " + message + "\n")


def warning(message):
    sys.stderr.write("collectd [WARNING]: " + message + "\n")


def error(message):
    sys.stderr.write("collectd [ERROR]: " + message + "\n")


################################################################################
# The plugin itself must register its methods, so that collectd knows which
# functions to call when. The config function is called once, so is the init
# function. The read function is called for once every couple of seconds or
# whatevery collecd's metric gathering interval is configured to be. The write
# function is not used in this project as this plugin is supposed to gather
# data not write it somewhere.
################################################################################

def register_config(func):
    return


def register_init(func):
    return


def register_read(callback, interval=60):
    return


def register_write(func):
    return


def register_shutdown(func):
    return
