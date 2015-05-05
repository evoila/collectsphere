#!/usr/bin/env python

"""
This script is used to execute the collectd plugin from the command line
without collectd. It uses the mock of the collectd framework in collectd.py to
initialize the plugin as collectd would.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import time
import collectsphere
from collectd import Conf
import sys   
 
def main():
    """ The plugin is initialized and executed once. This is intended for test
    and dev and measures execution times. """

    # This mocks the configuration of collectd. A proper example of a
    # configuration file can be found in collectsphere.conf
    children = []
    children.append(Conf('Name', ('NAME',)))
children.append(Conf('Host', ('FQDN',)))
    children.append(Conf('Port', ('443',)))
    children.append(Conf('Verbose', ('True',)))
    children.append(Conf('Username', ('USERNAME',)))
    children.append(Conf('Password', ('PASSWORD',)))
    children.append(Conf('Host_Counters', ('cpu.usage,mem.usage,disk.usage',)))
    children.append(Conf('VM_Counters', ('cpu.usage,mem.usage',)))
    children.append(Conf('Inventory_Refresh_Interval', ('600',)))
    conf = Conf(None, None, children)
    
    # Configure and initialize the plugin
    start_time = time.time()
    
    collectsphere.configure_callback(conf)
    collectsphere.init_callback()
    
    elapsed_time = time.time() - start_time      
    sys.stderr.write("Conf/Init Time: " + str(elapsed_time) + "\n")    

    # Execute the plugin once
    start_time = time.time() 

    collectsphere.read_callback()
        
    elapsed_time = time.time() - start_time      
    sys.stderr.write("Read Time: " + str(elapsed_time) + "\n")

if __name__ == "__main__":
    main()
