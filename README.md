# collectsphere
This is a Collectd Plugin for VMware vSphere. It fetches performance data via the vSphere API at very good performance
which makes it suitable for medium to large environments.

## Installation
Run
> python setup.py install

This would install the collectsphere module. Add the `collectsphere.conf ` in the including collect configuration folder
or add this to the collectd configuration file:
```
<LoadPlugin "python">
    Globals true
</LoadPlugin>

<Plugin "python">
    Import "collectsphere"
    <Module "collectsphere">
        Name "SOME_NAME"
        Host "FQDN"
        Port "443"
        Verbose "True"
        VerifyCertificate "True"
        Username "USERNAME"
        Password "PASSWORD"
        Host_Counters "cpu.usage,mem.usage,disk.usage"
        VM_Counters "cpu.usage,mem.usage"
    </Module>
</Plugin>
```


## Configuration
Key | Description
 --- | ---
**Name** | The `Name` is a name for the environment. At the moment this is only used for internal usage.
**Host** | The `Host` is the FQDN of the vSphere or an IP, e.g. *vsphere.local* or *10.0.0.10*. 
**Port** | The Port is the same port as the vSphere web client access port.
**VerifyCertificate** | When using self-signed certificates set this option to False
**Username** | A user with `System.View` privileges.
**Password** | Password for user authentication
**Counters** | ;TBD

## Features
- Fetch ESXi host metrics
- Fetch VM metrics
- Awareness of VMware HA/DRS clusters
- Very good performance
- Automatic inventory discovery

## Testing
Development and testing was conducted with ...
- vSphere version: 5.5
- vCenter type: linux appliance 

## Why Collectd?
A Collectd plugin allows to use existing infrastructure in collectd to start/stop/execute the plugin and forward values
to existing backends. Like this you can easily send the information gathered to for example Graphite and create nice
looking dashboards with e.g. Grafana. (This was the primary goal of this project.)

# Screenshot from Grafana
![](https://raw.githubusercontent.com/evoila/collectsphere/master/screenshots/vms-view.png)

