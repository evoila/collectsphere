# collectsphere
This is a Collectd Plugin for VMware vSphere. It fetches performance data via the vSphere API at very good performance which makes it suitable for medium to large environments.

## Why Collectd?
A Collectd plugin allows to use existing infrastructure in collectd to start/stop/execute the plugin and forward values to existing backends. Like this you can easily send the information gathered to for example Graphite and create nice looking dashboards with e.g. Grafana. (This was the primary goal of this project.)

## Features
- Fetch ESXi host metrics
- Fetch VM metrics
- Awareness of VMware HA/DRS clusters
- Very good performance
- Automatic inventory discovery
