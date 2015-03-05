Boundary Zookeeper Plugin
-----------------------------
Collects metrics from Zookeeper server.

### Platforms
- Linux

### Prerequisites
- Python 2.6 or later
- Zookeeper 3.4+

### Plugin Configuration

In order the plugin to collect statistics from Zookeeper server, it needs access to the service stats API endpoint.
In a default installation, this would be on port 2185. 
Changing the service_port setting in params.json allow the plugin to collect the metrics from zookeeper service on different port.

Additionally a service_host can be added which will set the server where the zookeeper is running.
