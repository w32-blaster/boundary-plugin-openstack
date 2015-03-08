from __future__ import (absolute_import, division, print_function, unicode_literals)
import logging
import time
import sys

import boundary_plugin
import boundary_accumulator

import ceilometerclient.client

"""
If getting statistics fails, we will retry up to this number of times before
giving up and aborting the plugin.  Use 0 for unlimited retries.
"""
PLUGIN_RETRY_COUNT = 0

"""
If getting statistics fails, we will wait this long (in seconds) before retrying.
"""
PLUGIN_RETRY_DELAY = 5

"""
Use the default openstack admin user if the setting is missing in the config file
"""
DEFAULT_USER = 'admin'

"""
Use the default openstack admin tenant if the setting is missing in the config file
"""
DEFAULT_TENANT = 'admin'

"""
Use the default openstack ceilometer endpoint if the setting is missing in the config file
"""
DEFAULT_ENDPOINT = 'http://controller:35357/v2.0'

"""
Default timeout vale
"""
DEFAULT_TIMEOUT = 1

CPU_UTIL_MAPPING = (
	('OS_CPUUTIL_AVG', 'avg', False),
	('OS_CPUUTIL_SUM', 'sum', False),
	('OS_CPUUTIL_MIN', 'min', False),
        ('OS_CPUUTIL_MAX', 'max', False)	
)

CPU_MAPPING = (
        ('OS_CPU_AVG', 'avg', False),
        ('OS_CPU_SUM', 'sum', False)
)

INSTANCE_MAPPING = (
        ('OS_INSTANCE_SUM', 'sum', False),
	('OS_INSTANCE_MAX', 'max', False)
)

MEMORY_MAPPING = (
        ('OS_MEMORY_SUM', 'sum', False),
	('OS_MEMORY_AVG', 'avg', False)
)

MEMORY_USAGE_MAPPING = (
        ('OS_MEMORY_USAGE_SUM', 'sum', False),
	('OS_MEMORY_USAGE_AVG', 'avg', False)
)

VOLUME_MAPPING = (
        ('OS_VOLUME_SUM', 'sum', False),
	('OS_VOLUME_AVG', 'avg', False)
)

IMAGE_MAPPING = (
        ('OS_IMAGE_SUM', 'sum', False),
	('OS_IMAGE_AVG', 'avg', False)
)

IMAGE_SIZE_MAPPING = (
        ('OS_IMAGE_SIZE_SUM', 'sum', False),
	('OS_IMAGE_SIZE_AVG', 'avg', False)
)

DISK_READ_MAPPING = (
        ('OS_DISK_READ_RATE_SUM', 'sum', False),
	('OS_DISK_READ_RATE_AVG', 'avg', False)
)

DISK_WRITE_MAPPING = (
        ('OS_DISK_WRITE_RATE_SUM', 'sum', False),
        ('OS_DISK_WRITE_RATE_AVG', 'avg', False)
)

NETWORK_IN_MAPPING = (
        ('OS_NETWORK_IN_BYTES_SUM', 'sum', False),
        ('OS_NETWORK_IN_BYTES_AVG', 'avg', False)
)

NETWORK_OUT_MAPPING = (
        ('OS_NETWORK_OUT_BYTES_SUM', 'sum', False),
        ('OS_NETWORK_OUT_BYTES_AVG', 'avg', False)
)

MAPPING = {'cpu_util': [CPU_UTIL_MAPPING, None], 'cpu': [CPU_MAPPING, None], 'instance': [INSTANCE_MAPPING, None], 'memory': [MEMORY_MAPPING, None], 'memory.usage': [MEMORY_USAGE_MAPPING, None], 'volume': [VOLUME_MAPPING, None], 'image': [IMAGE_MAPPING, None], 'image.size': [IMAGE_SIZE_MAPPING, None], 'disk.read.requests.rate': [DISK_READ_MAPPING, None], 'disk.write.requests.rate': [DISK_WRITE_MAPPING, None], 'network.incoming.bytes': [NETWORK_IN_MAPPING, None], 'network.outgoing.bytes': [NETWORK_OUT_MAPPING, None]}

class OpenstackPlugin(object):
    def __init__(self, boundary_metric_prefix):
        self.boundary_metric_prefix = boundary_metric_prefix
        self.settings = boundary_plugin.parse_params()
        self.accumulator = boundary_accumulator
	
	service_endpoint = self.settings.get("service_endpoint", DEFAULT_ENDPOINT)
	service_user = self.settings.get("service_user", DEFAULT_USER)
	service_tenant = self.settings.get("service_tenant", DEFAULT_TENANT)
	service_password = self.settings.get("service_password", None)

	if (service_password == None):
		raise Exception("Password is required and there is no default configured")
	
	self._timeout = self.settings.get("service_timeout", DEFAULT_TIMEOUT)
	
	self._cclient = ceilometerclient.client.get_client(2, os_username=service_user, os_password=service_password, os_tenant_name=service_tenant, os_auth_url=service_endpoint)

    def _send_cmd(self, cmd):
	return self._cclient.statistics.list(meter_name=cmd, period=300)

    def get_stats(self):
	for source in MAPPING:
	    data = self._send_cmd(source)
	    if len(data) > 0:
	    	MAPPING[source][1] = data[-1]
	return self._send_cmd('cpu_util')[-1]

    def get_stats_with_retries(self, *args, **kwargs):
        """
        Calls the get_stats function, taking into account retry configuration.
        """
        retry_range = xrange(PLUGIN_RETRY_COUNT) if PLUGIN_RETRY_COUNT > 0 else iter(int, 1)
        for _ in retry_range:
            try:
                return self.get_stats(*args, **kwargs)
            except Exception as e:
                logging.error("Error retrieving data: %s" % e)
                time.sleep(PLUGIN_RETRY_DELAY)

        logging.fatal("Max retries exceeded retrieving data")
        raise Exception("Max retries exceeded retrieving data")

    def handle_metrics(self, data):
	for group in MAPPING:
		maptuple = MAPPING[group][0]
	    	valueobj = MAPPING[group][1]
		#print (valueobj)
	    	if valueobj != None:
	    	    for boundary_name, column, accumulate in maptuple:
		        value = (getattr(valueobj, column))
            
            	        if not value:
                	    continue

            	    	if accumulate:
                	    value = self.accumulator.accumulate(metric_name, int(value) )

            	    	boundary_plugin.boundary_report_metric(self.boundary_metric_prefix + boundary_name, value)

    def main(self):
        logging.basicConfig(level=logging.ERROR, filename=self.settings.get('log_file', None))
        reports_log = self.settings.get('report_log_file', None)
        if reports_log:
            boundary_plugin.log_metrics_to_file(reports_log)

        boundary_plugin.start_keepalive_subprocess()

        while True:
            data = self.get_stats_with_retries()
            self.handle_metrics(data)
            boundary_plugin.sleep_interval()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        logging.basicConfig(level=logging.INFO)

    plugin = OpenstackPlugin('')
    plugin.main()
