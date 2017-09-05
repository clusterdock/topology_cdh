# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import requests

from clusterdock.utils import join_url_parts

DEFAULT_CM_USERNAME = 'admin'  #:
DEFAULT_CM_PASSWORD = 'admin'  #:

REQUIRED_HEADERS = {'Content-Type': 'application/json'}

logger = logging.getLogger('clusterdock.{}'.format(__name__))


class ApiClient:
    """API client to communicate with a Cloudera Manager instance.

    Args:
        server_url (:obj:`str`): Cloudera Manager server URL (including port).
        username (:obj:`str`, optional): Cloudera Manager username. Default:
            :py:const:`DEFAULT_CM_USERNAME`
        password (:obj:`str`, optional): Cloudera Manager password. Default:
            :py:const:`DEFAULT_CM_PASSWORD`
    """
    def __init__(self,
                 server_url,
                 username=DEFAULT_CM_USERNAME,
                 password=DEFAULT_CM_PASSWORD):
        self.server_url = server_url

        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update(REQUIRED_HEADERS)

        self.api_version = self._get_api_version()

    def get_all_hosts(self, view='summary'):
        """Get information about all the hosts in the deployment.

        Args:
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary (host ref list) of the hosts in the deployment.
        """
        return self._get(endpoint='{}/hosts'.format(self.api_version),
                         params=dict(view=view)).json()

    def get_cluster_parcels(self, cluster_name, view='summary'):
        """Get a list of all parcels to which a cluster has access.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary (parcel list) of the parcels to which a cluster has access.
        """
        return self._get(endpoint='{}/clusters/{}/parcels'.format(self.api_version,
                                                                  cluster_name),
                         params={'view': view}).json()

    def get_cluster_parcel_usage(self, cluster_name):
        """Get detailed parcel usage for a cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (parcel usage) of the parcels in use on the cluster.
        """
        return self._get(endpoint='{}/clusters/{}/parcels/usage'.format(self.api_version,
                                                                        cluster_name)).json()

    def get_host(self, host_id):
        """Get information about a specific host in the deployment.

        Args:
            host_id (:obj:`str`): The host ID of the host.

        Returns:
            A dictionary of information about the host.
        """
        return self._get(endpoint='{}/hosts/{}'.format(self.api_version,
                                                       host_id)).json()

    def get_cluster_hosts(self, cluster_name):
        """Get information about the hosts associated with the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (host ref list) of the hosts associated with the cluster.
        """
        return self._get(endpoint='{}/clusters/{}/hosts'.format(self.api_version,
                                                                cluster_name)).json()

    def add_cluster_hosts(self, cluster_name, host_ref_list):
        """Add hosts to the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_ref_list (:obj:`dict`)

        Returns:
            A dictionary (host ref list) of the hosts added to the cluster.
        """
        return self._post(endpoint='{}/clusters/{}/hosts'.format(self.api_version,
                                                                 cluster_name),
                          data=host_ref_list).json()

    def get_cluster_services(self, cluster_name, view='summary'):
        """Get a list of all services in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary (service list) of all services in the cluster.
        """
        return self._get(endpoint='{}/clusters/{}/services'.format(self.api_version,
                                                                   cluster_name),
                         params={'view': view}).json()

    def delete_cluster_service(self, cluster_name, service_name):
        """Deletes a service from the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (service) of details of the deleted service.
        """
        return self._delete(endpoint='{}/clusters/{}/services/{}'.format(self.api_version,
                                                                         cluster_name,
                                                                         service_name)).json()

    def get_service_roles(self, cluster_name, service_name):
        """Get a list of roles of a given service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (role list) of the roles of the service.
        """
        return self._get(endpoint='{}/clusters/{}/services/{}/roles'.format(self.api_version,
                                                                            cluster_name,
                                                                            service_name)).json()

    def get_service_role_config_groups(self, cluster_name, service_name):
        """Get a list of role config groups of a given service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (role config group list) of the role config groups of the service.
        """
        return self._get(endpoint=('{}/clusters/{}/services/{}/'
                                   'roleConfigGroups').format(self.api_version,
                                                              cluster_name,
                                                              service_name)).json()

    def update_service_role_config_group_config(self, cluster_name, service_name,
                                                role_config_group_name, config_list):
        """Update the service role config group configuration values.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.
            role_config_group_name (:obj:`str`): The name of the role config group.
            config_list (:obj:`dict`)

        Returns:
            A dictionary (config list) of the updated service role config group configuration.
        """
        return self._put(endpoint=('{}/clusters/{}/services/{}/'
                                   'roleConfigGroups/{}/config').format(self.api_version,
                                                                        cluster_name,
                                                                        service_name,
                                                                        role_config_group_name),
                         data=config_list).json()

    def update_service_config(self, cluster_name, service_name, service_config):
        """Update the service configuration values.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.
            service_config (:obj:`dict`)

        Returns:
            A dictionary (service config) of the new service configuration.
        """
        return self._put(endpoint='{}/clusters/{}/services/{}/config'.format(self.api_version,
                                                                             cluster_name,
                                                                             service_name),
                         data=service_config).json()

    def update_all_hosts_config(self, config_list):
        """Update the default configuration values for all hosts.

        Args:
            config_list (:obj:`dict`)

        Returns:
            A dictionary (config list) of updated config values.
        """
        return self._put(endpoint='{}/cm/allHosts/config'.format(self.api_version),
                         data=config_list).json()

    def update_hive_metastore_namenodes(self, cluster_name, service_name):
        """Update the Hive Metastore to point to a NameNode's Nameservice.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the Hive service.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(
            endpoint=('{}/clusters/{}/services/{}/'
                      'commands/hiveUpdateMetastoreNamenodes').format(self.api_version,
                                                                      cluster_name,
                                                                      service_name)
        ).json()

    def update_cm_config(self, config_list):
        """Update CM configuration values.

        Args:
            config_list (:obj:`dict`)

        Returns:
            An dictionary (config list) of updated config values.
        """
        return self._put(endpoint='{}/cm/config'.format(self.api_version),
                         data=config_list).json()

    def create_host_templates(self, cluster_name, host_template_list):
        """Create new host templates.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_template_list (:obj:`dict`)

        Returns:
            A dictionary (host template list) of the created host templates.
        """
        return self._post(endpoint='{}/clusters/{}/hostTemplates'.format(self.api_version,
                                                                         cluster_name),
                          data=host_template_list).json()

    def apply_host_template(self, cluster_name, host_template_name, start_roles, host_ref_list):
        """Apply a host template to a collection of hosts.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_template_name (:obj:`str`): The name of the host template.
            start_roles (:obj:`bool`): Start the newly-created roles.
            host_ref_list (:obj:`dict`)

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/hostTemplates/{}/'
                                    'commands/applyHostTemplate').format(self.api_version,
                                                                         cluster_name,
                                                                         host_template_name),
                          params={'startRoles': start_roles},
                          data=host_ref_list).json()

    def deploy_cluster_client_config(self, cluster_name):
        """Deploy the cluster-wide client configuration.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/commands/'
                                    'deployClientConfig').format(self.api_version,
                                                                 cluster_name).json())

    def start_all_cluster_services(self, cluster_name):
        """Start all cluster services in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint='{}/clusters/{}/commands/start'.format(self.api_version,
                                                                          cluster_name)).json()

    def get_cm_service(self, view='summary'):
        """Get Cloudera Manager Services service.

        Args:
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary (service) of the Cloudera Manager Services service.
        """
        return self._get(endpoint='{}/cm/service'.format(self.api_version),
                         params=dict(view=view)).json()

    def start_cm_service(self):
        """Start the Cloudera Manager Services.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint='{}/cm/service/commands/start'.format(self.api_version)).json()

    def get_command_information(self, command_id):
        """Get detailed information on an asynchronous command.

        Args:
            command_id (:obj:`str`): The command ID.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._get(endpoint='{}/commands/{}'.format(self.api_version,
                                                          command_id)).json()

    def _get_api_version(self):
        api_version = self._get(endpoint='/version').text
        if not api_version.startswith('v'):
            raise Exception('/api/version returned unexpected result ({}).'.format(api_version))
        else:
            logger.info('Detected CM API %s.', api_version)
            return api_version

    def _get(self, endpoint, params=None):
        url = join_url_parts(self.server_url, '/api', endpoint)
        logger.debug('Sending GET request to URL (%s) with parameters (%s) ...',
                     url,
                     params or 'None')
        response = self.session.get(url, params=params or {})
        if response.status_code != requests.codes.ok:
            logger.error(response.text)
        response.raise_for_status()
        return response

    def _post(self, endpoint, params=None, data=None):
        url = join_url_parts(self.server_url, '/api', endpoint)
        data = json.dumps(data)
        logger.debug('Sending POST request to URL (%s) with parameters (%s) and data (%s) ...',
                     url,
                     params or 'None',
                     data or 'None')
        response = self.session.post(url, params=params or {}, data=data)
        if response.status_code != requests.codes.ok:
            logger.error(response.text)
        response.raise_for_status()
        return response

    def _delete(self, endpoint, params=None, data=None):
        url = join_url_parts(self.server_url, '/api', endpoint)
        data = json.dumps(data)
        logger.debug('Sending POST request to URL (%s) with parameters (%s) and data (%s) ...',
                     url,
                     params or 'None',
                     data or 'None')
        response = self.session.delete(url, params=params or {}, data=data)
        if response.status_code != requests.codes.ok:
            logger.error(response.text)
        response.raise_for_status()
        return response

    def _put(self, endpoint, params=None, data=None):
        url = join_url_parts(self.server_url, '/api', endpoint)
        data = json.dumps(data)
        logger.debug('Sending PUT request to URL (%s) with parameters (%s) and data (%s) ...',
                     url,
                     params or 'None',
                     data or 'None')
        response = self.session.put(url, params=params or {}, data=data)
        if response.status_code != requests.codes.ok:
            logger.error(response.text)
        response.raise_for_status()
        return response
