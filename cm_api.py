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

    def refresh_parcel_repos(self):
        """Refresh parcel information.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/cm/commands/'
                                    'refreshParcelRepos').format(self.api_version)).json()

    def activate_cluster_parcel(self, cluster_name, product, version):
        """Activate a parcel on the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            product (:obj:`str`): The product to deactivate.
            version (:obj:`str`): The version to deactivate.

        Returns:
             A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/parcels/products/{}/'
                                    'versions/{}/commands/activate').format(self.api_version,
                                                                            cluster_name,
                                                                            product,
                                                                            version)).json()

    def deactivate_cluster_parcel(self, cluster_name, product, version):
        """Deactivate a parcel on the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            product (:obj:`str`): The product to deactivate.
            version (:obj:`str`): The version to deactivate.

        Returns:
             A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/parcels/products/{}/'
                                    'versions/{}/commands/deactivate').format(self.api_version,
                                                                              cluster_name,
                                                                              product,
                                                                              version)).json()

    def distribute_cluster_parcel(self, cluster_name, product, version):
        """Distribute parcel on the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            product (:obj:`str`): The product to deactivate.
            version (:obj:`str`): The version to deactivate.

        Returns:
             A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/parcels/products/{}/'
                                    'versions/{}/commands/'
                                    'startDistribution').format(self.api_version,
                                                                cluster_name,
                                                                product,
                                                                version)).json()

    def download_cluster_parcel(self, cluster_name, product, version):
        """Download parcel on the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            product (:obj:`str`): The product to deactivate.
            version (:obj:`str`): The version to deactivate.

        Returns:
             A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/parcels/products/{}/'
                                    'versions/{}/commands/startDownload').format(self.api_version,
                                                                                 cluster_name,
                                                                                 product,
                                                                                 version)).json()

    def remove_distributed_cluster_parcel(self, cluster_name, product, version):
        """Remove distributed parcel on the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            product (:obj:`str`): The product to deactivate.
            version (:obj:`str`): The version to deactivate.

        Returns:
             A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/parcels/products/{}/'
                                    'versions/{}/commands/'
                                    'startRemovalOfDistribution').format(self.api_version,
                                                                         cluster_name,
                                                                         product,
                                                                         version)).json()

    def remove_downloaded_cluster_parcel(self, cluster_name, product, version):
        """Remove downloaded parcel on the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            product (:obj:`str`): The product to deactivate.
            version (:obj:`str`): The version to deactivate.

        Returns:
             A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/parcels/products/{}/'
                                    'versions/{}/commands/removeDownload').format(self.api_version,
                                                                                  cluster_name,
                                                                                  product,
                                                                                  version)).json()

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

    def create_cluster_services(self, cluster_name, service_list):
        """Create a list of services.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_list (:obj:`dict`)

        Returns:
            A dictionary (service list) of the created services.
        """
        return self._post(endpoint='{}/clusters/{}/services'.format(self.api_version,
                                                                    cluster_name),
                          data=service_list).json()

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

    def get_service_role_config_group_config(self, cluster_name, service_name,
                                             role_config_group_name, view='summary'):
        """Get the service role config group configuration.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.
            role_config_group_name (:obj:`str`): The name of the role config group.
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary (config list) of the current service role config group configuration.
        """
        return self._get(endpoint=('{}/clusters/{}/services/{}/'
                                   'roleConfigGroups/{}/config').format(self.api_version,
                                                                        cluster_name,
                                                                        service_name,
                                                                        role_config_group_name),
                         params={'view': view}).json()

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

    def deploy_cluster_kerberos_client_config(self, cluster_name, host_ref_list):
        """Deploy cluster Kerberos client config.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_ref_list (:obj:`dict`)

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/commands/'
                                    'deployClusterClientConfig').format(self.api_version,
                                                                        cluster_name),
                          data=host_ref_list).json()

    def first_run_cluster_service(self, cluster_name, service_name):
        """Run firstRun on a service from the cluster. This command prepares and starts the service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/services/{}/'
                                    'commands/firstRun').format(self.api_version,
                                                                cluster_name,
                                                                service_name)).json()

    def restart_cluster_service(self, cluster_name, service_name):
        """Restart a cluster service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/services/{}/'
                                    'commands/restart').format(self.api_version,
                                                               cluster_name,
                                                               service_name)).json()

    def start_cluster_service(self, cluster_name, service_name):
        """Start a cluster service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/services/{}/'
                                    'commands/start').format(self.api_version,
                                                             cluster_name,
                                                             service_name)).json()

    def stop_cluster_service(self, cluster_name, service_name):
        """Stop a cluster service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/services/{}/'
                                    'commands/stop').format(self.api_version,
                                                            cluster_name,
                                                            service_name)).json()

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
        return self._post(endpoint=('{}/clusters/{}/services/{}/commands/'
                                    'hiveUpdateMetastoreNamenodes').format(self.api_version,
                                                                           cluster_name,
                                                                           service_name)).json()

    def format_hdfs_namenodes(self, cluster_name, service_name):
        """Format HDFS NameNodes.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the HDFS service.

        Returns:
            A dictionary (command) of the submitted command.
        """
        role_name = [role['name'] for role in self.get_service_roles(cluster_name, service_name)['items']
                     if role['type'] == 'NAMENODE']
        role_name_list = {'items': role_name}
        return self._post(endpoint=('{}/clusters/{}/services/{}/roleCommands/'
                                    'hdfsFormat').format(self.api_version,
                                                         cluster_name,
                                                         service_name),
                          data=role_name_list).json()

    def get_cm_config(self, view='summary'):
        """Get CM configuration values.

        Args:
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary (config list) of updated config values.
        """
        return self._get(endpoint='{}/cm/config'.format(self.api_version),
                         params=dict(view=view)).json()

    def update_cm_config(self, config_list):
        """Update CM configuration values.

        Args:
            config_list (:obj:`dict`)

        Returns:
            An dictionary (config list) of updated config values.
        """
        return self._put(endpoint='{}/cm/config'.format(self.api_version),
                         data=config_list).json()

    def import_admin_credentials(self, username, password):
        """Import KDC admin credentials that CM needs to create Kerberos principals.

        Args:
            username (:obj:`str`): CM principal username.
            password (:obj:`str`): CM principal password.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint='{}/cm/commands/importAdminCredentials'.format(self.api_version),
                          params=dict(username=username, password=password)).json()

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
                                                                 cluster_name)).json()

    def configure_cluster_for_kerberos(self, cluster_name):
        """Configure the cluster to use Kerberos.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint=('{}/clusters/{}/commands/'
                                    'configureForKerberos').format(self.api_version,
                                                                   cluster_name),
                          data={}).json()

    def get_cm_kerberos_principals(self):
        """Get list of Kerberos principals needed by the services being managed by Cloudera Manager.

        Returns:
            A dictionary (principal) of Kerberos principals needed by the services being managed by Cloudera Manager.
        """
        return self._get(endpoint='{}/cm/kerberosPrincipals'.format(self.api_version)).json()

    def start_all_cluster_services(self, cluster_name):
        """Start all cluster services in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint='{}/clusters/{}/commands/start'.format(self.api_version,
                                                                          cluster_name)).json()

    def stop_all_cluster_services(self, cluster_name):
        """Stop all cluster services in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint='{}/clusters/{}/commands/stop'.format(self.api_version,
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

    def begin_trial(self):
        """Begin the trial license for this Cloudera Manager instance.
        This allows the user to have enterprise-level features for a 60-day trial period.
        """
        self._post(endpoint='{}/cm/trial/begin'.format(self.api_version))

    def create_cm_roles(self, role_list):
        """Create Cloudera Manager roles.

        Args:
            role_list (:obj:`list`)

        Returns:
            A list (role list) of the created Cloudera Manager roles.
        """
        return self._post(endpoint='{}/cm/service/roles'.format(self.api_version),
                          data=role_list).json()

    def update_cm_service_role_config_group_config(self, role_config_group_name, config_list):
        """Update the configuration values for  Cloudera Manager service role config group.

        Args:
            role_config_group_name (:obj:`str`): The name of the role config group.
            config_list (:obj:`dict`)

        Returns:
            A dictionary (config list) of the updated service role config group configuration.
        """
        return self._put(endpoint=('{}/cm/service/roleConfigGroups/{}/config').format(self.api_version,
                                                                                      role_config_group_name),
                         data=config_list).json()

    def get_command_information(self, command_id):
        """Get detailed information on an asynchronous command.

        Args:
            command_id (:obj:`str`): The command ID.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._get(endpoint='{}/commands/{}'.format(self.api_version,
                                                          command_id)).json()

    def inspect_hosts(self, cluster_name):
        """Inspect hosts in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (command) of the submitted command.
        """
        return self._post(endpoint='{}/clusters/{}/commands/inspectHosts'.format(self.api_version,
                                                                                 cluster_name)).json()

    def list_all_clusters(self):
        """List clusters.

        Returns:
            A list of the clusters.
        """
        return self._get(endpoint='{}/clusters'.format(self.api_version)).json()

    def download_command_output(self, command_id):
        """Return the output of download of the command.

        Args:
            command_id (:obj:`str`): The command ID.

        Returns:
            Contents of the download.
        """
        return self._get(endpoint='/command/{}/download'.format(command_id),  endpoint_suffix='/cmf').json()

    def _get_api_version(self):
        api_version = self._get(endpoint='/version').text
        if not api_version.startswith('v'):
            raise Exception('/api/version returned unexpected result ({}).'.format(api_version))
        else:
            logger.info('Detected CM API %s.', api_version)
            return api_version

    def _get(self, endpoint, endpoint_suffix='/api', params=None):
        url = join_url_parts(self.server_url, endpoint_suffix, endpoint)
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
