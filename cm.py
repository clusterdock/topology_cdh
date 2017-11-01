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
import logging
from time import sleep

from clusterdock.utils import wait_for_condition

from topology_cdh import cm_api

logger = logging.getLogger('clusterdock.{}'.format(__name__))


class ParcelNotFoundError(Exception):
    pass


class ClouderaManagerParcel:
    def __init__(self,
                 cluster,
                 product,
                 version,
                 stage):
        self.cluster = cluster
        self.product = product
        self.version = version
        self.stage = stage

    def download(self, timeout=300, time_to_success=0):
        logger.info('Downloading parcel (product = %s, version = %s) on cluster %s ...',
                    self.product,
                    self.version,
                    self.cluster.name)
        command = self.cluster.api_client.download_cluster_parcel(cluster_name=self.cluster.name,
                                                                  product=self.product,
                                                                  version=self.version)
        if not command['active'] and not command['success']:
            raise Exception('{} parcel failed to download.'.format(self.product))
        self.wait_for_stage('DOWNLOADED', timeout=timeout, time_to_success=time_to_success)
        return self

    def distribute(self, timeout=300, time_to_success=0):
        logger.info('Distributing parcel (product = %s, version = %s) on cluster %s ...',
                    self.product,
                    self.version,
                    self.cluster.name)
        command = self.cluster.api_client.distribute_cluster_parcel(cluster_name=self.cluster.name,
                                                                    product=self.product,
                                                                    version=self.version)
        if not command['active'] and not command['success']:
            raise Exception('{} parcel failed to distribute.'.format(self.product))
        self.wait_for_stage('DISTRIBUTED', timeout=timeout, time_to_success=time_to_success)
        return self

    def activate(self, timeout=300, time_to_success=0):
        logger.info('Activating parcel (product = %s, version = %s) on cluster %s ...',
                    self.product,
                    self.version,
                    self.cluster.name)
        command = self.cluster.api_client.activate_cluster_parcel(cluster_name=self.cluster.name,
                                                                  product=self.product,
                                                                  version=self.version)
        if not command['active'] and not command['success']:
            raise Exception('{} parcel failed to activate.'.format(self.product))
        self.wait_for_stage('ACTIVATED', timeout=timeout, time_to_success=time_to_success)
        return self

    def deactivate(self, timeout=300, time_to_success=0):
        logger.info('Deactivating parcel (product = %s, version = %s) on cluster %s ...',
                    self.product,
                    self.version,
                    self.cluster.name)
        command = self.cluster.api_client.deactivate_cluster_parcel(cluster_name=self.cluster.name,
                                                                  product=self.product,
                                                                  version=self.version)
        if not command['active'] and not command['success']:
            raise Exception('{} parcel failed to deactivate.'.format(self.product))
        self.wait_for_stage('DISTRIBUTED', timeout=timeout, time_to_success=time_to_success)
        return self

    def wait_for_stage(self, stage, timeout=300, time_to_success=0):
        def condition():
            for parcel in self.cluster.parcels:
                if parcel.product == self.product and parcel.version == self.version:
                    break
            else:
                detected_parcels = ', '.join('{}-{}'.format(parcel.product, parcel.version)
                                             for parcel in self.cluster.parcels)
                raise ParcelNotFoundError('Could not find parcel (product = {}, version = {}). '
                                          'Detected parcels: {}.'.format(self.product,
                                                                         self.version,
                                                                         detected_parcels))
            logger.debug('%s parcel is in stage %s ...', self.product, parcel.stage)
            return parcel.stage == stage

        def success(time):
            logger.debug('%s parcel reached stage %s after %s seconds.', self.product, stage, time)

        def failure(timeout):
            raise TimeoutError('Timed out after {} seconds waiting for {} parcel '
                               'to reach stage {}.'.format(timeout, self.product, stage))

        return wait_for_condition(condition=condition,
                                  time_between_checks=3, timeout=timeout,
                                  time_to_success=time_to_success,
                                  success=success, failure=failure)


class ClouderaManagerCluster:
    def __init__(self,
                 api_client,
                 name):
        self.api_client = api_client
        self.name = name

    @property
    def parcels(self):
        return [ClouderaManagerParcel(cluster=self,
                                      product=parcel['product'],
                                      version=parcel['version'],
                                      stage=parcel['stage'])
                for parcel
                in self.api_client.get_cluster_parcels(cluster_name=self.name,
                                                       view='full')['items']]

    def parcel(self, product=None, version=None, stage=None):
        if not product and not version and not stage:
            raise Exception('A product and/or version and/or stage must '
                            'be specified to select a parcel.')
        return next((parcel
                    for parcel in self.parcels
                    if (not product or parcel.product == product)
                    and (not version or parcel.version.split('-')[0] == version)
                    and (not stage or parcel.stage == stage)), None)

    def wait_for_parcel_stage(self, product, version=None, stage=None):
        def condition(product, version, stage):
            parcel = self.parcel(product=product, version=version, stage=stage)
            return parcel is not None

        def success(time):
            logger.debug('%s parcel with %s version found in %s stage after %s seconds.', product, version, stage, time)

        def failure(timeout):
            raise TimeoutError('Timed out after {} seconds waiting for {} parcel with {} version'
                               ' in the {} stage.'.format(timeout, product, version, stage))
        wait_for_condition(condition=condition, condition_args=[product, version, stage],
                           time_between_checks=3, timeout=540, success=success, failure=failure)

    def deploy_client_config(self):
        command_id = self.api_client.deploy_cluster_client_config(cluster_name=self.name)['id']

        def condition(command_id):
            command_information = self.api_client.get_command_information(command_id)
            active = command_information.get('active')
            success = command_information.get('success')
            result_message = command_information.get('resultMessage')
            logger.debug('Deploy cluster client config command: (active: %s, success: %s)',
                         active, success)
            if not active and not success:
                if 'not currently available for execution' in result_message:
                    logger.debug('Deploy cluster client config execution not '
                                 'currently available. Continuing ...')
                    return True
                raise Exception('Failed to deploy cluster config.')
            return not active and success

        def success(time):
            logger.debug('Deployed cluster client config in %s seconds.', time)

        def failure(timeout):
            raise TimeoutError('Timed out after {} seconds waiting '
                               'for cluster client config to deploy.'.format(timeout))
        wait_for_condition(condition=condition, condition_args=[command_id],
                           time_between_checks=3, timeout=180, success=success, failure=failure)

    def start(self):
        command_id = self.api_client.start_all_cluster_services(cluster_name=self.name)['id']

        def condition(command_id):
            command_information = self.api_client.get_command_information(command_id)
            active = command_information.get('active')
            success = command_information.get('success')
            logger.debug('Start cluster command: (active: %s, success: %s)', active, success)
            if not active and not success:
                raise Exception('Failed to start cluster.')
            return not active and success

        def success(time):
            logger.debug('Started cluster in %s seconds.', time)

        def failure(timeout):
            raise TimeoutError('Timed out after {} seconds waiting '
                               'for cluster to start.'.format(timeout))
        wait_for_condition(condition=condition, condition_args=[command_id],
                           time_between_checks=3, timeout=600, success=success, failure=failure)

    def stop(self):
        services = self.api_client.get_cluster_services(cluster_name=self.name,
                                                        view='summary')['items']
        for service in services:
            # If stop_all_cluster_services is called when none of the cluster services is started, the command fails.
            # Hence call stop_all_cluster_services only if at least one service is started.
            if service['serviceState'] == 'STARTED':
                command_id = self.api_client.stop_all_cluster_services(cluster_name=self.name)['id']
                _wait_for_command(self, command_id)
                break

    def inspect_hosts(self):
        command_id = self.api_client.inspect_hosts(cluster_name=self.name)['id']
        _wait_for_command(self, command_id)
        return command_id

    def download_command_output(self, command_id):
        return self.api_client.download_command_output(command_id)

    def get_cluster_info(self):
        items = self.api_client.list_all_clusters()['items']
        logger.info(items)
        return next((item for item in items if item['name'] == self.name), None)


class ClouderaManagerDeployment:
    """Class to interact with a Cloudera Manager deployment.

    Args:
        server_url (:obj:`str`): Cloudera Manager server URL (including port).
        username (:obj:`str`, optional): Cloudera Manager username. Default:
            :py:const:`DEFAULT_CM_USERNAME`
        password (:obj:`str`, optional): Cloudera Manager password. Default:
            :py:const:`DEFAULT_CM_PASSWORD`
    """
    def __init__(self,
                 server_url,
                 username=cm_api.DEFAULT_CM_USERNAME,
                 password=cm_api.DEFAULT_CM_PASSWORD):
        self.api_client = cm_api.ApiClient(server_url=server_url,
                                           username=username,
                                           password=password)

    def cluster(self, name):
        return ClouderaManagerCluster(api_client=self.api_client, name=name)

    def get_all_hosts(self, view='summary'):
        """Get information about all the hosts in the deployment.

        Args:
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A list of dictionaries with each representing one host in the deployment.
        """
        return self.api_client.get_all_hosts(view=view)['items']

    def get_cluster_parcels(self, cluster_name, view='summary'):
        """Get a list of all parcels to which a cluster has access.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A list of dictionaries with each representing a parcel to which the cluster has access.
        """
        return self.api_client.get_cluster_parcels(cluster_name=cluster_name,
                                                   view=view)['items']

    def refresh_parcel_repos(self):
        """Refresh parcel information.

        For CM API versions without support for the REST endpoint, this will simply sleep.
        """
        if self.api_client.api_version < 'v16':
            logger.warning('Detected API version without support '
                           'for refreshParcelRepos (%s). Sleeping instead ...',
                           self.api_client.api_version)
            sleep(30)
        else:
            command_id = self.api_client.refresh_parcel_repos()['id']

            def condition(command_id):
                command_information = self.api_client.get_command_information(command_id)
                active = command_information.get('active')
                success = command_information.get('success')
                logger.debug('Refresh parcel repos command: (active: %s, success: %s)',
                             active, success)
                if not active and not success:
                    raise Exception('Failed to refresh parcel repos.')
                return not active and success

            def success(time):
                logger.debug('Refreshed parcel repos in %s seconds.', time)

            def failure(timeout):
                raise TimeoutError('Timed out after {} seconds waiting '
                                   'for parcel repos to refresh.'.format(timeout))
            wait_for_condition(condition=condition, condition_args=[command_id],
                               time_between_checks=3, timeout=180, success=success,
                               failure=failure)

    def get_cluster_hosts(self, cluster_name):
        """Get information about the hosts associated with the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A list of dictionaries with each representing one host associated with the cluster.
        """
        return self.api_client.get_cluster_hosts(cluster_name=cluster_name)['items']

    def add_cluster_hosts(self, cluster_name, host_ids):
        """Add hosts to the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_ids (:obj:`list`): A list of host IDs of hosts to add to the cluster.

        Returns:
            A list of host IDs of hosts added to the cluster.
        """
        host_ref_list = {
            'items': [{'hostId': host_id} for host_id in host_ids]
        }
        return [host['hostId']
                for host in self.api_client.add_cluster_hosts(cluster_name=cluster_name,
                                                              host_ref_list=host_ref_list)['items']]

    def deploy_cluster_kerberos_client_config(self, cluster_name, host_ids=None):
        """Deploy cluster Kerberos client config.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_ids (:obj:`list`): A list of host IDs of hosts to which to deploy configs.
                Default: ``None``

        Returns:
            A command.
        """
        host_ref_list = {
            'items': [{'hostId': host_id} for host_id in (host_ids or [])]
        }
        return self.api_client.deploy_cluster_kerberos_client_config(cluster_name, host_ref_list)

    def get_cm_kerberos_principals(self):
        """A list of strings for Kerberos principals.

        Returns:
            A list of Kerberos principals needed by the services being managed by Cloudera Manager.
        """
        return self.api_client.get_cm_kerberos_principals()['items']

    def create_cluster_services(self, cluster_name, services):
        """Create a list of services.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            services (:obj:`list`): A list of API service dictionaries to create.

        Returns:
            A list of the created services.
        """
        service_list = {'items': services}
        return self.api_client.create_cluster_services(cluster_name=cluster_name,
                                                       service_list=service_list)['items']

    def get_cluster_services(self, cluster_name, view='summary'):
        """Get a list of all services in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A list of all services in the cluster.
        """
        return self.api_client.get_cluster_services(cluster_name=cluster_name,
                                                    view=view)['items']

    def get_service_roles(self, cluster_name, service_name):
        """Get a list of roles of a given service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A list of the roles of the service.
        """
        return self.api_client.get_service_roles(cluster_name=cluster_name,
                                                 service_name=service_name)['items']

    def get_service_role_config_groups(self, cluster_name, service_name):
        """Get a list of role config groups of a given service.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            A list of the role config groups of the service.
        """
        return self.api_client.get_service_role_config_groups(cluster_name=cluster_name,
                                                              service_name=service_name)['items']

    def update_service_role_config_group_config(self, cluster_name, service_name,
                                                role_config_group_name, configs):
        """Update the service role config group configuration values.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.
            role_config_group_name (:obj:`str`): The name of the role config group.
            configs (:obj:`dict`): Configurations to update.

        Returns:
            A dictionary of the updated service role config group configuration.
        """
        config_list = {
            'items': [{'name': name, 'value': value}
                      for name, value in configs.items()]
        }
        return self.api_client.update_service_role_config_group_config(
            cluster_name=cluster_name, service_name=service_name,
            role_config_group_name=role_config_group_name, config_list=config_list
        )['items']

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
            A dictionary of the current service role config group configuration.
        """
        return {config['name']: config.get('value') or config.get('default')
                for config in self.api_client.get_service_role_config_group_config(
                    cluster_name=cluster_name, service_name=service_name,
                    role_config_group_name=role_config_group_name,
                    view=view
                )['items']}

    def update_service_config(self, cluster_name, service_name, configs):
        """Update the service configuration values.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.
            configs (:obj:`dict`): Configurations to update.

        Returns:
            A dictionary of the new service configuration.
        """
        service_config = {
            'items': [{'name': name, 'value': value}
                      for name, value in configs.items()]
        }
        return self.api_client.update_service_config(cluster_name=cluster_name,
                                                     service_name=service_name,
                                                     service_config=service_config)['items']

    def update_all_hosts_config(self, configs):
        """Update the default configuration values for all hosts.

        Args:
            configs (:obj:`dict`): Configurations to update.

        Returns:
            A dictionary of updated config values.
        """
        config_list = {
            'items': [{'name': name, 'value': value}
                      for name, value in configs.items()]
        }
        return self.api_client.update_all_hosts_config(config_list=config_list)['items']

    def get_cm_config(self, view='summary'):
        """Update CM configuration values.

        Args:
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A dictionary of config values.
        """
        return self.api_client.get_cm_config(view=view)['items']

    def update_cm_config(self, configs):
        """Update CM configuration values.

        Args:
            configs (:obj:`dict`): Configurations to update.

        Returns:
            An dictionary of updated config values.
        """
        config_list = {
            'items': [{'name': name, 'value': value}
                      for name, value in configs.items()]
        }
        return self.api_client.update_cm_config(config_list=config_list)['items']

    def update_cm_service_role_config_group_config(self, role_config_group_name, configs):
        """Update the configuration values for  Cloudera Manager service role config group.
        Args:
            role_config_group_name (:obj:`str`): The name of the role config group.
            configs (:obj:`dict`): Configurations to update.

        Returns:
            A dictionary of the updated CM service role config group configuration.
        """
        config_list = {
            'items': [{'name': name, 'value': value}
                      for name, value in configs.items()]
        }
        return self.api_client.update_cm_service_role_config_group_config(
            role_config_group_name=role_config_group_name, config_list=config_list
        )['items']

    def create_cm_roles(self, roles):
        """Create Cloudera Manager roles.

        Args:
            roles (:obj:`list`): A list of API role dictionaries to create.

        Returns:
            A list (role list) of the created Cloudera Manager roles.
        """
        role_list = {'items': roles}
        return self.api_client.create_cm_roles(role_list)['items']

    def begin_trial(self):
        """Begin the trial license for this Cloudera Manager instance.
        This allows the user to have enterprise-level features for a 60-day trial period.
        """
        self.api_client.begin_trial()

    def create_host_template(self, host_template_name, cluster_name, role_config_group_names):
        """Create a new host template.

        Args:
            host_template_name (:obj:`str`): The name of the host template.
            cluster_name (:obj:`str`): The name of the cluster.
            role_config_group_names (:obj:`list`): A list of role config group names to add
                to the template.

        Returns:
            A list of created host templates.
        """
        host_template_list = {
            'items': [{
                'name': host_template_name,
                'clusterRef': {
                    'clusterName': cluster_name
                },
                'roleConfigGroupRefs': [{'roleConfigGroupName': role_config_group_name}
                                        for role_config_group_name in role_config_group_names]
            }]
        }
        return self.api_client.create_host_templates(cluster_name=cluster_name,
                                                     host_template_list=host_template_list)['items']

    def apply_host_template(self, cluster_name, host_template_name, start_roles, host_ids):
        """Apply a host template to a collection of hosts.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            host_template_name (:obj:`str`): The name of the host template.
            start_roles (:obj:`bool`): Start the newly-created roles.
            host_ids (:obj:`list`): A list of host IDs of hosts to which to apply the host template.

        Returns:
            A command.
        """
        host_ref_list = {
            'items': [{'hostId': host_id} for host_id in host_ids]
        }
        return self.api_client.apply_host_template(cluster_name=cluster_name,
                                                   host_template_name=host_template_name,
                                                   start_roles=start_roles,
                                                   host_ref_list=host_ref_list)

    def first_run_service(self, cluster_name, service_name, timeout=180):
        command_id = self.api_client.first_run_cluster_service(cluster_name=cluster_name,
                                                               service_name=service_name)['id']
        _wait_for_command(self, command_id, timeout)

    def restart_service(self, cluster_name, service_name, timeout=180):
        command_id = self.api_client.restart_cluster_service(cluster_name=cluster_name,
                                                             service_name=service_name)['id']
        _wait_for_command(self, command_id, timeout)

    def start_service(self, cluster_name, service_name, timeout=180):
        command_id = self.api_client.start_cluster_service(cluster_name=cluster_name,
                                                           service_name=service_name)['id']
        _wait_for_command(self, command_id, timeout)

    def stop_service(self, cluster_name, service_name, timeout=180):
        command_id = self.api_client.stop_cluster_service(cluster_name=cluster_name,
                                                          service_name=service_name)['id']
        _wait_for_command(self, command_id, timeout)

    def format_hdfs_namenodes(self, cluster_name, service_name):
        command_id = self.api_client.format_hdfs_namenodes(cluster_name,
                                                           service_name)['items'][0]['id']
        _wait_for_command(self, command_id, timeout=360)


def _wait_for_command(object, command_id, timeout=180):
    def condition(object, command_id):
        command_information = object.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        name = command_information.get('name')
        logger.debug('Run %s command: (active: %s, success: %s)', name, active, success)
        if not active and not success:
            raise Exception('Failed to run command {}.'.format(name))
        return not active and success

    def success(time):
        logger.debug('Command ran in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for command to run.'.format(timeout))
    wait_for_condition(condition=condition, condition_args=[object, command_id],
                       time_between_checks=3, timeout=timeout, success=success, failure=failure)
