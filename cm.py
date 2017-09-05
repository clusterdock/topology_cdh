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
from topology_cdh import cm_api
from .cm_api import ApiClient, DEFAULT_CM_PASSWORD, DEFAULT_CM_USERNAME


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

    def get_cluster_parcel_usage(self, cluster_name):
        """Get detailed parcel usage for a cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A dictionary (parcel usage) of the parcels in use on the cluster.
        """
        return self.api_client.get_cluster_parcel_usage(cluster_name=cluster_name)

    def get_host(self, host_id):
        """Get information about a specific host in the deployment.

        Args:
            host_id (:obj:`str`): The host ID of the host.

        Returns:
            A dictionary of information about the host.
        """
        return self.api_client.get_host(host_id=host_id)

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

    def delete_cluster_service(self, cluster_name, service_name):
        """Deletes a service from the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.
            service_name (:obj:`str`): The name of the service.

        Returns:
            The deleted service.
        """
        return self.api_client.delete_cluster_service(cluster_name=cluster_name,
                                                      service_name=service_name)

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

    def update_hive_metastore_namenodes(self, cluster_name, service_name):
        return self.api_client.update_hive_metastore_namenodes(cluster_name=cluster_name,
                                                               service_name=service_name)

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

    def deploy_cluster_client_config(self, cluster_name):
        """Deploy the cluster-wide client configuration.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A command.
        """
        return self.api_client.deploy_cluster_client_config(cluster_name=cluster_name)

    def start_all_cluster_services(self, cluster_name):
        """Start all cluster services in the cluster.

        Args:
            cluster_name (:obj:`str`): The name of the cluster.

        Returns:
            A command.
        """
        return self.api_client.start_all_cluster_services(cluster_name=cluster_name)

    def get_cm_service(self, view='summary'):
        """Get Cloudera Manager Services service.

        Args:
            view (:obj:`str`, optional): The collection view. Could be ``summary`` or ``full``.
                Default: ``summary``

        Returns:
            A service.
        """
        return self.api_client.get_cm_service(view=view)

    def start_cm_service(self):
        """Start the Cloudera Manager Services.

        Returns:
            A command.
        """
        return self.api_client.start_cm_service()
