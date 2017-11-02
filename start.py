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
import io
import logging
import os
import re
import socket

from configobj import ConfigObj

from clusterdock.models import Cluster, client, Node
from clusterdock.utils import nested_get, wait_for_condition

from .cm import ClouderaManagerDeployment

DEFAULT_NAMESPACE = 'cloudera'

CM_PORT = 7180
HUE_PORT = 8888

CM_AGENT_CONFIG_FILE_PATH = '/etc/cloudera-scm-agent/config.ini'
CM_PRINCIPAL_PASSWORD = 'cldadmin'
CM_PRINCIPAL_USER = 'cloudera-scm/admin'
CM_SERVER_ETC_DEFAULT = '/etc/default/cloudera-scm-server'
DEFAULT_CLUSTER_NAME = 'cluster'
SECONDARY_NODE_TEMPLATE_NAME = 'Secondary'

KERBEROS_CONFIG_CONTAINER_DIR = '/etc/clusterdock/kerberos'
KDC_HOSTNAME = 'kdc'
KDC_IMAGE = 'clusterdock/topology_nodebase_kerberos:centos6.6'
KDC_ACL_FILENAME = '/var/kerberos/krb5kdc/kadm5.acl'
KDC_CONF_FILENAME = '/var/kerberos/krb5kdc/kdc.conf'
KDC_KEYTAB_FILENAME = '{}/clusterdock.keytab'.format(KERBEROS_CONFIG_CONTAINER_DIR)
KDC_KRB5_CONF_FILENAME = '/etc/krb5.conf'
LINUX_USER_ID_START = 1000

KUDU_PARCEL_VERSION_REGEX = r'(.*)-.*\.cdh(.*)\.p'
KAFKA_PARCEL_REPO_URL = 'https://archive.cloudera.com/kafka/parcels'
KUDU_PARCEL_REPO_URL = 'https://archive.cloudera.com/kudu/parcels'
KUDU_PARCEL_VERSIONS = {'1.2.0': '5.10.0',
                        '1.3.0': '5.11.0',
                        '1.4.0': '5.12.0'}

logger = logging.getLogger('clusterdock.{}'.format(__name__))


def main(args):
    image_prefix = '{}/{}/clusterdock:cdh{}_cm{}'.format(args.registry,
                                                         args.namespace or DEFAULT_NAMESPACE,
                                                         args.cdh_version,
                                                         args.cm_version)
    primary_node_image = '{}_{}'.format(image_prefix, 'primary-node')
    secondary_node_image = '{}_{}'.format(image_prefix, 'secondary-node')

    # Docker's API for healthcheck uses units of nanoseconds. Define a constant
    # to make this more readable.
    SECONDS = 1000000000
    cm_server_healthcheck = {
        'test': 'curl --silent --output /dev/null 127.0.0.1:{}'.format(CM_PORT),
        'interval': 1 * SECONDS,
        'timeout': 1 * SECONDS,
        'retries': 1,
        'start_period': 30 * SECONDS
    }
    ports = [{CM_PORT: CM_PORT} if args.predictable else CM_PORT,
             {HUE_PORT: HUE_PORT} if args.predictable else HUE_PORT]

    primary_node = Node(hostname=args.primary_node[0], group='primary',
                        image=primary_node_image, ports=ports,
                        healthcheck=cm_server_healthcheck)
    secondary_nodes = [Node(hostname=hostname, group='secondary', image=secondary_node_image)
                       for hostname in args.secondary_nodes]
    nodes = [primary_node] + secondary_nodes

    if args.java:
        java_image = '{}/{}/clusterdock:cdh_{}'.format(args.registry,
                                                       args.namespace,
                                                       args.java)
        for node in nodes:
            node.volumes.append(java_image)

    if args.kerberos:
        kerberos_config_host_dir = os.path.expanduser(args.kerberos_config_directory)
        volumes = [{kerberos_config_host_dir: KERBEROS_CONFIG_CONTAINER_DIR}]
        for node in nodes:
            node.volumes.extend(volumes)

        kdc_node = Node(hostname=KDC_HOSTNAME, group='kdc', image=KDC_IMAGE,
                        volumes=volumes)

    cluster = Cluster(*nodes + ([kdc_node] if args.kerberos else []))
    cluster.primary_node = primary_node
    cluster.start(args.network)

    # Keep track of whether to suppress DEBUG-level output in commands.
    quiet = not args.verbose

    if args.kerberos:
        cluster.kdc_node = kdc_node
        _configure_kdc(cluster, args.kerberos_principals, quiet=quiet)
        _install_kerberos_clients(nodes, quiet=quiet)
        if args.kerberos_principals:
            _create_kerberos_cluster_users(nodes, args.kerberos_principals, quiet=quiet)

    if args.java:
        _set_cm_server_java_home(primary_node, '/usr/java/{}'.format(args.java))
        primary_node.execute('service cloudera-scm-server restart')

    filesystem_fix_commands = ['cp {0} {0}.1; umount {0}; mv -f {0}.1 {0}'.format(file_)
                               for file_ in ['/etc/hosts',
                                             '/etc/resolv.conf',
                                             '/etc/hostname',
                                             '/etc/localtime']]
    for node in nodes:
        node.execute('; '.join(filesystem_fix_commands), quiet=quiet)

    _configure_cm_agents(nodes)

    # The CDH topology uses two pre-built images ('primary' and 'secondary'). If a cluster
    # larger than 2 nodes is started, some modifications need to be done to the nodes to
    # prevent duplicate heartbeats and things like that.
    if len(secondary_nodes) > 1:
        _remove_files(nodes=secondary_nodes[1:],
                      files=['/var/lib/cloudera-scm-agent/uuid',
                             '/dfs*/dn/current/*'])

    logger.info('Restarting Cloudera Manager agents ...')
    _restart_cm_agents(nodes)

    logger.info('Waiting for Cloudera Manager server to come online ...')
    _wait_for_cm_server(primary_node)

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    hostname = 'localhost' if client.info().get('Name') == 'moby' else socket.gethostname()
    port = primary_node.host_ports.get(CM_PORT)
    server_url = 'http://{}:{}'.format(hostname, port)
    logger.info('Cloudera Manager server is now reachable at %s', server_url)

    # The work we need to do through CM itself begins here...
    deployment = ClouderaManagerDeployment(server_url)
    cm_cluster = deployment.cluster(DEFAULT_CLUSTER_NAME)
    cdh_parcel = next(parcel for parcel in cm_cluster.parcels
                      if parcel.product == 'CDH' and parcel.stage in ('ACTIVATING',
                                                                      'ACTIVATED'))

    # Add all CM hosts to the cluster (i.e. only new hosts that weren't part of the original
    # images).
    all_host_ids = {}
    for host in deployment.get_all_hosts():
        all_host_ids[host['hostId']] = host['hostname']
        for node in nodes:
            if node.fqdn == host['hostname']:
                node.host_id = host['hostId']
                break
        else:
            raise Exception('Could not find CM host with hostname {}.'.format(node.fqdn))
    cluster_host_ids = {host['hostId']
                        for host in deployment.get_cluster_hosts(cluster_name=DEFAULT_CLUSTER_NAME)}
    host_ids_to_add = set(all_host_ids.keys()) - cluster_host_ids
    if host_ids_to_add:
        logger.debug('Adding %s to cluster %s ...',
                     'host{} ({})'.format('s' if len(host_ids_to_add) > 1 else '',
                                          ', '.join(all_host_ids[host_id]
                                                    for host_id in host_ids_to_add)),
                     DEFAULT_CLUSTER_NAME)
        deployment.add_cluster_hosts(cluster_name=DEFAULT_CLUSTER_NAME, host_ids=host_ids_to_add)
        cdh_parcel.wait_for_stage('ACTIVATED', timeout=120, time_to_success=10)

        # Create and apply a host template to ensure that all the secondary nodes have the same
        # CM roles running on them.
        logger.info('Creating secondary node host template ...')
        _create_secondary_node_template(deployment=deployment,
                                        cluster_name=DEFAULT_CLUSTER_NAME,
                                        secondary_node=secondary_nodes[0])
        deployment.apply_host_template(cluster_name=DEFAULT_CLUSTER_NAME,
                                       host_template_name=SECONDARY_NODE_TEMPLATE_NAME,
                                       start_roles=False,
                                       host_ids=host_ids_to_add)
    else:
        cdh_parcel.wait_for_stage('ACTIVATED', timeout=120, time_to_success=10)

    if args.java:
        java_home = '/usr/java/{}'.format(args.java)
        logger.info('Updating JAVA_HOME config on all hosts to %s ...', java_home)
        deployment.update_all_hosts_config(configs={'java_home': java_home})

    logger.info('Updating CM server configurations ...')
    deployment.update_cm_config(configs={'manages_parcels': True})

    if args.include_services:
        if args.exclude_services:
            raise ValueError('Cannot pass both --include-services and --exclude-services.')
        service_types_to_leave = args.include_services.upper().split(',')
        for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
            if service['type'] not in service_types_to_leave:
                logger.info('Removing cluster service (name = %s) ...',
                            service['name'])
                deployment.api_client.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                                             service_name=service['name'])
    elif args.exclude_services:
        service_types_to_remove = args.exclude_services.upper().split(',')
        for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
            if service['type'] in service_types_to_remove:
                logger.info('Removing cluster service (name = %s) ...',
                            service['name'])
                deployment.api_client.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                                             service_name=service['name'])

    cluster_service_types = {service['type']
                             for service
                             in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME)}

    logger.info('Updating database configurations ...')
    _update_database_configs(deployment=deployment,
                             cluster_name=DEFAULT_CLUSTER_NAME,
                             primary_node=primary_node)

    if 'HIVE' in cluster_service_types and 'HDFS' in cluster_service_types:
        logger.info('Updating Hive Metastore Namenodes ...')
        _update_hive_metastore_namenodes(deployment=deployment,
                                         cluster_name=DEFAULT_CLUSTER_NAME)

    # Whether a user requests a service version or not, we always begin by removing it from the
    # cluster services list (if present) so that configurations can always be generated anew.
    for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
        if service['type'] == 'KUDU':
            logger.debug('Removing cluster service (name = %s) ...',
                         service['name'])
            deployment.api_client.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                                         service_name=service['name'])
            logger.debug('Clearing /data/kudu/master on primary node ...')
            cluster.primary_node.execute('rm -rf /data/kudu/master')
        elif service['type'] == 'KAFKA':
            logger.debug('Removing cluster service (name = %s) ...',
                         service['name'])
            deployment.api_client.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                                         service_name=service['name'])

    if args.kafka_version:
        logger.info('Configuring Kafka ...')
        _configure_kafka(deployment, cluster, kafka_version=args.kafka_version)

    if args.kudu_version:
        logger.info('Configuring Kudu ...')
        _configure_kudu(deployment, cluster, kudu_version=args.kudu_version)

    if args.sdc_version:
        logger.info('Configuring StreamSets Data Collector ...')
        _configure_sdc(deployment, cluster, sdc_version=args.sdc_version)

    if args.kerberos:
        logger.info('Configure Cloudera Manager for Kerberos ...')
        _configure_cm_for_kerberos(deployment, cluster)

    logger.info('Deploying client config ...')
    cm_cluster.deploy_client_config()

    if not args.dont_start_cluster:
        logger.info('Starting cluster services ...')
        cm_cluster.start()
        logger.info('Starting CM services ...')
        _start_cm_service(deployment=deployment)

        logger.info('Validating service health ...')
        _validate_service_health(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)


def _configure_kdc(cluster, kerberos_principals, quiet):
    kdc_node = cluster.kdc_node

    logger.info('Updating KDC configurations ...')
    realm = cluster.network.upper()

    logger.debug('Updating krb5.conf ...')
    krb5_conf = cluster.kdc_node.get_file(KDC_KRB5_CONF_FILENAME)
    kdc_node.put_file(KDC_KRB5_CONF_FILENAME,
                      re.sub(r'EXAMPLE.COM', realm,
                             re.sub(r'example.com', cluster.network,
                                    re.sub(r'kerberos.example.com',
                                           kdc_node.fqdn,
                                           krb5_conf))))

    logger.debug('Updating kdc.conf ...')
    kdc_conf = kdc_node.get_file(KDC_CONF_FILENAME)
    kdc_node.put_file(KDC_CONF_FILENAME,
                      re.sub(r'EXAMPLE.COM', realm,
                             re.sub(r'\[kdcdefaults\]',
                                    r'[kdcdefaults]\n max_renewablelife = 7d\n max_life = 1d',
                                    kdc_conf)))

    logger.debug('Updating kadm5.acl ...')
    kadm5_acl = kdc_node.get_file(KDC_ACL_FILENAME)
    kdc_node.put_file(KDC_ACL_FILENAME,
                      re.sub(r'EXAMPLE.COM', realm, kadm5_acl))

    logger.info('Starting KDC ...')

    kdc_commands = [
        'kdb5_util create -s -r {} -P kdcadmin'.format(realm),
        'kadmin.local -q "addprinc -pw {} admin/admin@{}"'.format('acladmin', realm),
        'kadmin.local -q "addprinc -pw {} {}@{}"'.format(CM_PRINCIPAL_PASSWORD,
                                                         CM_PRINCIPAL_USER,
                                                         realm)
    ]
    if kerberos_principals:
        principals = ['{}@{}'.format(primary, realm)
                      for primary in kerberos_principals.split(',')]
        kdc_commands.extend(['kadmin.local -q "addprinc -randkey {}"'.format(principal)
                             for principal in principals])
        kdc_commands.append('kadmin.local -q '
                            '"xst -norandkey -k {} {}"'.format(KDC_KEYTAB_FILENAME,
                                                               ' '.join(principals)))
    kdc_commands.extend(['krb5kdc',
                         'kadmind',
                         'authconfig --enablekrb5 --update',
                         'cp -f {} {}'.format(KDC_KRB5_CONF_FILENAME,
                                              KERBEROS_CONFIG_CONTAINER_DIR)])
    if kerberos_principals:
        kdc_commands.append('chmod 644 {}'.format(KDC_KEYTAB_FILENAME))

    kdc_node.execute('; '.join(kdc_commands),
                     quiet=quiet)

    logger.info('Validating health of Kerberos services ...')

    def condition(node, services, quiet):
        services_with_poor_health = [service
                                     for service in services
                                     if node.execute(command='service {} status'.format(service),
                                                     quiet=quiet).exit_code != 0]
        if services_with_poor_health:
            logger.debug('Services with poor health: %s',
                         ', '.join(services_with_poor_health))
        # Return True if the list of services with poor health is empty.
        return not bool(services_with_poor_health)
    wait_for_condition(condition=condition, condition_args=[kdc_node,
                                                            ['krb5kdc', 'kadmin'],
                                                            quiet])


def _install_kerberos_clients(nodes, quiet):
    logger.info('Installing Kerberos libraries on Cloudera Manager nodes ...')
    for node in nodes:
        if node.execute('yum list installed yum-plugin-ovl', quiet=quiet).exit_code != 0:
            logger.debug('Installing yum-plugin-ovl to workaround https://git.io/vFvPW ...')
            node.execute('yum -y -q install yum-plugin-ovl', quiet=quiet)

        command = ('yum -y -q install openldap-clients krb5-libs krb5-workstation'
                   if node.group == 'primary'
                   else 'yum -y -q install krb5-libs krb5-workstation')
        node.execute(command=command, quiet=quiet)


def _create_kerberos_cluster_users(nodes, kerberos_principals, quiet):
    commands = ['useradd -u {} -g hadoop {}'.format(uid, primary)
                for uid, primary in enumerate(kerberos_principals.split(','),
                                              start=LINUX_USER_ID_START)]
    for node in nodes:
        node.execute('; '.join(commands), quiet=quiet)


def _configure_cm_for_kerberos(deployment, cluster):
    realm = cluster.network.upper()
    kerberos_config = dict(SECURITY_REALM=realm,
                           KDC_HOST=KDC_HOSTNAME,
                           KRB_MANAGE_KRB5_CONF=True,
                           KRB_ENC_TYPES='aes256-cts-hmac-sha1-96')
    logger.debug('Updating CM server configurations ...')
    deployment.update_cm_config(kerberos_config)

    logger.debug('Importing Kerberos admin credentials ...')
    command_id = deployment.api_client.import_admin_credentials('{}@{}'.format(CM_PRINCIPAL_USER,
                                                                               realm),
                                                                CM_PRINCIPAL_PASSWORD)['id']

    def success(time):
        logger.debug('Imported admin credentials in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'to import admin credentials.'.format(timeout))
    wait_for_condition(condition=_command_condition,
                       condition_args=[deployment, command_id, 'Import admin credentials'],
                       success=success, failure=failure)

    logger.debug('Configuring cluster for Kerberos ...')
    command_id = deployment.api_client.configure_cluster_for_kerberos(DEFAULT_CLUSTER_NAME)['id']

    def success(time):
        logger.debug('Configured cluster for Kerberos in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'to configure cluster for Kerberos.'.format(timeout))
    wait_for_condition(condition=_command_condition,
                       condition_args=[deployment, command_id, 'Configure cluster for Kerberos'],
                       success=success, failure=failure)

    logger.debug('Deploying Kerberos client config ...')
    command_id = deployment.deploy_cluster_kerberos_client_config(DEFAULT_CLUSTER_NAME)['id']

    def success(time):
        logger.debug('Deployed Kerberos client config in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'to deploy Kerberos client config.'.format(timeout))
    wait_for_condition(condition=_command_condition,
                       condition_args=[deployment, command_id, 'Deploy Kerberos client config'],
                       timeout=180,
                       success=success, failure=failure)


def _command_condition(deployment, command_id, command_description):
    command_information = deployment.api_client.get_command_information(command_id)
    active = command_information.get('active')
    success = command_information.get('success')
    logger.debug('%s command: (active: %s, success: %s)', command_description, active, success)
    if not active and not success:
        raise Exception('Failed to import admin credentials')
    return not active and success


# TODO: Get rid of the excess amount of code duplication between the following
# function and _configure_kudu.
def _configure_kafka(deployment, cluster, kafka_version):
    for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels:
        if parcel.product == 'KAFKA' and parcel.stage in 'ACTIVATED':
            parcel_kafka_version = parcel.version.split('-')[0]
            if parcel_kafka_version == kafka_version:
                logger.info('Detected Kafka version matches specified version. Continuing ...')
            else:
                parcel.deactivate()

                for config in deployment.get_cm_config():
                    if config['name'] == 'REMOTE_PARCEL_REPO_URLS':
                        break
                else:
                    raise Exception('Failed to find remote parcel repo URLs configuration.')
                parcel_repo_urls = config['value']

                kafka_parcel_repo_url = '{}/{}'.format(KAFKA_PARCEL_REPO_URL, kafka_version)
                logger.debug('Adding Kafka parcel repo URL (%s) ...', kafka_parcel_repo_url)
                deployment.update_cm_config(
                    {'REMOTE_PARCEL_REPO_URLS': '{},{}'.format(parcel_repo_urls,
                                                               kafka_parcel_repo_url)}
                )

                logger.debug('Refreshing parcel repos ...')
                deployment.refresh_parcel_repos()
                kafka_parcel = next(parcel
                                    for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels
                                    if parcel.product == 'KAFKA'
                                    and parcel.version.split('-')[0] == kafka_version)
                kafka_parcel.download().distribute().activate()
            # Only one parcel can be activated at a time, so once one is found, our work is done.
            break

    logger.info('Adding Kafka service to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    broker_role = {'type': 'KAFKA_BROKER',
                   'hostRef': {'hostId': cluster.primary_node.host_id}}
    for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
        if service['type'] == 'ZOOKEEPER':
            break
    else:
        raise Exception('Could not find ZooKeeper service on cluster %s.', DEFAULT_CLUSTER_NAME)
    deployment.create_cluster_services(
        cluster_name=DEFAULT_CLUSTER_NAME,
        services=[{'name': 'kafka',
                   'type': 'KAFKA',
                   'displayName': 'Kafka',
                   'roles': [broker_role],
                   'config': {'items': [{'name': 'zookeeper_service',
                                         'value': service['name']}]}}]
    )

    for role_config_group in deployment.get_service_role_config_groups(DEFAULT_CLUSTER_NAME,
                                                                       'kafka'):
        if role_config_group['roleType'] == 'KAFKA_BROKER':
            logger.debug('Setting Kafka Broker max heap size to 1024 MB ...')
            configs = {'broker_max_heap_size': '1024'}
            deployment.update_service_role_config_group_config(DEFAULT_CLUSTER_NAME,
                                                               'kafka',
                                                               role_config_group['name'],
                                                               configs)
            data_directories = ' '.join(deployment.get_service_role_config_group_config(
                cluster_name=DEFAULT_CLUSTER_NAME,
                service_name='kafka',
                role_config_group_name=role_config_group['name'],
                view='full'
            )['log.dirs'].split(','))
            cluster.primary_node.execute('rm -rf {}'.format(data_directories))


def _configure_kudu(deployment, cluster, kudu_version):
    for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels:
        if parcel.product == 'KUDU' and parcel.stage in 'ACTIVATED':
            parcel_kudu_version = re.match(KUDU_PARCEL_VERSION_REGEX, parcel.version).group(1)
            if parcel_kudu_version == kudu_version:
                logger.info('Detected Kudu version matches specified version. Continuing ...')
            else:
                parcel.deactivate()

                for config in deployment.get_cm_config():
                    if config['name'] == 'REMOTE_PARCEL_REPO_URLS':
                        break
                else:
                    raise Exception('Failed to find remote parcel repo URLs configuration.')
                parcel_repo_urls = config['value']

                kudu_parcel_repo_url = '{}/{}'.format(KUDU_PARCEL_REPO_URL,
                                                      KUDU_PARCEL_VERSIONS[kudu_version])
                logger.debug('Adding Kudu parcel repo URL (%s) ...', kudu_parcel_repo_url)
                deployment.update_cm_config(
                    {'REMOTE_PARCEL_REPO_URLS': '{},{}'.format(parcel_repo_urls,
                                                               kudu_parcel_repo_url)}
                )

                logger.debug('Refreshing parcel repos ...')
                deployment.refresh_parcel_repos()
                kudu_parcel = next(parcel
                                   for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels
                                   if parcel.product == 'KUDU'
                                   and parcel.version.split('-')[0] == kudu_version)
                kudu_parcel.download().distribute().activate()
            # Only one parcel can be activated at a time, so once one is found, our work is done.
            break

    logger.info('Adding Kudu service to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    master_role = {'type': 'KUDU_MASTER',
                   'hostRef': {'hostId': cluster.primary_node.host_id},
                   'config': {'items': [{'name': 'fs_wal_dir', 'value': '/data/kudu/master'},
                                        {'name': 'fs_data_dirs', 'value': '/data/kudu/master'}]}}
    tserver_roles = [{'type': 'KUDU_TSERVER',
                      'config': {'items': [
                          {'name': 'fs_wal_dir', 'value': '/data/kudu/tserver'},
                          {'name': 'fs_data_dirs', 'value': '/data/kudu/tserver'}
                      ]},
                      'hostRef': node.host_id}
                     for node in cluster if node.group == 'secondary']
    service_config = {'items': [{'name': 'gflagfile_service_safety_valve',
                                 'value': '--use_hybrid_clock=false'}]}
    deployment.create_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME,
                                       services=[{'name': 'kudu',
                                                  'type': 'KUDU',
                                                  'displayName': 'Kudu',
                                                  'roles': [master_role] + tserver_roles,
                                                  'config': service_config}])

    for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
        if service['type'] == 'KUDU':
            break
    else:
        raise Exception('Could not find Kudu service.')

    for role_config_group in deployment.get_service_role_config_groups(DEFAULT_CLUSTER_NAME,
                                                                       service['name']):
        if role_config_group['roleType'] == 'KUDU_MASTER':
            configs = {'fs_wal_dir': '/data/kudu/master',
                       'fs_data_dirs': '/data/kudu/master'}
            deployment.update_service_role_config_group_config(DEFAULT_CLUSTER_NAME,
                                                               service['name'],
                                                               role_config_group['name'],
                                                               configs)
        elif role_config_group['roleType'] == 'KUDU_TSERVER':
            configs = {'fs_wal_dir': '/data/kudu/tserver',
                       'fs_data_dirs': '/data/kudu/tserver'}
            deployment.update_service_role_config_group_config(DEFAULT_CLUSTER_NAME,
                                                               service['name'],
                                                               role_config_group['name'],
                                                               configs)


def _set_cm_server_java_home(node, java_home):
    command = 'echo "export JAVA_HOME={}" >> {}'.format(java_home, CM_SERVER_ETC_DEFAULT)
    logger.info('Setting JAVA_HOME to %s in %s ...',
                java_home,
                CM_SERVER_ETC_DEFAULT)
    node.execute(command=command)


def _configure_cm_agents(nodes):
    cm_server_host = next(node.fqdn for node in nodes if node.group == 'primary')

    for node in nodes:
        logger.info('Changing CM agent configs on %s ...', node.fqdn)

        cm_agent_config = io.StringIO(node.get_file(CM_AGENT_CONFIG_FILE_PATH))
        config = ConfigObj(cm_agent_config, list_item_delimiter=',')

        logger.debug('Changing server_host to %s ...', cm_server_host)
        config['General']['server_host'] = cm_server_host

        # During container start, a race condition can occur where the hostname passed in
        # to Docker gets overriden by a start script in /etc/rc.sysinit. To avoid this,
        # we manually set the hostnames and IP addresses that CM agents use.
        logger.debug('Changing listening IP to %s ...', node.ip_address)
        config['General']['listening_ip'] = node.ip_address

        logger.debug('Changing listening hostname to %s ...', node.fqdn)
        config['General']['listening_hostname'] = node.fqdn

        logger.debug('Changing reported hostname to %s ...', node.fqdn)
        config['General']['reported_hostname'] = node.fqdn

        for filesystem in ['aufs', 'overlay']:
            if filesystem not in config['General']['local_filesystem_whitelist']:
                config['General']['local_filesystem_whitelist'].append(filesystem)

        # ConfigObj.write returns a list of strings.
        node.put_file(CM_AGENT_CONFIG_FILE_PATH, '\n'.join(config.write()))


def _remove_files(nodes, files):
    command = 'rm -rf {}'.format(' '.join(files))
    logger.info('Removing files (%s) from nodes (%s) ...',
                ', '.join(files),
                ', '.join(node.fqdn for node in nodes))
    for node in nodes:
        node.execute(command=command)


def _restart_cm_agents(nodes):
    # Supervisor issues were seen when restarting the SCM agent;
    # doing a clean_restart and disabling quiet mode for the execution
    # were empirically determined to be necessary.
    command = 'service cloudera-scm-agent clean_restart_confirmed'
    for node in nodes:
        node.execute(command=command, quiet=False)


def _wait_for_cm_server(primary_node):
    def condition(container):
        container.reload()
        health_status = nested_get(container.attrs, ['State', 'Health', 'Status'])
        logger.debug('Cloudera Manager health status evaluated to %s.', health_status)
        return health_status == 'healthy'

    def success(time):
        logger.debug('Cloudera Manager reached healthy state after %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for Cloudera Manager to start.'.format(timeout))
    wait_for_condition(condition=condition, condition_args=[primary_node.container],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _create_secondary_node_template(deployment, cluster_name, secondary_node):
    role_config_group_names = [
        nested_get(role, ['roleConfigGroupRef', 'roleConfigGroupName'])
        for role_ref in deployment.api_client.get_host(host_id=secondary_node.host_id)['roleRefs']
        for role in deployment.get_service_roles(cluster_name=cluster_name,
                                                 service_name=role_ref['serviceName'])
        if role['name'] == role_ref['roleName']
    ]
    deployment.create_host_template(host_template_name=SECONDARY_NODE_TEMPLATE_NAME,
                                    cluster_name=cluster_name,
                                    role_config_group_names=role_config_group_names)


def _update_database_configs(deployment, cluster_name, primary_node):
    for service in deployment.get_cluster_services(cluster_name=cluster_name):
        if service['type'] == 'HIVE':
            configs = {'hive_metastore_database_host': primary_node.fqdn}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)
        elif service['type'] == 'HUE':
            configs = {'database_host': primary_node.fqdn}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)
        elif service['type'] == 'OOZIE':
            configs = {'oozie_database_host': '{}:7432'.format(primary_node.fqdn)}
            service_name = service['name']
            args = [cluster_name, service_name]
            for role_config_group in deployment.get_service_role_config_groups(*args):
                if role_config_group['roleType'] == 'OOZIE_SERVER':
                    args = [cluster_name, service_name, role_config_group['name'], configs]
                    deployment.update_service_role_config_group_config(*args)
        elif service['type'] == 'SENTRY':
            configs = {'sentry_server_database_host': primary_node.fqdn}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)


def _update_hive_metastore_namenodes(deployment, cluster_name):
    for service in deployment.get_cluster_services(cluster_name=cluster_name):
        if service['type'] == 'HIVE':
            command_id = deployment.api_client.update_hive_metastore_namenodes(
                cluster_name, service['name']
            )['id']
            break

    def condition(deployment, command_id):
        command_information = deployment.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        logger.debug('Hive Metastore namenodes command: (active: %s, success: %s)', active, success)
        if not active and not success:
            raise Exception('Failed to update Hive Metastore Namenodes.')
        return not active and success

    def success(time):
        logger.debug('Updated Hive Metastore Namenodes in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for Hive Metastore Namenodes to update.'.format(timeout))
    wait_for_condition(condition=condition, condition_args=[deployment, command_id],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _start_cm_service(deployment):
    command_id = deployment.api_client.start_cm_service()['id']

    def condition(deployment, command_id):
        command_information = deployment.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        logger.debug('Start CM service command: (active: %s, success: %s)', active, success)
        if not active and not success:
            raise Exception('Failed to start CM service.')
        return not active and success

    def success(time):
        logger.debug('Started CM service in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for CM service to start.'.format(timeout))
    wait_for_condition(condition=condition, condition_args=[deployment, command_id],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _validate_service_health(deployment, cluster_name):
    def condition(deployment, cluster_name):
        services = (deployment.get_cluster_services(cluster_name=cluster_name)
                    + [deployment.api_client.get_cm_service()])
        if all(service.get('serviceState') == 'NA' or
               service.get('serviceState') == 'STARTED' and service.get('healthSummary') == 'GOOD'
               for service in services):
            return True
        else:
            logger.debug('Services with poor health: %s',
                         ', '.join(service['name']
                                   for service in services
                                   if (service.get('healthSummary') != 'GOOD'
                                       and service.get('serviceState') != 'NA')
                                   or service.get('serviceState') not in ('STARTED', 'NA')))

    def success(time):
        logger.debug('Validated service health in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'to validate service health.'.format(timeout))
    wait_for_condition(condition=condition, condition_args=[deployment, cluster_name],
                       time_between_checks=3, timeout=600, time_to_success=30,
                       success=success, failure=failure)
