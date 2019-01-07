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

from topology_cdh import cm, sdc

DEFAULT_NAMESPACE = 'streamsets'

CM_PORT = 7180
HUE_PORT = 8888

CM_AGENT_CONFIG_FILE_PATH = '/etc/cloudera-scm-agent/config.ini'
CM_PRINCIPAL_PASSWORD = 'cldadmin'
CM_PRINCIPAL_USER = 'cloudera-scm/admin'
CM_SERVER_ETC_DEFAULT = '/etc/default/cloudera-scm-server'
CSD_DIRECTORY = '/opt/cloudera/csd'
PARCEL_REPO_DIRECTORY = '/opt/cloudera/parcel-repo'
# In the following two locations, clusterdock volume mounts the csds and parcels for cloudera services.
CLUSTERDOCK_CSD_DIRECTORY = '/opt/clusterdock/csd'
CLUSTERDOCK_PARCEL_REPO_DIRECTORY = '/opt/clusterdock/parcel-repo'
DEFAULT_CLUSTER_NAME = 'cluster'
SECONDARY_NODE_TEMPLATE_NAME = 'Secondary'

KERBEROS_CONFIG_CONTAINER_DIR = '/etc/clusterdock/client/kerberos'
KDC_HOSTNAME = 'kdc'
KDC_IMAGE = 'clusterdock/topology_nodebase_kerberos:centos6.6'
KDC_ACL_FILENAME = '/var/kerberos/krb5kdc/kadm5.acl'
KDC_CONF_FILENAME = '/var/kerberos/krb5kdc/kdc.conf'
KDC_KEYTAB_FILENAME = '{}/clusterdock.keytab'.format(KERBEROS_CONFIG_CONTAINER_DIR)
KDC_KRB5_CONF_FILENAME = '/etc/krb5.conf'
LINUX_USER_ID_START = 1000

EARLIEST_CDH_VERSION_WITH_KAFKA = (6, 0, 0)
EARLIEST_CDH_VERSION_WITH_KUDU = (5, 13, 0)
EARLIEST_CDH_VERSION_WITH_LEGACY_STAGE_LIB = (5, 9, 0)
EARLIEST_CDH_VERSION_WITH_SPARK2 = (6, 0, 0)
EARLIEST_CDH_VERSION_WITH_CENTOS6_8 = (6, 1, 1)
# JAAS configuration for SDC to connect to Kerberized Kafka.
JAAS_CONFIG = """KafkaClient {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="{}/streamsets.keytab"
    principal="{}";
}};"""
JAAS_CONFIG_FILE_PATH = '/etc/clusterdock/client/kafka/kafka_client_jaas.conf'

KUDU_PARCEL_VERSION_REGEX = r'(.*)-.*\.cdh(.*)\.p'
KAFKA_PARCEL_REPO_URL = 'https://archive.cloudera.com/kafka/parcels'
KUDU_PARCEL_REPO_URL = 'https://archive.cloudera.com/kudu/parcels'
KUDU_PARCEL_VERSIONS = {'1.2.0': '5.10.0',
                        '1.3.0': '5.11.0',
                        '1.4.0': '5.12.0'}
SERVICE_PARCEL_REPO_URLS = {'KAFKA': KAFKA_PARCEL_REPO_URL,
                            'KUDU': KUDU_PARCEL_REPO_URL}

# Files placed in this directory on primary_node are available
# in clusterdock_config_directory after cluster is started.
# Also, this gets volume mounted to all secondary nodes and hence available there too.
CLUSTERDOCK_CLIENT_CONTAINER_DIR = '/etc/clusterdock/client'

SSL_SERVER_CONTAINER_DIR = '/etc/clusterdock/server/ssl'
SSL_CLIENT_CONTAINER_DIR = '/etc/clusterdock/client/ssl'
SSL_SERVER_KEYSTORE = 'server.keystore.jks'
SSL_SERVER_TRUSTSTORE = 'server.truststore.jks'
SSL_CLIENT_KEYSTORE = 'client.keystore.jks'
SSL_CLIENT_TRUSTSTORE = 'client.truststore.jks'
SSL_SERVER_PASSWORD = 'serverpass'
SSL_CLIENT_PASSWORD = 'clientpass'

# Navigator related constants.
DB_MGMT_PROPERTIES_FILENAME = '/etc/cloudera-scm-server/db.mgmt.properties'
# CDH versions < 5.14.0 need to have Reports Manager role created when license is applied
# which is required for Navigator.
EARLIEST_CDH_VERSION_WITH_NO_REPORTS_MANAGER_NEEDED = (5, 14, 0)
NAVIGATOR_POSTGRESQL_PORT = 7432
NAVIGATOR_PORT = 7187

logger = logging.getLogger('clusterdock.{}'.format(__name__))


def main(args):

    if args.include_services and args.exclude_services:
        raise ValueError('Cannot pass both --include-services and --exclude-services.')

    image_prefix = '{}/{}/clusterdock:cdh{}_cm{}'.format(args.registry,
                                                         args.namespace or DEFAULT_NAMESPACE,
                                                         args.cdh_version,
                                                         args.cm_version)
    primary_node_image = '{}_{}'.format(image_prefix, 'primary-node')
    secondary_node_image = '{}_{}'.format(image_prefix, 'secondary-node')
    single_node_image = '{}_{}'.format(image_prefix, 'single-node')

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

    clusterdock_config_host_dir = os.path.realpath(os.path.expanduser(args.clusterdock_config_directory))
    volumes = [{clusterdock_config_host_dir: CLUSTERDOCK_CLIENT_CONTAINER_DIR}]
    if args.sdc_version:
        ports.append({sdc.SDC_PORT: sdc.SDC_PORT} if args.predictable else sdc.SDC_PORT)
    primary_node = Node(hostname=args.primary_node[0], group='primary',
                        image=primary_node_image if not args.single_node else single_node_image,
                        ports=ports,
                        healthcheck=cm_server_healthcheck,
                        volumes=volumes)
    secondary_nodes = [Node(hostname=hostname, group='secondary', image=secondary_node_image, volumes=volumes)
                       for hostname in args.secondary_nodes]
    nodes = [primary_node] + secondary_nodes if not args.single_node else [primary_node]

    if args.java:
        java_image = '{}/{}/clusterdock:cdh_{}'.format(args.registry,
                                                       args.namespace or DEFAULT_NAMESPACE,
                                                       args.java)
        for node in nodes:
            node.volumes.append(java_image)

    cdh_version_tuple = tuple(int(i) if i.isdigit() else i for i in args.cdh_version.split('.'))

    # Gather services to add depending on values of --include-services and --exclude-services.
    # These services accept version as arguments e.g. kafka_version, kudu_version.
    services_to_add = set()
    if args.spark2_version and cdh_version_tuple < EARLIEST_CDH_VERSION_WITH_SPARK2:
        if _is_service_to_add('SPARK2_ON_YARN', args.include_services, args.exclude_services):
            services_to_add.add('SPARK2_ON_YARN')
            image_name = '{}/{}/clusterdock:topology_cdh-spark-{}'
            spark2_parcel_image = image_name.format(args.registry,
                                                    args.namespace or DEFAULT_NAMESPACE,
                                                    args.spark2_version)
            logger.debug('Adding Spark2 parcel image %s to CM nodes ...', spark2_parcel_image)
            for node in nodes:
                node.volumes.append(spark2_parcel_image)

    if args.sdc_version:
        logger.info('args.sdc_version = %s', args.sdc_version)
        data_collector = sdc.StreamsetsDataCollector(args.sdc_version,
                                                     args.namespace or DEFAULT_NAMESPACE,
                                                     args.registry)
        sdc_parcel_image = data_collector.image_name
        logger.debug('Adding SDC parcel image %s to CM nodes ...', sdc_parcel_image)
        for node in nodes:
            node.volumes.append(sdc_parcel_image)

        _load_necessary_stage_lib_volumes_for_sdc(args.additional_stage_libs_version, args.cdh_version,
                                                  args.cm_version, data_collector, args.dataprotector,
                                                  args.kafka_version, args.kudu_version, args.navigator, primary_node)

        # Volume mount SDC resources.
        # e.g. If /home/ubuntu/protobuf is passed, then it gets mounted to SDC_RESOURCES_DIRECTORY/protobuf on SDC node
        if args.sdc_resources_directory:
            sdc_resources_directory_path = os.path.realpath(os.path.expanduser(args.sdc_resources_directory))
            sdc_resources_basename = os.path.basename(sdc_resources_directory_path)
            sdc_resources_mount_point = '{}/{}'.format(sdc.RESOURCES_DIRECTORY, sdc_resources_basename)
            logger.debug('Volume mounting resources from %s to %s ...',
                         sdc_resources_directory_path, sdc_resources_mount_point)
            primary_node.volumes.append({sdc_resources_directory_path: sdc_resources_mount_point})

    if args.kerberos:
        dir = '{}/kerberos'.format(args.clusterdock_config_directory)
        kerberos_config_host_dir = os.path.realpath(os.path.expanduser(dir))
        volumes = [{kerberos_config_host_dir: KERBEROS_CONFIG_CONTAINER_DIR}]
        for node in nodes:
            node.volumes.extend(volumes)

        kdc_node = Node(hostname=KDC_HOSTNAME, group='kdc', image=KDC_IMAGE,
                        volumes=volumes)

    cluster = Cluster(*nodes + ([kdc_node] if args.kerberos else []))
    cluster.primary_node = primary_node
    cluster.start(args.network, pull_images=args.always_pull)

    # Keep track of whether to suppress DEBUG-level output in commands.
    quiet = not args.verbose

    if args.spark2_version and 'SPARK2_ON_YARN' in services_to_add:
        # Install is needed only when Spark2 is not integrated.
        logger.info('Installing Spark2 from local repo ...')
        _install_service_from_local_repo(cluster, product='SPARK2')

    if args.sdc_version:
        _install_service_from_local_repo(cluster, product='STREAMSETS_DATACOLLECTOR')

    if args.kerberos:
        cluster.kdc_node = kdc_node
        _configure_kdc(cluster, args.kerberos_principals, args.kerberos_ticket_lifetime, quiet=quiet)
        _install_kerberos_clients(nodes, quiet=quiet)
        if args.kerberos_principals:
            _create_kerberos_cluster_users(nodes, args.kerberos_principals, quiet=quiet)

    if args.java:
        _set_cm_server_java_home(primary_node, '/usr/java/{}'.format(args.java))

    if args.java or (args.spark2_version and 'SPARK2_ON_YARN' in services_to_add):
        # In case change was made to Java version or parcel/s were installed from local repo,
        # then cloudera-scm-server needs to be restarted to take changes in effect.

        # Avoid CM database issues by waiting for CM to not be dead before restarting it.
        def cm_server_not_dead(primary_node):
            cm_server_status = primary_node.execute('service cloudera-scm-server status', quiet=True)
            logger.debug('CM server status command has output (%s) with exit code %s.',
                         cm_server_status.output,
                         cm_server_status.exit_code)
            return cm_server_status.exit_code != 1
        try:
            wait_for_condition(cm_server_not_dead,
                               [primary_node],
                               failure=lambda timeout: None)
        except TimeoutError:
            logger.warning('Timed out while waiting for the CM server to not be dead. '
                           'Will try to continue ...')

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
    _restart_cm_agents(nodes, cdh_version_tuple)

    logger.info('Waiting for Cloudera Manager server to come online ...')
    _wait_for_cm_server(primary_node)

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    if any(docker_for_mac_name in client.info().get('Name', '') for docker_for_mac_name in ['moby', 'linuxkit']):
        hostname = 'localhost'
    else:
        hostname = socket.gethostbyname(socket.gethostname())
    port = primary_node.host_ports.get(CM_PORT)
    server_url = 'http://{}:{}'.format(hostname, port)
    logger.info('Cloudera Manager server is now reachable at %s', server_url)

    # The work we need to do through CM itself begins here...
    deployment = cm.ClouderaManagerDeployment(server_url)
    cm_cluster = deployment.cluster(DEFAULT_CLUSTER_NAME)
    # For CDH 6, it takes a bit of time for CDH parcel to reach ACTIVATED stage.
    # Hence wait for that stage to reach, otherwise the next statement to find cdh_parcel in that stage fails.
    cm_cluster.wait_for_parcel_stage(product='CDH', version=args.cdh_version, stage='ACTIVATED')
    cdh_parcel = next(parcel for parcel in cm_cluster.parcels
                      if parcel.product == 'CDH' and parcel.stage == 'ACTIVATED')

    # Wait on all slave nodes to get back to CM
    wait_for_condition(lambda: len(deployment.get_all_hosts()) == len(nodes))

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
        cdh_parcel.wait_for_stage('ACTIVATED', timeout=600, time_to_success=10)

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
        cdh_parcel.wait_for_stage('ACTIVATED', timeout=600, time_to_success=10)

    if args.java:
        java_home = '/usr/java/{}'.format(args.java)
        logger.info('Updating JAVA_HOME config on all hosts to %s ...', java_home)
        deployment.update_all_hosts_config(configs={'java_home': java_home})

    logger.info('Updating CM server configurations ...')
    deployment.update_cm_config(configs={'manages_parcels': True})

    # Services that are not added by default by CDH core parcel are added. e.g. KAFKA, KUDU, SPARK2_ON_YARN.
    # Validate passed service versions and add them to set of services_to_add.
    # SPARK2_ON_YARN is already validated by now.
    services_to_add.update(_validate_services_to_add(cdh_version_tuple, cm_cluster, args.exclude_services,
                                                     args.include_services, args.kafka_version, args.kudu_version))
    if args.sdc_version:
        # The parcel is already present. Hence just distribute and activate it after refresing parcel repos.
        product = 'STREAMSETS_DATACOLLECTOR'
        deployment.refresh_parcel_repos()
        # Remove -RC, -SNAPSHOT etc. from version.
        version = args.sdc_version.rsplit('-')[0]
        cm_cluster.wait_for_parcel_stage(product=product, version=version, stage='DOWNLOADED')
        sdc_parcel = cm_cluster.parcel(product=product, version=version, stage='DOWNLOADED')
        sdc_parcel.distribute(timeout=900).activate(timeout=600)

    if args.include_services:
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

    if args.navigator:
        logger.info('Configuring Navigator ...')
        _configure_navigator(deployment, cluster, cdh_version_tuple)

    if 'SPARK2_ON_YARN' in services_to_add:
        logger.info('Configuring Spark2 ...')
        _configure_spark2(deployment, cluster, secondary_nodes[0] if not args.single_node else primary_node)
        if args.kerberos:
            # Stopping of services after first_run, is needed for kerberos to work correctly.
            logger.info('Stopping cluster services after adding Spark2 as some were started during first_run ...')
            cm_cluster.stop()

    if 'KAFKA' in services_to_add:
        if args.ssl:
            _setup_ssl(cluster, ['KAFKA'])
        logger.info('Configuring Kafka ...')
        _configure_kafka(deployment, cluster, args.kafka_version, cdh_version_tuple, args.ssl)

    if 'KUDU' in services_to_add:
        logger.info('Configuring Kudu ...')
        _configure_kudu(deployment, cluster, args.kudu_version, args.cdh_version, args.single_node)

    if args.sdc_version:
        logger.info('Configuring StreamSets Data Collector ...')
        _configure_sdc(args, cdh_version_tuple, cluster, data_collector, deployment)
        logger.info('Adding sdc user to YARN user whitelist ..')
        _configure_yarn(deployment, cluster, cluster_name=DEFAULT_CLUSTER_NAME)

    if args.kerberos:
        logger.info('Configure Cloudera Manager for Kerberos ...')
        _configure_cm_for_kerberos(deployment, cluster, args.kerberos_ticket_lifetime)

    _configure_for_streamsets_before_start(deployment, cluster, cluster_name=DEFAULT_CLUSTER_NAME,
                                           sdc_resources_directory=args.sdc_resources_directory)
    logger.info('Deploying client config ...')
    cm_cluster.deploy_client_config()

    # This is needed due to the fact some service might have run first_run_service command while configuring.
    # first_run_service command in turn starts the service along with its dependent services.
    # In case of dont_start_cluster, the cluster services need to be stopped.
    # Otherwise, some of these started services end up with stale configurations at this point
    # - which asks for their stop and later run start.
    logger.info('Stopping cluster services ...')
    cm_cluster.stop()
    if not args.dont_start_cluster:
        # Navigator requires restart of some of the services after CM service is started. Hence start CM service first.
        logger.info('Starting CM services ...')
        _start_cm_service(deployment=deployment)
        logger.info('Starting cluster services ...')
        cm_cluster.start()
        # If Navigator is enabled, then restart of SDC is required to publish lineage events properly.
        if args.sdc_version and args.navigator:
            logger.info('Restarting SDC service ...')
            deployment.restart_service(DEFAULT_CLUSTER_NAME, 'streamsets')

        logger.info('Validating service health ...')
        _validate_service_health(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)

        logger.info('Configuring after start of cluster ...')
        _configure_after_start(deployment=deployment,
                               cluster_name=DEFAULT_CLUSTER_NAME,
                               cluster=cluster, quiet=not args.verbose,
                               kerberos_principals=args.kerberos_principals)

        _configure_for_streamsets_after_start(deployment=deployment,
                                              cluster_name=DEFAULT_CLUSTER_NAME,
                                              cluster=cluster,
                                              quiet=not args.verbose,
                                              kerberos_enabled=args.kerberos,
                                              kerberos_principals=args.kerberos_principals,
                                              navigator_enabled=args.navigator)


def _load_necessary_stage_lib_volumes_for_sdc(additional_stage_libs_version, cdh_version, cm_version,
                                              data_collector, dataprotector, kafka_version, kudu_version,
                                              navigator, primary_node):
    stage_lib_images = data_collector.get_legacy_stage_lib_images(cdh_version, kafka_version, kudu_version)
    if dataprotector:
        stage_lib_images.append(data_collector.get_dataprotector_stage_lib(additional_stage_libs_version))
    if navigator:
        stage_lib_images.append(data_collector.get_navigator_stage_lib(cm_version, additional_stage_libs_version))
    if stage_lib_images != []:
        for stage_lib_image in stage_lib_images:
            try:
                logger.debug('Adding stage lib image %s to primary node ... ', stage_lib_image)
                primary_node.volumes.append(stage_lib_image)
            except:
                raise Exception('No stage library image found for {} ...'.format(stage_lib_image))


def _is_service_to_add(service_type, include_services, exclude_services):
    if include_services:
        return service_type in include_services.upper().split(',')
    elif exclude_services:
        return service_type not in exclude_services.upper().split(',')
    else:
        return True


def _configure_kdc(cluster, kerberos_principals, kerberos_ticket_lifetime, quiet):
    kdc_node = cluster.kdc_node

    logger.info('Updating KDC configurations ...')
    realm = cluster.network.upper()

    logger.debug('Updating krb5.conf ...')
    krb5_conf = cluster.kdc_node.get_file(KDC_KRB5_CONF_FILENAME)
    # Here '\g<1>' represents group matched in regex which is the original default value of ticket_lifetime.
    ticket_lifetime_replacement = kerberos_ticket_lifetime if kerberos_ticket_lifetime else '\g<1>'
    krb5_conf_contents = re.sub(r'EXAMPLE.COM', realm,
                                re.sub(r'example.com', cluster.network,
                                       re.sub(r'ticket_lifetime = ((.)*)',
                                              r'ticket_lifetime = {}'.format(ticket_lifetime_replacement),
                                              re.sub(r'kerberos.example.com',
                                                     kdc_node.fqdn,
                                                     krb5_conf))))
    kdc_node.put_file(KDC_KRB5_CONF_FILENAME, krb5_conf_contents)

    logger.debug('Updating kdc.conf ...')
    kdc_conf = kdc_node.get_file(KDC_CONF_FILENAME)
    max_time_replacement = kerberos_ticket_lifetime if kerberos_ticket_lifetime else '1d'
    kdc_node.put_file(KDC_CONF_FILENAME,
                      re.sub(r'EXAMPLE.COM', realm,
                             re.sub(r'\[kdcdefaults\]',
                                    r'[kdcdefaults]\n max_renewablelife = 7d\n max_life = {}'.format(max_time_replacement),
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
        if kerberos_ticket_lifetime:
            kdc_commands.extend([('kadmin.local -q "addprinc -maxlife {}sec '
                                  '-maxrenewlife 5day -randkey {}"'.format(kerberos_ticket_lifetime, principal))
                                 for principal in principals])
        else:
            kdc_commands.extend(['kadmin.local -q "addprinc -randkey {}"'.format(principal)
                                 for principal in principals])
        kdc_commands.append('kadmin.local -q '
                            '"xst -norandkey -k {} {}"'.format(KDC_KEYTAB_FILENAME,
                                                               ' '.join(principals)))
    kdc_commands.extend(['service krb5kdc start',
                         'service kadmin start',
                         'authconfig --enablekrb5 --update',
                         'cp -f {} {}'.format(KDC_KRB5_CONF_FILENAME,
                                              KERBEROS_CONFIG_CONTAINER_DIR)])
    if kerberos_principals:
        kdc_commands.append('chmod 644 {}'.format(KDC_KEYTAB_FILENAME))

    kdc_node.execute(' && '.join(kdc_commands),
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


def _configure_cm_for_kerberos(deployment, cluster, kerberos_ticket_lifetime):
    realm = cluster.network.upper()
    kerberos_config = dict(SECURITY_REALM=realm,
                           KDC_HOST=cluster.kdc_node.fqdn,
                           KRB_MANAGE_KRB5_CONF=True,
                           KRB_ENC_TYPES='aes256-cts-hmac-sha1-96')
    # Suppress CM's warnings for built-in parameter validation for Kerberos ticket lifetime and renewable lifetime.
    # This will allow ticket lifetime and renewable lifetime to be less than an hour.
    if kerberos_ticket_lifetime:
        kerberos_config.update(dict(scm_config_suppression_krb_renew_lifetime=True,
                                    scm_config_suppression_krb_ticket_lifetime=True,
                                    krb_ticket_lifetime=kerberos_ticket_lifetime))
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

    if kerberos_ticket_lifetime:
        cm_kerberos_principals = deployment.get_cm_kerberos_principals()
        logger.debug('cm_kerberos_principals = %s', cm_kerberos_principals)
        _apply_kerberos_ticket_expiration_for_cm_principals(cluster, cm_kerberos_principals, kerberos_ticket_lifetime)

    for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
        if service['type'] == 'HUE':
            _apply_kerberos_fix_for_hue(cluster)
            break


def _apply_kerberos_fix_for_hue(cluster):
    """ Fix for Hue service as explained at:
    http://www.cloudera.com/documentation/manager/5-1-x/Configuring-Hadoop-Security-with-Cloudera-Manager/cm5chs_enable_hue_sec_s10.html
    """
    logger.info('Applying Kerberos fix for hue...')
    kdc_node = cluster.kdc_node
    primary_node = cluster.primary_node

    realm = cluster.network.upper()
    kdc_hue_commands = [
        'kadmin.local -q "modprinc -maxrenewlife 90day krbtgt/{realm}"'.format(realm=realm),
        'kadmin.local -q "modprinc -maxrenewlife 90day +allow_renewable hue/{hue_node_name}@{realm}"'.format(
            realm=realm,
            hue_node_name=primary_node.fqdn),
        'service krb5kdc restart',
        'service kadmin restart'
    ]
    kdc_node.execute('&& '.join(kdc_hue_commands))


def _apply_kerberos_ticket_expiration_for_cm_principals(cluster, cm_kerberos_principals, kerberos_ticket_lifetime):
    """Apply Kerberos ticket expiration for Kerberos principals needed by the services being managed by
    Cloudera Manager.
    """
    logger.info('Applying kerberos ticket expiration...')

    change_ticket_expiration_cmd = 'kadmin.local -q "modprinc -maxrenewlife 5day -maxlife {maxlife}sec {principal}"'
    commands = [
        change_ticket_expiration_cmd.format(maxlife=kerberos_ticket_lifetime, principal=principal)
        for principal in cm_kerberos_principals
    ]
    commands.extend(['service krb5kdc restart', 'service kadmin restart'])
    cluster.kdc_node.execute('&& '.join(commands))


def _command_condition(deployment, command_id, command_description):
    command_information = deployment.api_client.get_command_information(command_id)
    active = command_information.get('active')
    success = command_information.get('success')
    logger.debug('%s command: (active: %s, success: %s)', command_description, active, success)
    if not active and not success:
        raise Exception('Failed to import admin credentials')
    return not active and success


def _deactivate_service_parcel(deployment, cluster, product, version):
    for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels:
        if parcel.product == product and parcel.stage in 'ACTIVATED':
            parcel_version = parcel.version.split('-')[0]
            if parcel_version == version:
                logger.info('Detected parcel version matches specified version. Continuing ...')
            else:
                parcel.deactivate()
            # Only one parcel can be activated at a time, so once one is found, our work is done.
            break


def _install_parcel_from_remote_repo(deployment, product, version, parcel_repo_version=None):
    for config in deployment.get_cm_config():
        if config['name'] == 'REMOTE_PARCEL_REPO_URLS':
            break
    else:
        raise Exception('Failed to find remote parcel repo URLs configuration.')
    parcel_repo_urls = config['value']

    service_parcel_repo_url = '{}/{}'.format(SERVICE_PARCEL_REPO_URLS[product],
                                             parcel_repo_version if parcel_repo_version is not None else version)
    logger.debug('Adding parcel repo URL (%s) ...', service_parcel_repo_url)
    deployment.update_cm_config(
        {'REMOTE_PARCEL_REPO_URLS': '{},{}'.format(parcel_repo_urls,
                                                   service_parcel_repo_url)}
    )

    logger.debug('Refreshing parcel repos ...')
    deployment.refresh_parcel_repos()
    deployment.cluster(DEFAULT_CLUSTER_NAME).wait_for_parcel_stage(product=product, version=version)
    service_parcel = deployment.cluster(DEFAULT_CLUSTER_NAME).parcel(product=product, version=version)
    service_parcel.download().distribute().activate()


def _configure_kafka(deployment, cluster, kafka_version, cdh_version_tuple, ssl):
    if cdh_version_tuple < EARLIEST_CDH_VERSION_WITH_KAFKA:
        _deactivate_service_parcel(deployment, cluster, 'KAFKA', kafka_version)
        for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels:
            if parcel.product == 'KAFKA' and parcel.stage in 'ACTIVATED':
                break
        else:
            _install_parcel_from_remote_repo(deployment, 'KAFKA', kafka_version)

    logger.info('Adding Kafka service to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    broker_role = {'type': 'KAFKA_BROKER',
                   'hostRef': {'hostId': cluster.primary_node.host_id}}
    for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
        if service['type'] == 'ZOOKEEPER':
            break
    else:
        raise Exception('Could not find ZooKeeper service on cluster %s.', DEFAULT_CLUSTER_NAME)

    # Replication factor needs to be set to 1 for CDH Kafka 3.0.0 onwards
    # when Default Number of partitions = 1 (which is default).
    # Also changing replication factor to 1 works with CDH Kafka < 3.0.0.
    deployment.create_cluster_services(
        cluster_name=DEFAULT_CLUSTER_NAME,
        services=[{'name': 'kafka',
                   'type': 'KAFKA',
                   'displayName': 'Kafka',
                   'roles': [broker_role],
                   'config': {'items': [{'name': 'zookeeper_service', 'value': service['name']},
                                        {'name': 'offsets.topic.replication.factor', 'value': 1}]}}]
    )

    for role_config_group in deployment.get_service_role_config_groups(DEFAULT_CLUSTER_NAME,
                                                                       'kafka'):
        if role_config_group['roleType'] == 'KAFKA_BROKER':
            logger.debug('Setting Kafka Broker max heap size to 1024 MB ...')
            configs = {'broker_max_heap_size': '1024'}
            if ssl:
                configs.update({'ssl_enabled': 'true',
                                'ssl_server_keystore_location': '{}/{}.{}'.format(SSL_SERVER_CONTAINER_DIR,
                                                                                  'kafka',
                                                                                  SSL_SERVER_KEYSTORE),
                                'ssl_server_keystore_password': SSL_SERVER_PASSWORD,
                                'ssl_server_keystore_keypassword': SSL_SERVER_PASSWORD,
                                'ssl_client_truststore_location': '{}/{}.{}'.format(SSL_SERVER_CONTAINER_DIR,
                                                                                    'kafka',
                                                                                    SSL_SERVER_TRUSTSTORE),
                                'ssl_client_truststore_password': SSL_SERVER_PASSWORD})
            if ssl == 'authentication':
                configs.update({'ssl.client.auth': 'required'})

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


def _install_service_from_local_repo(cluster, product):
    # We install service using local repo /opt/cloudera/parcel-repo.
    # Set file and folder permissions correctly.
    commands = ['chown -R cloudera-scm:cloudera-scm {} {}'.format(CLUSTERDOCK_CSD_DIRECTORY,
                                                                  CLUSTERDOCK_PARCEL_REPO_DIRECTORY),
                'chmod 644 {}/{}/*.jar'.format(CLUSTERDOCK_CSD_DIRECTORY, product),
                'ln -s {}/{}/*.jar {}'.format(CLUSTERDOCK_CSD_DIRECTORY, product, CSD_DIRECTORY),
                'chmod 644 {}/*.jar'.format(CSD_DIRECTORY),
                'chown cloudera-scm:cloudera-scm {}/*.jar'.format(CSD_DIRECTORY),
                'ln -s {}/{}/* {}/'.format(CLUSTERDOCK_PARCEL_REPO_DIRECTORY, product, PARCEL_REPO_DIRECTORY)]
    cluster.primary_node.execute(' && '.join(commands))


def _configure_spark2(deployment, cluster, history_server_node):
    # The parcel is already present. Hence just distribute and activate it after refresing parcel repos.
    deployment.refresh_parcel_repos()
    deployment.cluster(DEFAULT_CLUSTER_NAME).wait_for_parcel_stage(product='SPARK2', stage='DOWNLOADED')
    parcel = deployment.cluster(DEFAULT_CLUSTER_NAME).parcel(product='SPARK2', stage='DOWNLOADED')
    parcel.distribute().activate()

    logger.info('Adding Spark2 service to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    service_name = 'spark2_on_yarn'
    history_server_role = {'type': 'SPARK2_YARN_HISTORY_SERVER',
                           'hostRef': {'hostId': history_server_node.host_id}}
    gateway_roles = [{'type': 'GATEWAY', 'hostRef': node.host_id}
                     for node in cluster if node.group in ['primary', 'secondary']]
    service_config = {'items': [{'name': 'hive_service', 'value': 'hive', 'sensitive': False},
                                {'name': 'yarn_service', 'value': 'yarn', 'sensitive': False}]}
    deployment.create_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME,
                                       services=[{'name': service_name,
                                                  'type': 'SPARK2_ON_YARN',
                                                  'displayName': 'Spark2',
                                                  'roles': [history_server_role] + gateway_roles,
                                                  'config': service_config}])
    # first_run_service is needed as it creates HDFS dir. /user/spark/spark2ApplicationHistory that is needed by
    # Spark2 History Server. Without that, starting of Spark2 service fails.
    deployment.first_run_service(DEFAULT_CLUSTER_NAME, service_name, 300)


def _configure_navigator(deployment, cluster, cdh_version_tuple):
    logger.info('Begin trial license for cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    deployment.begin_trial()

    primary_node = cluster.primary_node
    navigator_roles = []
    # Fetch the embedded database management properties.
    db_mgmt_prop_data = primary_node.get_file(DB_MGMT_PROPERTIES_FILENAME)
    if cdh_version_tuple < EARLIEST_CDH_VERSION_WITH_NO_REPORTS_MANAGER_NEEDED:
        # These CDH versions need to have Reports Manager role created when license is applied.
        logger.info('Updating Reports Manager base role configs ...')
        configs = {'headlamp_database_name': 'rman',
                   'headlamp_database_user': 'rman'}
        deployment.update_cm_service_role_config_group_config('mgmt-REPORTSMANAGER-BASE', configs)

        logger.info('Configuring Reports Manager role ...')
        reports_manager_dbpassword = re.search('com.cloudera.cmf.REPORTSMANAGER.db.password=(.*)', db_mgmt_prop_data)
        reports_manager_role = {'type': 'REPORTSMANAGER',
                                'config': {'items': [
                                    {'name': 'headlamp_database_host',
                                     'value': '{}:{}'.format(primary_node.fqdn, NAVIGATOR_POSTGRESQL_PORT)},
                                    {'name': 'headlamp_database_password',
                                     'value': reports_manager_dbpassword.group(1)},
                                    {'name': 'headlamp_database_type',
                                     'value': 'postgresql'}
                                ]},
                                'hostRef': {'hostId': cluster.primary_node.host_id}}
        navigator_roles.append(reports_manager_role)

    logger.info('Configuring Navigator Metaserver role ...')
    navigator_metaserver_dbpassword = re.search('cloudera.cmf.NAVIGATORMETASERVER.db.password=(.*)', db_mgmt_prop_data)
    navigator_metadata_server_role = {'type': 'NAVIGATORMETASERVER',
                                      'config': {'items': [
                                          {'name': 'nav_metaserver_database_host',
                                           'value': '{}:{}'.format(primary_node.fqdn, NAVIGATOR_POSTGRESQL_PORT)},
                                          {'name': 'nav_metaserver_database_password',
                                           'value': navigator_metaserver_dbpassword.group(1)},
                                          {'name': 'nav_metaserver_database_type',
                                           'value': 'postgresql'}
                                      ]},
                                      'hostRef': {'hostId': cluster.primary_node.host_id}}
    navigator_roles.append(navigator_metadata_server_role)

    logger.info('Configuring Audit Server role ...')
    navigator_dbpassword = re.search('com.cloudera.cmf.NAVIGATOR.db.password=(.*)', db_mgmt_prop_data)
    navigator_audit_server_role = {'type': 'NAVIGATOR',
                                   'config': {'items': [
                                       {'name': 'navigator_database_host',
                                        'value': '{}:{}'.format(primary_node.fqdn, NAVIGATOR_POSTGRESQL_PORT)},
                                       {'name': 'navigator_database_password',
                                        'value': navigator_dbpassword.group(1)},
                                       {'name': 'navigator_database_type',
                                        'value': 'postgresql'}
                                   ]},
                                   'hostRef': {'hostId': primary_node.host_id}}
    navigator_roles.append(navigator_audit_server_role)

    logger.info('Adding Navigator roles to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    deployment.create_cm_roles(navigator_roles)


def _install_kudu(deployment, kudu_version):
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


# Check if service needs to be added and if yes, validate the service version passed.
# Returns services to add if valid.
def _validate_services_to_add(cdh_version_tuple, cm_cluster, exclude_services,
                              include_services, kafka_version, kudu_version):
    services_to_validate = set()
    if kafka_version and _is_service_to_add('KAFKA', include_services, exclude_services):
        services_to_validate.add('KAFKA')
    if kudu_version and _is_service_to_add('KUDU', include_services, exclude_services):
        services_to_validate.add('KUDU')
    if services_to_validate:
        logger.info('Validating passed service versions ...')

    cluster_info = cm_cluster.get_cluster_info()
    if cluster_info is not None:
        cluster_display_name = cluster_info['displayName']
        inspect_hosts_contents = cm_cluster.download_command_output(cm_cluster.inspect_hosts())

    for service in services_to_validate:
        if service == 'KUDU':
            if cdh_version_tuple < EARLIEST_CDH_VERSION_WITH_KUDU:
                cdh_version_from_map = KUDU_PARCEL_VERSIONS.get(kudu_version)
                if cdh_version_from_map is None:
                    cdh_version = '.'.join(str(i) for i in cdh_version_tuple)
                    raise Exception('Kudu version {} is not available for '
                                    'CDH {} Aborting ...'.format(kudu_version, cdh_version))
            else:
                _validate_integrated_service_version(cdh_version_tuple, cluster_display_name,
                                                     EARLIEST_CDH_VERSION_WITH_KUDU,
                                                     inspect_hosts_contents, 'kudu', kudu_version)
        elif service == 'KAFKA':
            _validate_integrated_service_version(cdh_version_tuple, cluster_display_name,
                                                 EARLIEST_CDH_VERSION_WITH_KAFKA,
                                                 inspect_hosts_contents, 'kafka', kafka_version)

    return services_to_validate


def _validate_integrated_service_version(cdh_version_tuple, cluster_display_name, earliest_cdh_version_with_service,
                                         inspect_hosts_contents, service_name, service_version):
    if cdh_version_tuple >= earliest_cdh_version_with_service:
        # Check if the parameter service_version matches integrated service version for the specific CDH version.
        for item in inspect_hosts_contents['componentSetByCluster'][cluster_display_name][0]['componentInfo']:
            if item['name'] == service_name:
                break
        else:
            cdh_version = '.'.join(str(i) for i in cdh_version_tuple)
            raise Exception('Integrated {} version for CDH {} not found.'.format(service_name, cdh_version))
        cluster_service_version = item['componentVersion'].split('+')[0]
        if service_version != cluster_service_version:
            cdh_version = '.'.join(str(i) for i in cdh_version_tuple)
            raise Exception('{} version {} does not match integrated version {} for '
                            'CDH {} Aborting ...'.format(service_name, service_version,
                                                         cluster_service_version, cdh_version))


def _configure_kudu(deployment, cluster, kudu_version, cdh_version, single_node):
    cdh_version_tuple = tuple(int(i) if i.isdigit() else i for i in cdh_version.split('.'))

    if cdh_version_tuple < EARLIEST_CDH_VERSION_WITH_KUDU:
        _deactivate_service_parcel(deployment, cluster, 'KUDU', kudu_version)
        for parcel in deployment.cluster(DEFAULT_CLUSTER_NAME).parcels:
            if parcel.product == 'KUDU' and parcel.stage in 'ACTIVATED':
                break
        else:
            _install_parcel_from_remote_repo(deployment, 'KUDU', kudu_version, KUDU_PARCEL_VERSIONS[kudu_version])

    logger.info('Adding Kudu service to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    master_role = {'type': 'KUDU_MASTER',
                   'hostRef': {'hostId': cluster.primary_node.host_id},
                   'config': {'items': [{'name': 'fs_wal_dir', 'value': '/data/kudu/master'},
                                        {'name': 'fs_data_dirs', 'value': '/data/kudu/master'}]}}
    tserver_node_group = 'secondary' if not single_node else 'primary'
    tserver_roles = [{'type': 'KUDU_TSERVER',
                      'config': {'items': [
                          {'name': 'fs_wal_dir', 'value': '/data/kudu/tserver'},
                          {'name': 'fs_data_dirs', 'value': '/data/kudu/tserver'}
                      ]},
                      'hostRef': node.host_id}
                     for node in cluster if node.group == tserver_node_group]
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


def _setup_ssl(cluster, service_list):
    _setup_ssl_ca_authority(cluster)
    for service in service_list:
        # Python kerberos client for Kafka needs files to be generated using kafka.client.keystore.jks
        # This is generated in authentication setup. Hence call it always.
        _setup_ssl_encryption_authentication(cluster, service)


def _setup_ssl_ca_authority(cluster):
    ssl_ca_authority_commands = [
        'mkdir -p {}'.format(SSL_SERVER_CONTAINER_DIR),
        'mkdir -p {}'.format(SSL_CLIENT_CONTAINER_DIR),
        'cd {}'.format(SSL_SERVER_CONTAINER_DIR),
        ('openssl req -new -newkey rsa:4096 -days 365 -x509 -subj "/CN=Kafka-Security-CA"'
         ' -keyout ca-private-key -out ca-public-key -nodes'),
    ]
    cluster.primary_node.execute(' && '.join(ssl_ca_authority_commands))


def _setup_ssl_encryption(cluster, service):
    cur_service = service.lower()
    service_server_keystore = '{}.{}'.format(cur_service, SSL_SERVER_KEYSTORE)
    service_server_truststore = '{}.{}'.format(cur_service, SSL_SERVER_TRUSTSTORE)

    # Configuration of server side e.g. for Kafka broker.
    ssl_server_commands = [
        'export SRVPASS={}'.format(SSL_SERVER_PASSWORD),
        # Add JAVA_HOME to PATH to make keytool available.
        # awk needs single quotes to get the correct value.
        "export JAVA_HOME=`echo $(awk -F = '/JAVA_HOME/{print $NF}' /etc/default/cloudera-scm-server)`",
        'export PATH=$PATH:$JAVA_HOME/bin',
        # Create a broker certificate.
        'cd {}'.format(SSL_SERVER_CONTAINER_DIR),
        ('keytool -genkey -keystore {} -validity 365 -storepass $SRVPASS -keypass $SRVPASS -dname "CN=node-1.cluster" '
         '-storetype pkcs12').format(service_server_keystore),
        # Sign the broker certificate.
        ('keytool -keystore {} -certreq -file cert-file -storepass $SRVPASS '
         '-keypass $SRVPASS').format(service_server_keystore),
        ('openssl x509 -req -CA ca-public-key -CAkey ca-private-key -in cert-file -out cert-signed '
         '-days 365 -CAcreateserial -passin pass:$SRVPASS'),
        # Create a truststore for Kafka broker.
        ('keytool -keystore {} -alias CARoot -import -file ca-public-key -storepass $SRVPASS '
         '-keypass $SRVPASS -noprompt').format(service_server_truststore),
        # Import public certificate and signed certificate to keystore.
        ('keytool -keystore {} -alias CARoot -import -file ca-public-key -storepass $SRVPASS '
         '-keypass $SRVPASS -noprompt').format(service_server_keystore),
        ('keytool -keystore {} -import -file cert-signed -storepass $SRVPASS '
         '-keypass $SRVPASS -noprompt').format(service_server_keystore)
    ]

    service_client_truststore = '{}.{}'.format(cur_service, SSL_CLIENT_TRUSTSTORE)
    # Setup a truststore for Kafka client.
    ssl_client_commands = [
        'cd {}'.format(SSL_CLIENT_CONTAINER_DIR),
        ('keytool -keystore {0} -alias CARoot -import -file {1}/ca-public-key -storepass {2} '
         '-keypass {2} -noprompt').format(service_client_truststore, SSL_SERVER_CONTAINER_DIR, SSL_CLIENT_PASSWORD)
    ]
    ssl_encryption_commands = ssl_server_commands + ssl_client_commands
    cluster.primary_node.execute(' && '.join(ssl_encryption_commands))


def _setup_ssl_encryption_authentication(cluster, service):
    _setup_ssl_encryption(cluster, service)

    service_small_case = service.lower()
    service_client_keystore = '{}.{}'.format(service_small_case, SSL_CLIENT_KEYSTORE)

    ssl_authentication_commands = [
        'cd {}'.format(SSL_CLIENT_CONTAINER_DIR),
        'export CLIPASS={}'.format(SSL_CLIENT_PASSWORD),
        # Add JAVA_HOME to PATH to make keytool available.
        # awk needs single quotes to get the correct value.
        "export JAVA_HOME=`echo $(awk -F = '/JAVA_HOME/{print $NF}' /etc/default/cloudera-scm-server)`",
        'export PATH=$PATH:$JAVA_HOME/bin',
        # Create a keystore for the client.
        ('keytool -genkey -keystore {} -validity 365 -storepass $CLIPASS -keypass $CLIPASS -dname "CN=node-1.cluster" '
         '-alias my-cluster -storetype pkcs12').format(service_client_keystore),
        # Sign the client certificate.
        ('keytool -keystore {} -certreq -file client-cert-sign-req -alias my-cluster -storepass $CLIPASS '
         '-keypass $CLIPASS').format(service_client_keystore),
        ('openssl x509 -req -CA {0}/ca-public-key -CAkey {0}/ca-private-key -in client-cert-sign-req '
         '-out client-cert-signed -days 365 -CAcreateserial '
         '-passin pass:serversecret').format(SSL_SERVER_CONTAINER_DIR),
        # Add the signed certificate and public key to the client certificate.
        ('keytool -keystore {} -alias CARoot -import -file {}/ca-public-key -storepass $CLIPASS '
         '-keypass $CLIPASS -noprompt').format(service_client_keystore, SSL_SERVER_CONTAINER_DIR),
        ('keytool -keystore {} -import -file client-cert-signed -alias my-cluster -storepass $CLIPASS '
         '-keypass $CLIPASS -noprompt').format(service_client_keystore)
    ]

    # Python Kafka client needs following files to connect with SSL enabled or authenticated Kafka.
    service_certificate = '{}.certificate.pem'.format(service_small_case)
    service_cert_and_key_p12 = '{}.cert_and_key.p12'.format(service_small_case)
    service_key = '{}.key.pem'.format(service_small_case)
    service_caroot_certificate = '{}.CARoot.pem'.format(service_small_case)
    ssl_commands = [('keytool -exportcert -alias my-cluster -keystore {} -rfc -file {} '
                     '-storepass $CLIPASS').format(service_client_keystore, service_certificate),
                    ('keytool -v -importkeystore -srckeystore {} -srcalias my-cluster -destkeystore {} '
                     '-deststoretype PKCS12 -deststorepass $CLIPASS '
                     '-srcstorepass $CLIPASS').format(service_client_keystore, service_cert_and_key_p12),
                    ('openssl pkcs12 -in {} -nocerts -nodes -password "pass:$CLIPASS" | '
                     'sed -ne "/-BEGIN PRIVATE KEY-/,/-END PRIVATE KEY-/p" '
                     '> {}').format(service_cert_and_key_p12, service_key),
                    ('keytool -exportcert -alias CARoot -keystore {} -rfc '
                     '-file {} -storepass $CLIPASS').format(service_client_keystore, service_caroot_certificate)]

    cluster.primary_node.execute(' && '.join(ssl_authentication_commands + ssl_commands))


def _get_navigator_stage_lib_name(cm_version, additional_stage_libs_version=None):
    cm_version_tuple = tuple(cm_version.split('.'))
    navigator_stage_lib_prefix = 'streamsets-datacollector-cm_{}_{}-lib'.format(*cm_version_tuple[:2])
    return ('{}-{}'.format(navigator_stage_lib_prefix, additional_stage_libs_version)
            if additional_stage_libs_version else navigator_stage_lib_prefix)


def _configure_sdc(args, cdh_version_tuple, cluster, data_collector, deployment):
    logger.info('Adding StreamSets service to cluster (%s) ...', DEFAULT_CLUSTER_NAME)
    primary_node = cluster.primary_node
    datacollector_role = {'type': 'DATACOLLECTOR',
                          'hostRef': {'hostId': primary_node.host_id}}
    deployment.create_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME,
                                       services=[{'name': 'streamsets',
                                                  'type': 'STREAMSETS',
                                                  'displayName': 'StreamSets',
                                                  'roles': [datacollector_role]}])

    cluster_service_types = {service['type']
                             for service
                             in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME)}

    configs = {}
    environment_variables = {}
    if 'SPARK2_ON_YARN' in cluster_service_types:
        # When running an application  with Spark2, the following
        # environment variables must be set before starting StreamSets Data Collector.
        environment_variables.update({'SPARK_SUBMIT_YARN_COMMAND': '/usr/bin/spark2-submit',
                                      'SPARK_KAFKA_VERSION': '0.10',
                                      'SPARK_HOME': '/opt/cloudera/parcels/SPARK2/lib/spark2'})
    elif 'SPARK_ON_YARN' in cluster_service_types:
        # When running an application on YARN, the Spark executor requires access to the spark-submit script located in
        # the Spark installation directory. Default is directory specified by SPARK_HOME environment variable.
        # Hence SPARK_HOME environment variable must be set before starting StreamSets Data Collector.
        environment_variables.update({'SPARK_HOME': '/opt/cloudera/parcels/CDH/lib/spark'})
        if cdh_version_tuple >= EARLIEST_CDH_VERSION_WITH_SPARK2:
            environment_variables.update({'SPARK_KAFKA_VERSION': '0.10'})

    if args.navigator:
        navigator_stage_lib = _get_navigator_stage_lib_name(args.cm_version)
        sdc_properties = sdc.PROPERTIES_FOR_NAVIGATOR.format(navigator_node_fqdn=primary_node.fqdn,
                                                             navigator_port=NAVIGATOR_PORT,
                                                             navigator_stage_lib=navigator_stage_lib,
                                                             sdc_node_fqdn=primary_node.fqdn,
                                                             sdc_port=sdc.SDC_PORT)
        configs['sdc.properties_role_safety_valve'] = sdc_properties

    if data_collector.user_libs_exist:
        environment_variables.update({'USER_LIBRARIES_DIR': sdc.USER_LIBS_PATH})
        configs['sdc-security.policy_role_safety_valve'] = sdc.USER_LIBS_SECURITY_POLICY

    if environment_variables:
        configs.update({'sdc-env.sh_role_safety_valve': '\n'.join('export {}={}'.format(key, value)
                                                                  for key, value in environment_variables.items())})

    if args.kerberos:
        # Create JAAS config file on node-1. Needed to access kerberized Kafka.
        sdc_principal = 'sdc/{kafka_node_name}@{realm}'.format(kafka_node_name=primary_node.fqdn,
                                                               realm=cluster.network.upper())
        primary_node.put_file(JAAS_CONFIG_FILE_PATH, JAAS_CONFIG.format(KERBEROS_CONFIG_CONTAINER_DIR, sdc_principal))
        # Configure SDC to use the JAAS config file created above.
        opts = '-Djava.security.auth.login.config={0} -Dsun.security.krb5.debug=true'.format(JAAS_CONFIG_FILE_PATH)
        configs['java.opts'] = opts

    # Increase the maximum number of stage libraries SDC allows.
    # This avoids errors of the sort 'classloader pool exhausted' when huge number of tests are run.
    configs['max.stage.private.classloaders'] = 1000

    for role_config_group in deployment.get_service_role_config_groups(DEFAULT_CLUSTER_NAME,
                                                                       'streamsets'):
        deployment.update_service_role_config_group_config(DEFAULT_CLUSTER_NAME,
                                                           'streamsets',
                                                           role_config_group['name'],
                                                           configs)


def _set_cm_server_java_home(node, java_home):
    node.execute('sed -i "/JAVA_HOME/d" /etc/default/cloudera-scm-server')
    commands = ['sed -i "/JAVA_HOME/d" {}'.format(CM_SERVER_ETC_DEFAULT),
                'echo "export JAVA_HOME={}" >> {}'.format(java_home,
                                                          CM_SERVER_ETC_DEFAULT)]
    logger.info('Setting JAVA_HOME to %s in %s ...',
                java_home,
                CM_SERVER_ETC_DEFAULT)
    node.execute(command=' && '.join(commands))


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


def _restart_cm_agents(nodes, cdh_version_tuple):
    # Supervisor issues were seen when restarting the SCM agent;
    # doing a clean_restart and disabling quiet mode for the execution
    # were empirically determined to be necessary.
    centos_major_version = nodes[0].execute("rpm -q --queryformat '%{VERSION}' centos-release", quiet=True)

    command = ('service cloudera-scm-agent restart' if cdh_version_tuple >= EARLIEST_CDH_VERSION_WITH_CENTOS6_8
               else 'service cloudera-scm-agent clean_restart_confirmed')
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
                       time_between_checks=3, timeout=360, success=success, failure=failure)


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
                       time_between_checks=3, timeout=360, success=success, failure=failure)


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
                       time_between_checks=3, timeout=360, success=success, failure=failure)


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
                       time_between_checks=3, timeout=1200, time_to_success=30,
                       success=success, failure=failure)


def _execute_commands_against_kerberized_service(cluster, commands, service_keytab_directory_suffix,
                                                 service_name, service_node_fqdn):
    principal = '{service_name}/{service_node}@{realm}'.format(service_name=service_name,
                                                               service_node=service_node_fqdn,
                                                               realm=cluster.network.upper())
    kinit_commands = ['cd /var/run/cloudera-scm-agent/process',
                      ('kinit -k -t "$(ls -d *{} | sort -nr '
                       '| head -1)/{}.keytab" {}'.format(service_keytab_directory_suffix, service_name, principal))]
    kdestroy_command = ['kdestroy']
    cluster.primary_node.execute(' && '.join(kinit_commands + commands + kdestroy_command))


def _configure_after_start(deployment, cluster_name, cluster, quiet, kerberos_principals):
    # To contact kerberized HDFS using keytab of any of the Kerberos principals,
    # for each Kerberos principal there needs to be HDFS /user/{principal} directory.
    if kerberos_principals:
        cluster_service_types = {service['type']
                                 for service
                                 in deployment.get_cluster_services(cluster_name)}

        if 'HDFS' in cluster_service_types:
            dir_command = 'hadoop fs -mkdir /user/{0} && hadoop fs -chown {0}:{0} /user/{0}'
            dir_commands = [dir_command.format(primary) for primary in kerberos_principals.split(',')]
            _execute_commands_against_kerberized_service(cluster, dir_commands, 'hdfs-NAMENODE',
                                                         'hdfs', cluster.primary_node.fqdn)


def _configure_for_streamsets_before_start(deployment, cluster, cluster_name, sdc_resources_directory):
    if sdc_resources_directory:
        logger.info('Setting correct permissions for sdc resources recursively ...')
        cluster.primary_node.execute('chown -R sdc:sdc {}'.format(os.path.dirname(sdc.RESOURCES_DIRECTORY)))

    logger.info('Adding HDFS proxy user ...')
    for service in deployment.get_cluster_services(cluster_name=cluster_name):
        if service['type'] == 'HDFS':
            configs = {'core_site_safety_valve': ('<property>'
                                                  '<name>hadoop.proxyuser.sdc.hosts</name>'
                                                  '<value>*</value>'
                                                  '</property>'
                                                  '<property>'
                                                  '<name>hadoop.proxyuser.sdc.users</name>'
                                                  '<value>*</value>'
                                                  '</property>')}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)
            break

    logger.info('Disabling security for HBase Thrift server ...')
    for service in deployment.get_cluster_services(cluster_name=cluster_name):
        if service['type'] == 'HBASE':
            configs = {'hbase_thriftserver_security_authentication': 'none'}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)
            break


def _configure_yarn(deployment, cluster, cluster_name):
    logger.info('Configuring Yarn ...')

    for role_config_group in deployment.get_service_role_config_groups(cluster_name, 'yarn'):
        if role_config_group['roleType'] == 'NODEMANAGER':
            configs = deployment.get_service_role_config_group_config(cluster_name,
                                                                      'yarn',
                                                                      role_config_group['name'],
                                                                      view='full')
            whitelisted_users = configs['container_executor_allowed_system_users']
            configs = {'container_executor_allowed_system_users': '{},sdc'.format(whitelisted_users)}
            deployment.update_service_role_config_group_config(cluster_name,
                                                               'yarn',
                                                               role_config_group['name'],
                                                               configs)


def _configure_for_streamsets_after_start(deployment, cluster_name, cluster, quiet,
                                          kerberos_enabled, kerberos_principals, navigator_enabled):
    primary_node = cluster.primary_node
    if navigator_enabled:
        command = 'chown -R sdc:sdc {}'.format(sdc.USER_LIBS_PATH)
        primary_node.execute(command)

    # Following is needed for Kerberos and Kafka to work correctly.
    if kerberos_enabled:
        logger.info('Copying streamsets keytab to a fixed location which is shared on all clustered nodes ...')
        commands = ['cd /var/run/cloudera-scm-agent/process',
                    ('cp "$(ls -d *streamsets-DATACOLLECTOR | sort -nr | head -1)/streamsets.keytab" '
                     '{}/streamsets.keytab').format(KERBEROS_CONFIG_CONTAINER_DIR),
                    'chown sdc:sdc {}/streamsets.keytab'.format(KERBEROS_CONFIG_CONTAINER_DIR),
                    'chown sdc:sdc {}'.format(JAAS_CONFIG_FILE_PATH)]
        cluster.primary_node.execute(' && '.join(commands))

    cluster_service_types = {service['type']
                             for service
                             in deployment.get_cluster_services(cluster_name=cluster_name)}

    if 'HDFS' in cluster_service_types and kerberos_principals:
        if 'sdctest' in kerberos_principals.split(','):
            # During testing, SDC generates some HDFS files and folders as `sdc` user.
            # To delete them after testing, python clients use `sdctest` user.
            # To make this delete possible by `sdctest`, on CDH cluster nodes, add `sdctest` user to supergroup
            # which is HDFS superuser group.
            commands = ['groupadd supergroup',
                        'usermod -a -G supergroup sdctest']
            for node_group_name in ['primary', 'secondary']:
                cluster.node_groups[node_group_name].execute(' && '.join(commands), quiet=quiet)

    if 'SOLR' in cluster_service_types:
        SOLR_CONFIG_FILE_PATH = '/root/sample_collection_solr_configs/conf/solrconfig.xml'

        logger.info('Creating sample schemaless collection for Solr ...')
        primary_node.execute('solrctl instancedir --generate '
                             '/root/sample_collection_solr_configs -schemaless', quiet=quiet)
        solr_config = primary_node.get_file(SOLR_CONFIG_FILE_PATH)
        primary_node.put_file(SOLR_CONFIG_FILE_PATH,
                              re.sub(r'<!--<(str name="df")>text<(/str)>-->',
                                     r'<\1>id<\2>',
                                     solr_config))
        primary_node.execute('solrctl instancedir --create sample_collection '
                             '/root/sample_collection_solr_configs', quiet=quiet)

        command = 'solrctl collection --create sample_collection -s 1 -c sample_collection'
        if kerberos_enabled:
            _execute_commands_against_kerberized_service(cluster, [command], 'solr-SOLR_SERVER',
                                                         'solr', cluster.primary_node.fqdn)
        else:
            primary_node.execute(command, quiet=quiet)
