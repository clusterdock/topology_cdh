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

import docker

logger = logging.getLogger('clusterdock.{}'.format(__name__))

APACHE_KUDU_LEGACY_STAGE_LIB_TEMPLATE = 'streamsets-datacollector-apache-kudu_{}_{}-lib'
CDH_KAFKA_LEGACY_STAGE_LIB_TEMPLATE = 'streamsets-datacollector-cdh_kafka_{}_{}-lib'
CDH_LEGACY_STAGE_LIB_TEMPLATE = 'streamsets-datacollector-cdh_{}_{}-lib'
DATAPROTECTOR_STAGE_LIB_TEMPLATE = 'streamsets-datacollector-dataprotector-lib-{}'
EARLIEST_SDC_VERSION_WITH_LEGACY_MANIFEST = (3, 0, 0, 0)
NAVIGATOR_STAGE_LIB_TEMPLATE = 'streamsets-datacollector-cm_{}_{}-lib'

IMAGE_NAME_TEMPLATE = '{}/{}/clusterdock:topology_cdh-streamsets_datacollector-{}'
IMAGE_NAME_NO_REGISTRY_TEMPLATE = '{}/clusterdock:topology_cdh-streamsets_datacollector-{}'

SDC_PORT = 18630
PROPERTIES_FOR_NAVIGATOR = """lineage.publishers=navigator
lineage.publisher.navigator.def={navigator_stage_lib}::com_streamsets_pipeline_stage_plugin_navigator_NavigatorLineagePublisher
lineage.publisher.navigator.config.application_url=http://{sdc_node_fqdn}:{sdc_port}
lineage.publisher.navigator.config.navigator_url=http://{navigator_node_fqdn}:{navigator_port}
lineage.publisher.navigator.config.namespace=sdc
lineage.publisher.navigator.config.username=admin
lineage.publisher.navigator.config.password=admin
lineage.publisher.navigator.config.autocommit=true"""
RESOURCES_DIRECTORY = '/var/lib/sdc/resources'  # Default path for $SDC_RESOURCES.
# Location of SDC user libs on the cluster node where SDC is installed.
# Docker images of the SDC user libs volume mount to the following location.
USER_LIBS_PATH = '/opt/streamsets-datacollector-user-libs/'
USER_LIBS_SECURITY_POLICY = """grant codebase "file:///opt/streamsets-datacollector-user-libs/-" {
  permission java.security.AllPermission;
};"""

docker_client = docker.from_env(timeout=300)


class StreamsetsDataCollector:

    def __init__(self, version, namespace, registry):
        self._image_name = IMAGE_NAME_TEMPLATE.format(registry, namespace, version)
        self.image_name_no_registry = IMAGE_NAME_NO_REGISTRY_TEMPLATE.format(namespace, version)
        self.namespace = namespace
        self.registry = registry
        self.version = version
        self.version_tuple = tuple(int(i) if i.isdigit() else i for i in version.split('.'))
        self._user_libs_exist = False

    @property
    def image_name(self):
        return self._image_name

    @property
    def user_libs_exist(self):
        return self._user_libs_exist

    def get_navigator_stage_lib(self, cm_version, additional_stage_libs_version=None):
        """ Returns name of navigator stage lib name.

            Args:
                cm_version (:obj:`str`): Version of Cloudera Manager.
                additional_stage_libs_version (:obj:`str`): Version of additional stage libs.

            Returns:
                Name of navigator stage lib.
        """
        if not additional_stage_libs_version:
            return None
        cm_version_tuple = tuple(cm_version.split('.'))
        navigator_stage_lib_prefix = NAVIGATOR_STAGE_LIB_TEMPLATE.format(*cm_version_tuple[:2])
        navigator_stage_lib = ('{}-{}'.format(navigator_stage_lib_prefix, additional_stage_libs_version))
        self._user_libs_exist = True
        return '{}/{}/additional-datacollector-libs:{}'.format(self.registry, self.namespace, navigator_stage_lib)

    def get_dataprotector_stage_lib(self, additional_stage_libs_version=None):
        """ Returns name of dataprotector stage lib name.

            Args:
                additional_stage_libs_version (:obj:`str`): Version of additional stage libs.

            Returns:
                Name of dataprotector stage lib.
        """
        if not additional_stage_libs_version:
            return ''
        stage_lib_name = DATAPROTECTOR_STAGE_LIB_TEMPLATE.format(additional_stage_libs_version)
        self._user_libs_exist = True
        return '{}/{}/additional-datacollector-libs:{}'.format(self.registry, self.namespace, stage_lib_name)

    def _get_legacy_libs(self):
        """Fetch the legacy stage libs from label of the SDC parcel image. Returns it as a string."""
        if self.version_tuple < EARLIEST_SDC_VERSION_WITH_LEGACY_MANIFEST:
            # No legacy stage libs list is available.
            return None
        # Inspect the image and get the value for label called 'legacy-libs'.
        # This label is set while the image of SDC parcel was built.
        try:
            inspect_image_results = docker_client.api.inspect_image(self.image_name_no_registry)
        except docker.errors.NotFound as not_found:
            if (not_found.response.status_code == 404 and
                    'No such image' in not_found.explanation):
                logger.info('Could not find %s locally. Attempting to pull ...', self.image_name_no_registry)
                docker_client.images.pull(self.image_name_no_registry)
                inspect_image_results = docker_client.api.inspect_image(self.image_name_no_registry)

        image_labels = inspect_image_results['ContainerConfig']['Labels']
        return image_labels.get('legacy-libs', None)

    def get_legacy_stage_lib_images(self, cdh_version, kafka_version=None, kudu_version=None):
        """ Gets a list of applicable legacy stage lib names.
            Parses the legacy stage-lib-manifest.properties file to determine the
            legacy stage libs applicable for this SDC version, cdh_version, kafka_version and kudu_version.

            Args:
                cdh_version (:obj:`str`): Version of CDH.
                kafka_version (:obj:`str`): Version of CDH Kafka.
                kudu_version (:obj:`str`): Version of Apache Kudu.

            Returns:
                A list of applicable legacy stage lib names.
        """
        legacy_stage_libs_index = self._get_legacy_libs()
        result = []
        if legacy_stage_libs_index is None:
            return result

        cdh_version_tuple = tuple(int(i) if i.isdigit() else i for i in cdh_version.split('.'))
        cdh_stage_lib_name = CDH_LEGACY_STAGE_LIB_TEMPLATE.format(*cdh_version_tuple[:2])
        if legacy_stage_libs_index.find(cdh_stage_lib_name) != -1:
            result.append(cdh_stage_lib_name)

        if kafka_version:
            kafka_version_tuple = tuple(int(i) if i.isdigit() else i for i in kafka_version.split('.'))
            kafka_stage_lib_name = CDH_KAFKA_LEGACY_STAGE_LIB_TEMPLATE.format(*kafka_version_tuple[:2])
            if legacy_stage_libs_index.find(kafka_stage_lib_name) != -1:
                result.append(kafka_stage_lib_name)

        if kudu_version:
            kudu_version_tuple = tuple(int(i) if i.isdigit() else i for i in kudu_version.split('.'))
            kudu_stage_lib_name = APACHE_KUDU_LEGACY_STAGE_LIB_TEMPLATE.format(*kudu_version_tuple[:2])
            if legacy_stage_libs_index.find(kudu_stage_lib_name) != -1:
                result.append(kudu_stage_lib_name)

        # Prefix with repo and suffix with SDC version.
        if result == []:
            return result
        else:
            self._user_libs_exist = True
            return ['{}/{}/datacollector-libs:{}-{}'.format(self.registry, self.namespace,
                                                            item, self.version) for item in result]
