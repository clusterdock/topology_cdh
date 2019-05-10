============================
CDH topology for clusterdock
============================

This repository houses the **CDH** topology for `clusterdock`_.

.. _clusterdock: https://github.com/clusterdock/clusterdock

Usage
=====

Assuming you've already installed **clusterdock** (if not, go `read the docs`_),
you use this topology by cloning it to a local folder and then running commands
with the ``clusterdock`` script:

For instructions on building the Docker images for the CDH nodes, refer to the
``BUILDING_CDH_IMAGES.md`` file.

.. _read the docs: http://clusterdock.readthedocs.io/en/latest/

.. code-block:: console

    $ git clone https://github.com/clusterdock/topology_cdh.git
    $ clusterdock start topology_cdh

To see full usage instructions for the ``start`` action, use ``-h``/``--help``:

.. code-block:: console

    $ clusterdock topology_cdh start -h
    usage: clusterdock start [--always-pull] [--namespace ns] [--network nw]
                             [-o sys] [-r url] [-h] [--cdh-version ver]
                             [--cm-version ver] [--dont-start-cluster]
                             [--exclude-services svc1,svc2,...]
                             [--include-services svc1,svc2,...] [--java ver]
                             [--kafka-version ver] [--kudu-version ver]
                             [--primary-node node [node ...]]
                             [--secondary-nodes node [node ...]]
                             topology

    Start a CDH cluster

    positional arguments:
      topology              A clusterdock topology directory

    optional arguments:
      --always-pull         Pull latest images, even if they're available locally
                            (default: False)
      --namespace ns        Namespace to use when looking for images (default:
                            None)
      --network nw          Docker network to use (default: cluster)
      -o sys, --operating-system sys
                            Operating system to use for cluster nodes (default:
                            centos6.6)
      -r url, --registry url
                            Docker Registry from which to pull images (default:
                            docker.io)
      -h, --help            show this help message and exit

    CDH arguments:
      --cdh-version ver     CDH version to use (default: 5.12.0)
      --cm-version ver      CM version to use (default: 5.12.0)
      --dont-start-cluster  Don't start clusters/services in Cloudera Manager
                            (default: False)
      --exclude-services svc1,svc2,..., -x svc1,svc2,...
                            If specified, a comma-separated list of service types
                            to exclude from the CDH cluster (default: None)
      --include-services svc1,svc2,..., -i svc1,svc2,...
                            If specified, a comma-separated list of service types
                            to include in the CDH cluster (default: None)
      --java ver            If specified, a Java version to use in place of the
                            default value present on the cluster (default: None)
      --kafka-version ver   If specified, the version of Kafka to install from
                            archive.cloudera.com (default: None)
      --kudu-version ver    If specified, the version of Kudu to install from
                            archive.cloudera.com (default: None)
      --spark2-version ver  If specified, the version of Spark2 to install from
                            archive.cloudera.com (default: None)

    Node groups:
      --primary-node node [node ...]
                            Nodes of the primary-node group (default: ['node-1'])
      --secondary-nodes node [node ...]
                            Nodes of the secondary-nodes group (default:
                            ['node-2'])
