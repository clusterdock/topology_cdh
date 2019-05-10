## Creating StreamSets clusterdock images for CDH

### Dima Spivak

## Overview

The clusterdock topology for CDH allows a user to start up an _n_-node CDH cluster running on one host using Docker containers. The topology relies upon Docker images being pre-created and available on Docker Hub. StreamSets maintains a private Docker repository called _streamsets/clusterdock _to house these images; for licensing reasons, **these are only suitable for internal use**. The images are created by starting up a _nodebase _cluster (i.e. one that just runs a base operating system in containers) and manually installing and doing basic configuration of a Cloudera Manager-based CDH cluster. 

## CDH

This section describes the steps required to complete this image creation process as of Cloudera Manager’s 5.11.1 and CDH 6.x releases.

### Steps

1. For CDH 5.x, Start two-node nodebase cluster, forwarding port 7180 on `node-1`to the host:
    `clusterdock start topology_nodebase -p 'node-1:7180->7180'`

    1. For CDH 6.x, use `-o centos7.4` like following:
    `git clone https://github.com/clusterdock/topology_nodebase`
    1. Add following lines to `topology_nodebase/start.py`:
        ```
        if 'centos7' in args.operating_system:
                for node in cluster.nodes:
                    node.volumes.append({'/sys/fs/cgroup': '/sys/fs/cgroup'})
                    # do not use tempfile.mkdtemp, as systemd wont be able to bring services up when temp ends to be created in
                    # /var/tmp/ directory
                    node.volumes.append(['/run', '/run/lock'])
         Before line cluster.start(args.network)
        ```
    1. Start two-node nodebase cluster with CentOS 7.4:
    ```
    clusterdock start topology_nodebase -p 'node-1:7180->7180' -o centos7.4
    ```
1. SSH into each node (using `clusterdock ssh`) and prepare environment for CM requirements around mounted directories: \
`for i in /etc/hosts /etc/resolv.conf /etc/hostname /etc/localtime; do /bin/cp -f ${i} ${i}.1; umount ${i}; /bin/mv -f ${i}.1 ${i}; done; /bin/cp -f /usr/share/zoneinfo/UTC /etc/localtime`
1. Go back to `node-1` and get/run the installer:
    - Installer
        - For CDH 5.x,
            ```
            wget http://archive.cloudera.com/cm5/installer/latest/cloudera-manager-installer.bin
            ```
        - For CDH 6.x,
        
            | Version | Command |
            | --------|---------|
            | 6.0.1   | `wget https://archive.cloudera.com/cm6/6.0.1/cloudera-manager-installer.bin` |
            | 6.1.1   | `wget https://archive.cloudera.com/cm6/6.1.1/cloudera-manager-installer.bin` |
            | 6.2.0   | `wget https://archive.cloudera.com/cm6/6.2.0/cloudera-manager-installer.bin` |

    - `chmod u+x ./cloudera-manager-installer.bin`
    - `./cloudera-manager-installer.bin`
    - `rm -f ./cloudera-manager-installer.bin`
1. Open your web browser and go to `<host>:7180` to continue the installation.
1. Choose to install Cloudera Express and tell the installer to use both nodes started as part of the nodebase cluster (enter `node-1.cluster` `node-2.cluster` in box). During the installation process, tell the wizard to use parcels. When prompted, install Oracle JDK (for CDH 6.x, make sure to accept 2 licenses under JDK, second one is for Install Java Unlimited Strength Encryption Policy Files), don't enable single user mode, and point the installation at the nodebase private SSH key.
1. For CDH 6.0.1 and CDH 6.1.1, before ‘Inspect Hosts’ screen, install psycopg2 version above 2.5.4 on all nodes of cluster. Hue requires that. Since we do not install Hue, this is optional but I did it since this is helpful if someone wants to install Hue after cluster start.
    ```
    yum install -y epel-release 
    yum install -y python-pip
    pip install psycopg2==2.7.5
    ```
1. After the CM agent is installed, but before you select services (i.e. once you get to the screen that asks you to select which services to install),  add aufs and overlay filesystems to CM agent local filesystem whitelist. SSH into each node of the cluster and run: \
`sed -i "/local_filesystem_whitelist/ s/$/,aufs,overlay/" /etc/cloudera-scm-agent/config.ini && service cloudera-scm-agent restart`
1. For CDH 5.x: Go ahead and tell the wizard to install "All Services." 

    For CDH 6.x: Go ahead and tell the wizard to install "Custom Services."  Do not select services Hue, Oozie (as Oozie had problems and Hue is dependent on Oozie), Isilon, Kafka, Kudu.

    Default role assignments are usually fine, with the following exceptions:

    1. Add an HDFS httpfs role to `node-1`
    1. Add an HBase Thrift role to `node-1`
1. When prompted, tell the installer to use embedded databases with default credentials. Otherwise, click through and wait for First Run to complete.
1. Once services are started, do the following to your cluster:
    1. In CM, set the HDFS replication factor (`dfs_replication`) to 1. 
    1. SSH to a node and set the HDFS replication factor to 1 and then redeploy client configurations for the affected services in Cloudera Manager: \
`sudo -u hdfs hdfs dfs -setrep -R 1 /`
11. Create a /user/sdc folder in HDFS: \
`sudo -u hdfs hdfs dfs -mkdir /user/sdc \
sudo -u hdfs hdfs dfs -chown sdc:sdc /user/sdc`
12. Disable CM management of parcels (set `manages_parcels` to false).
13. Disable host clock offset check (set `host_health_suppression_host_clock_offset` to true). Due to a bug in CM, the easiest way to do this is to go to [http://node-1.cluster:7180/cmf/hardware/hosts/config?task=ALL&q=host_health_suppression_host_clock_offset](http://node-1.cluster:7180/cmf/hardware/hosts/config?task=ALL&q=host_health_suppression_host_clock_offset).
14. Disable embedded DB check (set `enable_embedded_db_check` to false).
15. Rename `Cluster 1` to `Cluster 1 (clusterdock)` through the UI.
16. **For CDH 6.x onwards only**:

    Our SSL functionality generates SSL keys using Java. For that it needs to know JAVA installation location and so set it in this step.

    **Only for CDH 6.0.1**: Cloudera installs version 1.8.0_141 and that does not include `Java Unlimited Strength Encryption Policy Files` which are needed for Kerberos functionality.

    So download proper JDK version and install it.

    On all cluster nodes,

    `wget` [https://archive.cloudera.com/cm6/6.1.1/redhat7/yum/RPMS/x86_64/oracle-j2sdk1.8-1.8.0+update181-1.x86_64.rpm](https://archive.cloudera.com/cm6/6.1.1/redhat7/yum/RPMS/x86_64/oracle-j2sdk1.8-1.8.0+update181-1.x86_64.rpm)

    ```
    yum install -y oracle-j2sdk1.8-1.8.0+update181-1.x86_64.rpm

    ```

    **For all CDH 6.x versions:** 

    open file `/etc/default/cloudera-scm-server` and add/change a line for `JAVA_HOME`.

    ```
    export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera
    ```

    Restart Cloudera Manager server.

    ```
    service cloudera-scm-server restart
    ```

1. Restart any stale services and ensure everything comes up as healthy.
1. Stop the cluster services.
1. Stop the Cloudera Management service.
1. From each node, remove contents of CM parcel folders to reduce image size: \
`rm -rf /opt/cloudera/parcel-repo/* /opt/cloudera/parcels/.flood/*`
1. From each node, clean yum and Bash history: \
`yum clean all \
cat /dev/null > ~/.bash_history && history -c`
1. Commit images. Use the following command as a template. Note that we need to add the `-c  'VOLUME /data/kudu'` command to workaround KUDU-1419. As an example from CDH 5.11.0: \
    ```
    docker commit -c 'VOLUME /data/kudu' <primary node container id> streamsets/clusterdock:cdh5.11.0_cm5.11.0_primary-node
    docker commit -c 'VOLUME /data/kudu' <secondary node container id> streamsets/clusterdock:cdh5.11.0_cm5.11.0_secondary-node
    ```
1. Push images to Docker Hub.

