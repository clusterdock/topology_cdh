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
FROM alpine:latest
MAINTAINER Dima Spivak <dima@spivak.ch>

ARG CSD_DIRECTORY=/opt/cloudera/csd
ARG CSD_URL=http://archives.streamsets.com/datacollector/2.7.2.0/csd/STREAMSETS-2.7.2.0.jar

ARG PARCEL_DIRECTORY=/opt/cloudera/parcels/STREAMSETS_DATACOLLECTOR-2.7.2.0
ARG PARCEL_URL=http://archives.streamsets.com/datacollector/2.7.2.0/parcel/STREAMSETS_DATACOLLECTOR-2.7.2.0-el6.parcel

RUN apk --no-cache add tar

RUN wget -O /parcel.tgz ${PARCEL_URL} && \
    mkdir -p "$(dirname ${PARCEL_DIRECTORY})" && \
    tar xf /parcel.tgz -C "$(dirname ${PARCEL_DIRECTORY})" && \
    rm /parcel.tgz

# See http://community.cloudera.com/t5/Cloudera-Manager-Installation/Stop-CM-undistributing-my-parcel/m-p/61402.
RUN touch "${PARCEL_DIRECTORY}/.dont_delete"

VOLUME ${PARCEL_DIRECTORY}

RUN mkdir -p "${CSD_DIRECTORY}" && \
    wget -O "${CSD_DIRECTORY}/$(basename ${CSD_URL})" "${CSD_URL}"

VOLUME ${CSD_DIRECTORY}

CMD ["/bin/true"]
