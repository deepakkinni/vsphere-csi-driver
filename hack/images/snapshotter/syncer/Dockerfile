# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This build arg allows the specification of a custom base image.
ARG BASE_IMAGE=gcr.io/cloud-provider-vsphere/extra/csi-driver-base:latest

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${BASE_IMAGE}

COPY /.build/bin/syncer.linux_amd64 /bin/vsphere-syncer

RUN mkdir -p config

ADD pkg/apis/cnsoperator/config/cnsregistervolume_crd.yaml /config/

ADD pkg/apis/cnsoperator/config/cnsfileaccessconfig_crd.yaml /config/

ADD pkg/internal/cnsoperator/config/cnsfilevolumeclient_crd.yaml /config/

ENTRYPOINT ["/bin/vsphere-syncer"]
