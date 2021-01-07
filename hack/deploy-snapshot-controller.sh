#!/bin/bash

# kubectl is required to run this script

if ! command -v kubectl > /dev/null; then
  echo "kubectl is missing"
  echo "please refer to https://kubernetes.io/docs/tasks/tools/install-kubectl/ to install kubectl"
  exit 1
fi

# deploy csi-snapshot crds
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v4.0.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v4.0.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v4.0.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml

# deploy snapshot controller
DIR=$(dirname $0)

kubectl apply -f $DIR/manifests/rbac-snapshot-controller.yaml
kubectl apply -f $DIR/manifests/setup-snapshot-controller.yaml
