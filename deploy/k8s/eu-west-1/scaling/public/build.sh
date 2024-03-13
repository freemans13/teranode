#!/bin/bash

# Check if kubectl is installed
if ! command -v kubectl &>/dev/null; then
  echo "kubectl could not be found, please install it to continue (brew install kubectl)."
  exit 1
fi

# Check if kustomize is installed
if ! command -v kustomize &>/dev/null; then
  echo "kustomize could not be found, please install it to continue (brew install kustomize)."
  exit 1
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

CONTEXT=$(kubectl config current-context)
NAMESPACE=$(kubectl config view --minify --output 'jsonpath={..namespace}')

kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground

kubectl describe namespace public >/dev/null
if [[ $? == 1 ]]; then
  kubectl create namespace public
fi

kubectl config set-context --current --namespace=public

kubectl delete secret tunnel-credentials
kubectl create secret generic tunnel-credentials --from-file=credentials.json=$DIR/cloudflare-credentials.json

kustomize build $DIR | kubectl apply -f -

kubectl config use-context $CONTEXT
kubectl config set-context --current --namespace=$NAMESPACE
