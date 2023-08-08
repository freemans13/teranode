# Jaeger

For the full documentation, see https://www.jaegertracing.io/docs/1.44/operator/




## Commands to install Jaeger

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.6.3/cert-manager.yaml

kubectl create namespace observability

kubectl create -f https://github.com/jaegertracing/jaeger-operator/releases/download/v1.44.0/jaeger-operator.yaml -n observability

kubectl get deployment jaeger-operator -n observability
```


## Development deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger-operator
spec:
  template:
    spec:
      containers:
      - name: jaeger-operator
        image: jaegertracing/jaeger-operator:master
        args: ["start"]
        env:
        - name: LOG-LEVEL
          value: debug
```

## Production deployment

```yaml
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: simple-prod
spec:
  strategy: production
  collector:
    maxReplicas: 5
    resources:
      limits:
        cpu: 100m
        memory: 128Mi
```
