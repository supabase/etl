# Local Kubernetes resources

These manifests create the pre-defined Kubernetes resources that `etl-api`
expects in a local OrbStack cluster before it deploys pipeline replicators.

Apply them with:

```bash
kubectl --context orbstack apply -f scripts/k8s/local
```

