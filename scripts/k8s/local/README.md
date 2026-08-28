# Local Kubernetes resources

Kubernetes resources the API needs in a local OrbStack cluster.

`cargo x setup api` applies this directory for you.

Apply them manually with:

```bash
kubectl --context orbstack apply -f scripts/k8s/local
```

See [DEVELOPMENT.md](../../../DEVELOPMENT.md) for the full local setup.

