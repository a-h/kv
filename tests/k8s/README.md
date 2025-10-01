# KV Benchmark Kubernetes Deployment

## Tasks

### apply

```bash
kubectl apply -f deployment.yaml
```

### pods

```bash
kubectl -n kvtest get pods
```

### logs

```bash
kubectl logs -n kvtest job/kv-benchmark-job
```

### exec

Interactive: true

```bash
kubectl exec -it -n kvtest deployment/kv-benchmark -- /bin/bash
```

### delete

```bash
kubectl delete -f deployment.yaml
```

### docker-test-bind-mount

Show that the same issue occurs with Docker bind mounts.

```bash
mkdir -p "$PWD/data"

docker run --rm \
  -v "$PWD/data:/data" \
  ghcr.io/a-h/kv:latest \
  /bin/sh -c "
    kv --type=sqlite --connection='file:/data/benchmark.db?mode=rwc' init &&
    kv --type=sqlite --connection='file:/data/benchmark.db?mode=rwc' benchmark mixed 10000 15
  "

rm -rf "$PWD/data"
```

### docker-test-internal

```bash
docker volume create kvtest
docker run --rm -v kvtest:/data busybox chown 1000:1000 /data
docker run --rm -v kvtest:/data ghcr.io/a-h/kv:latest /bin/sh -c "
    kv --type=sqlite --connection='file:/data/benchmark.db?mode=rwc' init &&
    kv --type=sqlite --connection='file:/data/benchmark.db?mode=rwc' benchmark mixed 10000 15
  "
```