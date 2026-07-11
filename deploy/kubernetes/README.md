# Malachi on Kubernetes

A worked example of a **3-node CP cluster** with **rack (zone) aware placement**, in one manifest
([`malachi.yaml`](malachi.yaml)). It ties together the log control plane over `ra`, libcluster node
discovery, and the failure-domain placement guarantee.

## Why these choices

Malachi's control plane is **Raft** (`ra`), which needs **stable node identities**. That drives the shape:

- **StatefulSet**, not Deployment — each pod gets a stable name (`malachi-0/1/2`), stable DNS, and a stable
  `PersistentVolume`, so a restarted pod rejoins its Raft groups as the *same* member.
- **Headless Service** (`malachi-headless`, `clusterIP: None`) — publishes per-pod DNS
  (`malachi-0.malachi-headless.<ns>.svc.cluster.local`) for Erlang distribution and the `ra` peer set.
  `publishNotReadyAddresses: true` lets peers resolve each other *during* cluster formation, before Ready.
- **Static peer set** (`MALACHIMQ_LOG_NODES` = the three pod DNS names) — idiomatic for a fixed-size CP
  cluster (this is how RabbitMQ and friends do it), and the node names match `RELEASE_NODE` exactly.
- **libcluster, `epmd` strategy** — connectivity only: it keeps the (already-known) peers connected over
  Erlang distribution. It reuses `MALACHIMQ_LOG_NODES`, so no extra config and no Kubernetes-API RBAC.
  (The `kubernetes` strategy is available for dynamic, autoscaling deployments — see the note below.)
- **`podManagementPolicy: Parallel`** — the three pods start together so the Raft cluster forms with a
  quorum instead of waiting pod-by-pod.
- **PodDisruptionBudget `minAvailable: 2`** — keeps a Raft majority through drains and rollouts.

### Rack-aware placement

Segment replicas should span distinct failure domains (zones). Two pieces make that real:

1. **`topologySpreadConstraints`** on `topology.kubernetes.io/zone` spread the pods across zones.
2. An **init container** reads each pod's node zone label (via a least-privilege `get nodes` ClusterRole)
   and the main container folds it into `MALACHIMQ_LOG_ATTRIBUTES=zone=<zone>`. With
   `MALACHIMQ_LOG_SPREAD_BY=zone`, placement then spreads replicas across zones, and
   `MALACHIMQ_LOG_MIN_DOMAINS=2` requires each segment to span two zones. The policy is `soft` (best-effort,
   violations are surfaced as the `malachi_domain_violations` metric); set it to `hard` to *reject* a
   produce that cannot span two zones.

## Deploy

1. Build and push the image, and point the StatefulSet `image:` at it (the default is a placeholder).
2. Set your namespace in the `ClusterRoleBinding` `subjects[0].namespace`.
3. Replace the `Secret` values (or create it out-of-band):

   ```sh
   # All four seed users need a non-default password, or the prod config validator refuses to boot.
   kubectl create secret generic malachi-secrets \
     --from-literal=release-cookie="$(openssl rand -hex 32)" \
     --from-literal=admin-pass="$(openssl rand -hex 16)" \
     --from-literal=producer-pass="$(openssl rand -hex 16)" \
     --from-literal=consumer-pass="$(openssl rand -hex 16)" \
     --from-literal=app-pass="$(openssl rand -hex 16)"
   ```

4. Apply:

   ```sh
   kubectl apply -f malachi.yaml
   kubectl rollout status statefulset/malachi
   ```

## Verify

```sh
# readiness (each pod exposes /health and /ready on 4041)
kubectl get pods -l app=malachi

# the cluster's metrics, including the failure-domain gauge
kubectl port-forward svc/malachi 4041:4041
curl -H 'Accept: text/plain' -u app:<app-pass> localhost:4041/metrics | grep malachi_domain_violations

# drive the log protocol from the reference client against the client Service
kubectl port-forward svc/malachi 4040:4040
MALACHI_PORT=4040 node ../../scripts/producer.js orders 100 --create
```

If nodes are unlabelled (no `topology.kubernetes.io/zone`), the zone is empty, placement falls back to
no-spread, and `malachi_domain_violations` reports the segments that cannot meet `min_domains` — the
degradation is surfaced, not hidden.

## Dynamic discovery (alternative)

For an autoscaling, non-fixed deployment you can swap the `epmd` strategy for Kubernetes-API discovery:

```yaml
- name: MALACHIMQ_CLUSTER_STRATEGY
  value: kubernetes
- name: MALACHIMQ_CLUSTER_KUBERNETES_SELECTOR
  value: app=malachi
- name: MALACHIMQ_CLUSTER_KUBERNETES_NODE_BASENAME
  value: malachi
```

This needs a `get/list/watch` on `pods` in the RBAC, and the discovered node names must match
`RELEASE_NODE` (with a StatefulSet + headless service and `mode: hostname` they do). The `ra` peer set
still comes from `MALACHIMQ_LOG_NODES`, since Raft membership is explicit — dynamic *membership* changes
ride on the rebalancing coordinator, not on discovery.
