

kubectl delete -f service/status-service-bootstrap.yaml
kubectl delete -f service/status-service-node.yaml
kubectl delete -f relay/status-backend-relay.yaml
kubectl delete -f light/status-backend-light.yaml

#kubectl --kubeconfig $KUBECONFIG delete -f ../base-manifests/anvil-statefulset.yaml
#kubectl --kubeconfig $KUBECONFIG delete -f ../base-manifests/snt-contract-deployment.yaml
#kubectl --kubeconfig $KUBECONFIG delete -f ../base-manifests/snt-communities-deployment.yaml

# kubectl --kubeconfig $KUBECONFIG delete -f ../controlbox/controlbox.yaml
