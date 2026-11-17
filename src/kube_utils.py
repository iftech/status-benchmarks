# Python Imports
import kubernetes
import logging
import os
from pathlib import Path
from kubernetes.client import ApiException
from typing import List

# Project Imports

logger = logging.getLogger(__name__)


def setup_kubernetes_client():
    logger.info("Setting up Kubernetes client")

    # SSH/IDE launches can lose the pod's environment while keeping its mounted credentials.
    service_account = Path("/var/run/secrets/kubernetes.io/serviceaccount")
    if (service_account / "token").is_file() and (service_account / "ca.crt").is_file():
        if not os.environ.get("KUBERNETES_SERVICE_HOST"):
            os.environ["KUBERNETES_SERVICE_HOST"] = "kubernetes.default.svc"
        if not os.environ.get("KUBERNETES_SERVICE_PORT"):
            os.environ["KUBERNETES_SERVICE_PORT"] = "443"

    try:
        kubernetes.config.load_incluster_config()
        logger.debug("Loaded in-cluster Kubernetes configuration")
    except kubernetes.config.config_exception.ConfigException as incluster_error:
        try:
            kubernetes.config.load_kube_config()
            logger.info("Loaded kubeconfig configuration")
        except kubernetes.config.config_exception.ConfigException as kubeconfig_error:
            logger.error("Could not configure Kubernetes client: in-cluster=%s; kubeconfig=%s",
                         incluster_error, kubeconfig_error)
            raise RuntimeError(
                f"Failed to configure Kubernetes client. In-cluster: {incluster_error}; "
                f"kubeconfig: {kubeconfig_error}"
            ) from kubeconfig_error


def get_pods(name: str, namespace: str) -> List[str]:
    pods = []

    try:
        statefulset = kubernetes.client.AppsV1Api().read_namespaced_stateful_set(
            name=name,
            namespace=namespace
        )
        replicas = statefulset.status.replicas
        for replica in range(replicas):
            pods.append(f"{name}-{replica}.{name}.{namespace}")
        logger.info(f"Found {replicas} pods with name {name} in namespace {namespace}")
    except ApiException as e:
        logger.error(f"Failed to get pods: {e}")
        raise

    return pods
