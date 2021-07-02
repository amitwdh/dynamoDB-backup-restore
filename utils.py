import os
import logging
from dateutil import parser
from dateutil.tz import UTC

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def scale_down_pod(dest_client, config, deployment):

    logging.info("Scaling down %s pod" %(deployment))
    cmd = 'kubectl scale --replicas=0 deployment/%s -n %s --kubeconfig %s' % (deployment, dest_client, config)
    execute_kubectl(cmd)

def scale_up_pod(dest_client, config, deployment):

    logging.info("Scaling up %s pod" %(deployment))
    cmd = 'kubectl scale --replicas=1 deployment/%s -n %s --kubeconfig %s' % (deployment, dest_client, config)
    execute_kubectl(cmd)

def get_orchestrator_deployments(dest_client, config):
    cmd = 'kubectl get deployments --no-headers -o custom-columns=":metadata.name" -n %s --kubeconfig %s | grep "orchestrator-deployment"' % (dest_client, config)
    orchestrator_deployments = execute_kubectl(cmd)
    return orchestrator_deployments.split('\n')


def get_pod(dest_client, config, deployment):
    cmd = 'kubectl get pods --no-headers -o custom-columns=":metadata.name" -n %s --kubeconfig %s | grep %s' % (dest_client, config, deployment)
    pod = execute_kubectl(cmd)
    return pod.replace('\n','')

def get_offline_query_deployments(dest_client, config):
    cmd = 'kubectl get deployments --no-headers -o custom-columns=":metadata.name" -n %s --kubeconfig %s | grep "offline-query-data-sync-deployment"' % (dest_client, config)
    offline_query_deployments = execute_kubectl(cmd)
    return offline_query_deployments.split('\n')

def delete_pvc(pvc, dest_client, config):
    cmd = 'kubectl delete pvc %s -n %s --kubeconfig %s' % (pvc, dest_client, config)
    print(cmd)
    execute_kubectl(cmd)

def execute_kubectl(command, exception_enabled=True):
    stdout = os.popen(command)
    return stdout.read()

def restart_pods(dest_client, orchestrator_offline_deployments, config):

    logging.info("Restarting pods")
    logging.info(orchestrator_offline_deployments)
    if len(orchestrator_offline_deployments) != 0:
        for deployment in orchestrator_offline_deployments:
            scale_down_pod(dest_client, config, deployment)
            scale_up_pod(dest_client, config, deployment)

def get_dynamodb_tables(client, env):

    offline_query_lro = "offline-query-lro-store-" + client + "-" + env
    adapter_lro_store = "adapter-lro-store-" + client + "-" + env
    tables = []
    tables.append(offline_query_lro)
    tables.append(adapter_lro_store)
    logging.info("Dynamo DB tables for %s = %s",client, tables)
    return tables

def get_lro_store_table_name(client, env):
    return "adapter-lro-store-" + client + "-" + env

def convert_datetime_to_utc_tz(date_time_object):
    return parser.parse(date_time_object).astimezone(UTC)