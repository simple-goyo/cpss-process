# coding=utf-8

from kubernetes import client, config

"""
Creates, updates, and deletes a deployment using AppsV1Api.
"""


def create_deployment_object(workflow_proxy_name, image_name):
    # Configurate Pod template container
    container = client.V1Container(
        name=image_name,
        image="lizhengjie/hcp-re:"+image_name,
        image_pull_policy="Always",
        ports=[client.V1ContainerPort(container_port=8888)])
    image_pull_secret = client.V1LocalObjectReference(name='regcred')
    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"k8s-app": workflow_proxy_name}),
        spec=client.V1PodSpec(image_pull_secrets=[image_pull_secret], containers=[container]))
    # Create the specification of deployment
    spec = client.V1DeploymentSpec(
        replicas=1,
        template=template,
        selector={'matchLabels': {'k8s-app': workflow_proxy_name}})
    # Instantiate the deployment object
    deployment = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=workflow_proxy_name),
        spec=spec)
    return deployment


def create_deployment(api_instance, deployment):
    # Create deployment
    api_response = api_instance.create_namespaced_deployment(
        body=deployment,
        namespace="default")
    print("Deployment created. status='%s'" % str(api_response.status))


def update_deployment(api_instance, deployment, image_, workflow_proxy_name):
    # image_: image with new version
    # Update container image
    deployment.spec.template.spec.containers[0].image = image_
    # Update the deployment
    api_response = api_instance.patch_namespaced_deployment(
        name=workflow_proxy_name,
        namespace="default",
        body=deployment)
    print("Deployment updated. status='%s'" % str(api_response.status))


def delete_deployment(api_instance, workflow_proxy_name):
    # Delete deployment
    api_response = api_instance.delete_namespaced_deployment(
        name=workflow_proxy_name,
        namespace="default",
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Deployment deleted. status='%s'" % str(api_response.status))


def create_service(service_name):
    core_v1_api = client.CoreV1Api()
    body = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(
            labels={"k8s-app": service_name},
            name=service_name
        ),
        spec=client.V1ServiceSpec(
            type="NodePort",
            selector={"k8s-app": service_name},
            ports=[client.V1ServicePort(
                port=8888,     # port in k8s node
                target_port=8888    # port in container
            )]
        )
    )
    # Creation of the Service in specified namespace
    api_response = core_v1_api.create_namespaced_service(namespace="default", body=body)
    print("Service created. status='%s'" % str(api_response.status))


def create_service_main(workflow_proxy_name):
    # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.

    config.load_kube_config()
    apps_v1 = client.AppsV1Api()
    # Create a deployment object with client-python API. The deployment we

    deployment = create_deployment_object(workflow_proxy_name=workflow_proxy_name, image_name='coffeemaker-service')
    create_deployment(apps_v1, deployment)
    # update_deployment(apps_v1, deployment)
    # delete_deployment(apps_v1, 'workflow1-task1')

    # Create a service object
    create_service(workflow_proxy_name)


if __name__ == '__main__':
    create_service_main()
