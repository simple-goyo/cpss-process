# coding=utf-8

from kubernetes import client, config


def create_deployment_object(deployment_name, image_name):
    # Configurate Pod template container
    container = client.V1Container(
        name=image_name,
        image="lizhengjie/hcp-re:" + image_name,
        image_pull_policy="Always",
        ports=[client.V1ContainerPort(container_port=8888)])
    image_pull_secret = client.V1LocalObjectReference(name='regcred')
    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"k8s-app": deployment_name}),
        spec=client.V1PodSpec(image_pull_secrets=[image_pull_secret], containers=[container]))
    # Create the specification of deployment
    spec = client.V1DeploymentSpec(
        replicas=1,
        template=template,
        selector={'matchLabels': {'k8s-app': deployment_name}})
    # Instantiate the deployment object
    deployment = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=deployment_name),
        spec=spec)
    return deployment


def create_deployment(api_instance, deployment):
    # Create deployment
    api_response = api_instance.create_namespaced_deployment(
        body=deployment,
        namespace="default")
    print("Deployment created. status='%s'" % str(api_response.status))


def delete_deployment(api_instance, deployment_name):
    # Delete deployment
    api_response = api_instance.delete_namespaced_deployment(
        name=deployment_name,
        namespace='default',
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Deployment deleted. status='%s'" % str(api_response.status))


def create_service(api_instance, service_name):
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
                port=8888,  # port in k8s node
                target_port=8888  # port in container
            )]
        )
    )
    # Creation of the Service in specified namespace
    api_response = api_instance.create_namespaced_service(namespace="default", body=body)
    print("Service created. status='%s'" % str(api_response.status))
    # print("Service cluster_ip is '%s'" % str(api_response.spec.cluster_ip))
    # return cluster-ip
    return api_response.spec.cluster_ip


def read_service_cluster_ip(api_instance, service_name):
    api_response = api_instance.read_namespaced_service(name=service_name, namespace='default')
    # print(api_response)
    # print("Service cluster_ip is '%s'" % str(api_response.spec.cluster_ip))
    return api_response.spec.cluster_ip


def delete_service(api_instance, service_name):
    # Delete service
    api_response = api_instance.delete_namespaced_service(
        name=service_name,
        namespace='default',
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Service deleted. status='%s'" % str(api_response.status))


def create_service_main(deployment_name, service_name):
    # Define the barer token we are going to use to authenticate.
    # See here to create the token:
    # https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/
    with open('token.txt', 'r') as file:
        Token = file.read().strip('\n')

    APISERVER = 'https://139.196.228.210:6443'

    # Create a configuration object
    configuration = client.Configuration()

    # Specify the endpoint of your Kube cluster
    configuration.host = APISERVER

    # Security part.
    # In this simple example we are not going to verify the SSL certificate of
    # the remote cluster (for simplicity reason)
    configuration.verify_ssl = False

    # Nevertheless if you want to do it you can with these 2 parameters
    # configuration.verify_ssl=True
    # ssl_ca_cert is the filepath to the file that contains the certificate.
    # configuration.ssl_ca_cert="certificate"
    configuration.api_key = {"authorization": "Bearer " + Token}

    # configuration.api_key["authorization"] = "bearer " + Token
    # configuration.api_key_prefix['authorization'] = 'Bearer'
    # configuration.ssl_ca_cert = 'ca.crt'
    # Create a ApiClient with our config
    client.Configuration.set_default(configuration)

    # Create a deployment object
    deployment = create_deployment_object(deployment_name=deployment_name, image_name='workflow-proxy-template')
    create_deployment(client.AppsV1Api(), deployment)

    # Create a service object, return the service's cluster-ip
    clusterIP = create_service(client.CoreV1Api(), service_name)
    print("Service cluster_ip is: " + str(clusterIP))
    # Or read cluster-ip when needed
    clusterIP = read_service_cluster_ip(client.CoreV1Api(), service_name)
    print("Service cluster_ip is: " + str(clusterIP))
    # access the service on kubernetes nodes via cluster-ip : http://clusterIP:8888/

    # Delete created service and deployment
    delete_service(client.CoreV1Api(), service_name)
    delete_deployment(client.AppsV1Api(), deployment_name)
    return clusterIP


if __name__ == '__main__':
    create_service_main(deployment_name="workflow1-task1", service_name="workflow1-task1")
