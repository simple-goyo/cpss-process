# -*- coding: utf-8 -*-
import requests
from bson.json_util import loads, dumps
import json


# 课题四接口服务
def get_all_resource_instance_id(name, x, y):
    data = {
        "refApp": {
            "name": name,
            "kind": "FusionApp",
            "namespace": "fusion-app-resources"
        },
        "userLabel": {
            "x": x,
            "y": y
        }
    }
    reponse = requests.post("http://221.228.66.83:32022/apis/app_instance", dumps(data))
    # output = loads(reponse.content)
    return reponse


def delete_five_app_instance_by_uid(uid):
    namespace_name = uid.split("/")
    data = {
        "refAppInstance": {
            "namespace": namespace_name[0],
            "name": namespace_name[1]
        }
    }
    data_json = json.dumps(data)
    reponse = requests.delete("http://221.228.66.83:32022/apis/app_instance", data=data_json)
    # output = loads(reponse.content)
    return reponse


def get_resource_by_id(resource_instance_resource):
    data = {
        "refResource": resource_instance_resource
    }
    data_json = json.dumps(data)
    reponse = requests.get("http://221.228.66.83:32022/apis/resource", data=data_json)
    output = reponse.json()
    return output


def bind_resource(app_instance_resource):
    data = {
        "refResource": app_instance_resource
    }
    data_json = json.dumps(data)
    reponse = requests.post("http://221.228.66.83:32022/apis/resource/bind", data=data_json)
    output = reponse.json()
    return output


def delete_bind_resource(app_instance_resource):
    data = {
        "refResource": app_instance_resource
    }
    data_json = json.dumps(data)
    reponse = requests.delete("http://221.228.66.83:32022/apis/resource/bind", data=data_json)
    output = reponse.json()
    return output


# 自己接口
def get_resource_instance_id_self(user_id, app_instance_id, resource_id):
    data = {
        "user_id": user_id,
        "app_instance_id": app_instance_id,
        "resource_id": resource_id
    }
    resource_instance_map = {
        "工人": "crowdsourcing-service",
        "CoffeeMaker": "coffeemaker-service",
        "音箱": "speaker-mi",
        "体重秤": "weight-lexin",
        "Keep应用": "tv",
        "空气盒子": "smartbox-haier",
        "小米空气净化器": "purifier-mi",
        "水壶": "kettle-mi",
        "星巴克": "starbuck-service",
        "小爱音箱": "speaker-mi",
        "Orders": "ordercoffee-service",
    }
    resource_instance = resource_instance_map.get(resource_id[0])
    return resource_instance


def get_resource_param():
    data = {
        "filePath": "/root/activiti/kg/hct_Ontology_latest.ttl",
        "userId": 18
    }
    # data_json = json.dumps(data)
    reponse = requests.post("http://www.cpss2019.fun:21910/KG201910/getWorkflowInstanceInitParams", data)
    output = reponse.json()
    return output
