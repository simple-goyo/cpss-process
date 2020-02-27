# -*-coding:utf8-*-
import json
import time
import datetime
import pymongo
from bson.json_util import loads, dumps
from flask import Flask, request, jsonify, redirect, url_for
from flask_cors import CORS
from datetime import datetime, timedelta
from util.mongodb import *
from util.utctime import *
from service.app_service import *
from service.flow_self import *
from service.resource_service import *
from service.serviceDeployment_remote import *
from kubernetes import client, config
import requests

app = Flask(__name__)
CORS(app, supports_credentials=True)
myclient = pymongo.MongoClient("mongodb://119.29.194.211:27017/")
# dblist = myclient.list_database_names()
db_hcp = myclient["db_hcp"]
db_hcp.authenticate("hcp", "Hcp!!1996", mechanism='SCRAM-SHA-1')
t_app_class = db_hcp["t_app_class"]
t_app_instance = db_hcp["t_app_instance"]
t_app_show = db_hcp["t_app_show"]


# 保存app_class
@app.route('/save_app_class', methods={'POST', 'GET'})
def save_app_class():
    data = request.data.replace(b"$$", b"")
    app_class = json.loads(data)
    app_class_id = insert_app_class(t_app_class, app_class)
    data = {'app_class_id': str(app_class_id)}
    return jsonify(data)


# 读取所有app_class的简单介绍
@app.route('/get_all_app_class_introduction', methods={'POST', 'GET'})
def get_all_app_class_introduction():
    app_classes_introduction = find_all_app_class_introduction(t_app_class)
    app_classes_introduction = get_id_str(app_classes_introduction)
    data = {'app_classes_introduction': app_classes_introduction}
    return jsonify(data)


# 获取一个app_class的详细流程
@app.route('/get_app_class_by_id', methods={'POST', 'GET'})
def get_app_class_by_id():
    app_class_id = request.values.get("app_class_id")
    app_class = find_app_class_by_id(t_app_class, app_class_id)
    app_class = get_id_str(app_class)
    data = {'app_class': app_class}
    return jsonify(data)


# 读取所有app_class
@app.route('/get_all_app_class', methods={'POST', 'GET'})
def get_all_app_class():
    app_classes = find_all_app_class(t_app_class)
    app_classes = get_id_str(app_classes)
    data = {'app_classes': app_classes}
    return jsonify(data)


# 保存app_instance
@app.route('/save_app_instance', methods={'POST', 'GET'})
def save_app_instance():
    app_class_id = request.values.get("app_class_id")
    user_id = request.values.get("user_id")
    x = request.values.get("x")
    y = request.values.get("y")
    count = request.values.get("count")
    create_time = get_time()
    # create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # now_time = datetime.now()
    # utc_time = now_time + timedelta(hours=8)  # UTC只是比北京时间提前了8个小时
    # create_time = utc_time.strftime("%Y-%m-%d %H:%M:%S")  # 转换成Aliyun要求的传参格式...
    app_instance = {"app_class_id": ObjectId(app_class_id), "user_id": user_id, "create_time": create_time,
                    "action_state": {}, "app_instance_state": "0", "uid": "", "type": "project"}
    # 创建应用实例
    app_instance_id = insert_app_instance(t_app_instance, app_instance)
    # 获取执行资源
    executor_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "activityelement")
    # 获取输入资源
    input_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "input")
    # 获取输出资源
    output_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "output")
    # 所有需要的资源,可能需要应用初始时实例化,也有可能是在应用执行过程中产生绑定实例
    all_need_resource_ids = executor_resource_ids.union(input_resource_ids)
    # 初始资源实例化--暂时不用--仅存resource_id
    all_need_four_resource_id = []
    for resource_id in all_need_resource_ids:
        if resource_id in output_resource_ids:
            # 对于执行才产生的实例暂时先不绑定
            continue
        else:
            # 对于非执行产生的实例,在初始时进行实例化绑定
            all_need_four_resource_id.append(resource_id)
            # resource_instance_id = get_resource_instance_id(user_id, str(app_instance_id), resource_id)
            # insert_app_instance_resource(t_app_instance, app_instance_id, resource_id[1], resource_instance_id)
    # 初始资源实例化
    # 先获取应用类名
    app_class = find_app_class_by_instance_id(t_app_instance, app_instance_id)
    # 请求课题四
    reponse = get_all_resource_instance_id(
        app_class.get("app_class").get("properties").get("process_id"), x, y)
    # 如果output为空，状态码为201就更换流程
    status_code = reponse.status_code
    output = reponse.json()
    if status_code == 200:
        print("has resource")
    elif status_code == 201:
        if count == "1":
            print("第二次获取到室外喝咖啡的实例资源")
        else:
            return redirect(
                url_for('save_app_instance', app_class_id="5dda2e1ad90231244a5ac0ca", user_id=user_id, x=x, y=y,
                        count=1))
    else:
        return "no resource error"
    # 更新uid
    namespace = output.get("metadata").get("namespace")
    name = output.get("metadata").get("name")
    uid = namespace + "/" + name
    update_app_instance_uid(t_app_instance, app_instance_id, uid)
    # 保存资源id
    all_four_resource_id = output.get("spec").get("refResource")
    name_map = {
        "工人": "human",
        "咖啡机": "coffee-maker",
        "音箱": "speaker-mi",
        "体重秤": "weight-lexin",
        "Keep应用": "tv",
        "空气盒子": "smartbox-haier",
        "小米空气净化器": "purifier-mi",
        "水壶": "kettle-mi",
        "星巴克": "starbuck-service",
        "小爱音箱": "speaker-mi",
    }
    for need_four_resource_id in all_need_four_resource_id:
        need_name = need_four_resource_id[0]
        four_name = name_map.get(need_name)
        for four_resource_id in all_four_resource_id:
            if four_name in four_resource_id.get("name"):
                insert_app_instance_resource(t_app_instance, app_instance_id, need_four_resource_id[1],
                                             four_resource_id)
    app_instance_resource = find_app_instance_resource_by_instance_id(t_app_instance, app_instance_id)
    app_instance_resource = get_id_str(app_instance_resource)
    process_version = app_class.get("app_class").get("properties").get("process_version")
    data = {
        'app_instance_resource': app_instance_resource,
        'process_version': process_version
    }
    return jsonify(data)


# 保存app_instance
@app.route('/save_app_instance_self', methods={'POST', 'GET'})
def save_app_instance_self():
    app_class_id = request.values.get("app_class_id")
    user_id = request.values.get("user_id")
    create_time = get_time()
    app_instance = {"app_class_id": ObjectId(app_class_id), "user_id": user_id, "create_time": create_time,
                    "action_state": {}, "app_instance_state": "0", "uid": "", "type": "self"}
    # 创建应用实例
    app_instance_id = insert_app_instance(t_app_instance, app_instance)
    # 获取执行资源
    executor_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "activityelement")
    # 获取输入资源
    input_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "input")
    # 获取输出资源
    output_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "output")
    # 所有需要的资源,可能需要应用初始时实例化,也有可能是在应用执行过程中产生绑定实例
    all_need_resource_ids = executor_resource_ids.union(input_resource_ids)
    # 初始资源实例化--暂时不用--仅存resource_id
    all_need_four_resource_id = []
    for resource_id in all_need_resource_ids:
        if resource_id in output_resource_ids:
            # 对于执行才产生的实例暂时先不绑定
            continue
        else:
            # 对于非执行产生的实例,在初始时进行实例化绑定
            resource_instance_id = get_resource_instance_id_self(user_id, str(app_instance_id), resource_id)
            # 去获取资源知识图谱接口获取对象信息
            insert_app_instance_resource(t_app_instance, app_instance_id, resource_id[1], resource_instance_id)
            body = get_resource_param()
            insert_app_instance_resource_param(t_app_instance, app_instance_id, resource_instance_id, body)
    # 调用执行引擎
    mongodb = {
        't_app_class': t_app_class,
        't_app_instance': t_app_instance,
    }
    cf = CommonFlow(app_instance_id, **mongodb)
    cf.run_flow()
    return "success"


# 保存app_instance
@app.route('/delete_app_instance_service_self', methods={'POST', 'GET'})
def delete_app_instance_service_self():
    app_instances = find_all_app_instance(t_app_instance)
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

    for app_instance in app_instances:
        app_instance_id = str(app_instance["_id"])
        deployment_and_service_name0 = "s" + str(hash(str(app_instance_id + "sid-3F7627C8-62BF-4B04-B41B-14693EEE69EB"))) + "s"
        deployment_and_service_name1 = "s" + str(hash(str(app_instance_id + "sid-9BEDCB3D-5BBA-4FEA-BA19-A611331C220A"))) + "s"
        deployment_and_service_name2 = "s" + str(hash(str(app_instance_id + "sid-8B3E7258-1ABF-41FC-8DC2-D89E225910E2") ))+ "s"
        print("deployment_and_service_name0" + deployment_and_service_name0)
        delete_service(client.CoreV1Api(), deployment_and_service_name0)
        delete_deployment(client.CoreV1Api(), deployment_and_service_name0)
        delete_service(client.CoreV1Api(), deployment_and_service_name1)
        delete_deployment(client.CoreV1Api(), deployment_and_service_name1)
        delete_service(client.CoreV1Api(), deployment_and_service_name2)
        delete_deployment(client.CoreV1Api().deployment_and_service_name2)
    return "success"


# 执行app_instance
@app.route('/invoke_app_instance', methods={'POST', 'GET'})
def invoke_app_instance():
    app_instance_id = request.values.get("app_instance_id")
    update_app_instance_state(t_app_instance, app_instance_id, "1")
    # 调用执行引擎
    mongodb = {
        't_app_class': t_app_class,
        't_app_instance': t_app_instance,
    }
    cf = CommonFlow(app_instance_id, **mongodb)
    cf.run_flow()
    return app_instance_id


# 语音传参调用星巴克
@app.route('/voice_invoke_instance', methods={'POST', 'GET'})
def voice_invoke_instance():
    app_class_id = request.values.get("app_class_id")
    user_id = request.values.get("user_id")
    x = request.values.get("x")
    y = request.values.get("y")
    count = request.values.get("count")
    create_time = get_time()
    # create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    app_instance = {"app_class_id": ObjectId(app_class_id), "user_id": user_id, "create_time": create_time,
                    "action_state": {}, "app_instance_state": "0", "uid": ""}
    # 创建应用实例
    app_instance_id = insert_app_instance(t_app_instance, app_instance)
    # 获取执行资源
    executor_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "activityelement")
    # 获取输入资源
    input_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "input")
    # 获取输出资源
    output_resource_ids = get_resource_ids_by_app_class_id(t_app_class, app_class_id, "output")
    # 所有需要的资源,可能需要应用初始时实例化,也有可能是在应用执行过程中产生绑定实例
    all_need_resource_ids = executor_resource_ids.union(input_resource_ids)
    # 初始资源实例化--暂时不用--仅存resource_id
    all_need_four_resource_id = []
    for resource_id in all_need_resource_ids:
        if resource_id in output_resource_ids:
            # 对于执行才产生的实例暂时先不绑定
            continue
        else:
            # 对于非执行产生的实例,在初始时进行实例化绑定
            all_need_four_resource_id.append(resource_id)
            # resource_instance_id = get_resource_instance_id(user_id, str(app_instance_id), resource_id)
            # insert_app_instance_resource(t_app_instance, app_instance_id, resource_id[1], resource_instance_id)
    # 初始资源实例化
    # 先获取应用类名
    app_class = find_app_class_by_instance_id(t_app_instance, app_instance_id)
    # 请求课题四
    reponse = get_all_resource_instance_id(
        app_class.get("app_class").get("properties").get("process_id"), x, y)
    # 如果output为空，状态码为201就更换流程
    status_code = reponse.status_code
    output = reponse.json()
    if status_code == 200:
        print("has resource")
    elif status_code == 201:
        if count == "1":
            return "no resource error"
        else:
            return redirect(
                url_for('save_app_instance', app_class_id="5dda2e1ad90231244a5ac0ca", user_id=user_id, x=x, y=y,
                        count=1))
    else:
        return "no resource error"
    # 更新uid
    namespace = output.get("metadata").get("namespace")
    name = output.get("metadata").get("name")
    uid = namespace + "/" + name
    update_app_instance_uid(t_app_instance, app_instance_id, uid)
    # 保存资源id
    all_four_resource_id = output.get("spec").get("refResource")
    name_map = {
        "工人": "human",
        "咖啡机": "coffee-maker",
        "音箱": "speaker-mi",
        "体重秤": "weight-lexin",
        "Keep应用": "tv",
        "空气盒子": "smartbox-haier",
        "小米空气净化器": "purifier-mi",
        "水壶": "kettle-mi",
        "星巴克": "starbuck-service",
        "小爱音箱": "speaker-mi",
    }
    for need_four_resource_id in all_need_four_resource_id:
        need_name = need_four_resource_id[0]
        four_name = name_map.get(need_name)
        for four_resource_id in all_four_resource_id:
            if four_name in four_resource_id.get("name"):
                insert_app_instance_resource(t_app_instance, app_instance_id, need_four_resource_id[1],
                                             four_resource_id)
    update_app_instance_state(t_app_instance, app_instance_id, "1")
    # 调用执行引擎
    mongodb = {
        't_app_class': t_app_class,
        't_app_instance': t_app_instance,
    }
    cf = CommonFlow(app_instance_id, **mongodb)
    cf.run_flow()
    return "success"


# 测试
# @app.route('/test_save_app_instance', methods={'POST', 'GET'})
# def test_save_app_instance():
#     app_instance_id = "5dca774eefb59bfd7f4ad03e"
#     # 调用执行引擎
#     mongodb = {
#         't_app_class': t_app_class,
#         't_app_instance': t_app_instance,
#     }
#     cf = CommonFlow(app_instance_id, **mongodb)
#     cf.run_flow()
#     return str(app_instance_id)


# 删除app_instance
@app.route('/delete_app_instance', methods={'POST', 'GET'})
def delete_app_instance():
    app_instance_id = request.values.get("app_instance_id")
    app_instance = find_app_instance_by_id(t_app_instance, app_instance_id)
    if app_instance is None:
        return "no app instance"
    uid = app_instance.get("uid")
    # 删除应用实例
    delete_app_instance_by_id(t_app_instance, app_instance_id)
    # 删除课题五应用实例
    delete_five_app_instance_by_uid(uid)
    return "success"


# 修改app_instance


# 查询app_instance
@app.route('/get_all_app_instance_introduction', methods={'POST', 'GET'})
def get_all_app_instance_introduction():
    app_instance_introduction = find_all_app_instance_introduction(t_app_instance)
    app_instance_introduction = get_id_str(app_instance_introduction)
    data = {'app_instance_introduction': app_instance_introduction}
    return jsonify(data)


@app.route('/get_app_class_by_instance_id', methods={'POST', 'GET'})
def get_app_class_by_instance_id():
    app_instance_id = request.values.get("app_instance_id")
    if app_instance_id == "":
        return "null"
    app_class = find_app_class_by_instance_id(t_app_instance, app_instance_id)
    app_class = get_id_str(app_class)
    data = {'app_class': app_class}
    return jsonify(data)


@app.route('/get_app_instance_action_state_by_instance_id', methods={'POST', 'GET'})
def get_app_instance_action_state_by_instance_id():
    app_instance_id = request.values.get("app_instance_id")
    app_instance_action_state = find_app_instance_action_state_by_instance_id(t_app_instance, app_instance_id)
    app_instance_action_state = get_id_str(app_instance_action_state)
    data = {'app_instance_action_state': app_instance_action_state}
    return jsonify(data)


@app.route('/get_app_instance_action_state_and_resource_by_instance_id', methods={'POST', 'GET'})
def get_app_instance_action_state_and_resource_by_instance_id():
    app_instance_id = request.values.get("app_instance_id")
    # 获取action和对应state
    app_instance_action_state = find_app_instance_action_state_by_instance_id(t_app_instance, app_instance_id)
    # app_instance_action_state = get_id_str(app_instance_action_state)
    # data = {'app_instance_action_state': app_instance_action_state}
    # 获取action 和对应执行者资源id
    app_class = find_app_class_by_instance_id(t_app_instance, app_instance_id)
    # 获取执行者资源id和对应资源实例id
    app_instance_resource = find_app_instance_resource_by_instance_id(t_app_instance, app_instance_id)
    # 找到所有有执行者的action，拼接action_id ,state,resource_id,resource_instance_id
    data = []
    for child_shape in app_class.get("app_class").get("childShapes"):
        activity_element = child_shape.get("properties").get("activityelement")
        action_name = child_shape.get("properties").get("name")
        if activity_element is not None:
            action_id = child_shape.get("resourceId")
            resource_id = activity_element.get("id")
            state = app_instance_action_state.get("action_state").get(action_id)
            if state is None:
                state = "0"
            resource_instance_id = app_instance_resource.get("resource").get(resource_id)
            if resource_instance_id is None:
                state = ""
            data.append({
                "action_id": action_id,
                "action_name": action_name,
                "state": state,
                "resource_id": resource_id,
                "resource_instance_id": resource_instance_id,
            })
    return jsonify(data)


@app.route('/get_app_instance_action_state_and_resource_by_uid', methods={'POST', 'GET'})
def get_app_instance_action_state_and_resource_by_uid():
    uid = request.values.get("uid")
    # 获取action和对应state
    app_instance = find_app_instance_by_uid(t_app_instance, uid)
    app_instance_id = str(app_instance.get("_id"))
    app_instance_action_state = find_app_instance_action_state_by_instance_id(t_app_instance, app_instance_id)
    # app_instance_action_state = get_id_str(app_instance_action_state)
    # data = {'app_instance_action_state': app_instance_action_state}
    # 获取action 和对应执行者资源id
    app_class = find_app_class_by_instance_id(t_app_instance, app_instance_id)
    # 获取执行者资源id和对应资源实例id
    app_instance_resource = find_app_instance_resource_by_instance_id(t_app_instance, app_instance_id)
    # 找到所有有执行者的action，拼接action_id ,state,resource_id,resource_instance_id
    data = []
    for child_shape in app_class.get("app_class").get("childShapes"):
        activity_element = child_shape.get("properties").get("activityelement")
        action_name = child_shape.get("properties").get("name")
        if activity_element is not None:
            action_id = child_shape.get("resourceId")
            resource_id = activity_element.get("id")
            state = app_instance_action_state.get("action_state").get(action_id)
            if state is None:
                state = "0"
            resource_instance_id = app_instance_resource.get("resource").get(resource_id)
            if resource_instance_id is None:
                resource_instance_id = ""
            ast = app_instance_action_state.get("action_state_time")
            if ast is None:
                action_state_time = ""
            else:
                ai = ast.get(action_id)
                if ai is None:
                    action_state_time = ""
                else:
                    action_state_time = ai.get(state)
                    if action_state_time is None:
                        action_state_time = ""
            data.append({
                "action_id": action_id,
                "action_name": action_name,
                "state": state,
                "resource_id": resource_id,
                "resource_instance_id": resource_instance_id,
                "action_state_time": action_state_time
            })
    return jsonify(data)


"""
t_app_show
"""


@app.route('/get_app_show', methods={'POST', 'GET'})
def get_app_show():
    user_id = request.values.get("user_id")
    app_show = find_app_show(t_app_show, user_id)
    app_show = get_id_str(app_show)
    data = {'app_show': app_show}
    return jsonify(data)


@app.route('/update_app_show_instance_id', methods={'POST', 'GET'})
def update_app_show_instance_id():
    user_id = request.values.get("user_id")
    instance_id = request.values.get("instance_id")
    update_t_app_show_instance_id(t_app_show, user_id, instance_id)
    return "success"


@app.route('/update_app_show_uid', methods={'POST', 'GET'})
def update_app_show_uid():
    uid = request.values.get("uid")
    app_instance = find_app_instance_by_uid(t_app_instance, uid)
    app_instance_id = str(app_instance.get("_id"))
    update_t_app_show_instance_id(t_app_show, "1", app_instance_id)
    return "success"


@app.route('/get_resource_info', methods={'POST', 'GET'})
def get_resource_info():
    data = request.get_data()
    app_instance_resource = json.loads(data)
    data = get_resource_by_id(app_instance_resource)
    return jsonify(data)


@app.route('/show_app_instance')
def index():
    # app_instance_resource = {
    #     "uid": "bed4c157-79fb-4016-b50c-69f18122b619",
    #     "namespace": "fusion-app-resources",
    #     "kind": "Edge",
    #     "name": "speaker-mi"
    # }
    # data = delete_bind_resource(app_instance_resource)
    r = requests.get("http://39.104.154.79:6000/tts/一段播送消息", verify=False)
    return "success"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5001')
    # app.run(host='0.0.0.0', port='5001', ssl_context=(
    #     '/home/figo/shenbiao/www.cpss2019.fun.cer', '/home/figo/shenbiao/www.cpss2019.fun.key'))
