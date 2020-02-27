# coding=utf-8

# 流程代理服务

import pymongo
from bson import ObjectId
from flask import Flask, request
import logging
import requests
import pika
import time

logging.basicConfig(level=logging.INFO, filename='logger.log',
                    format="%(levelname)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s")
app = Flask(__name__)
myclient = pymongo.MongoClient("mongodb://119.29.194.211:27017/")
db_hcp = myclient["db_hcp"]
db_hcp.authenticate("hcp", "Hcp!!1996", mechanism='SCRAM-SHA-1')
t_app_class = db_hcp["t_app_class"]
t_app_instance = db_hcp["t_app_instance"]

user_id = ''
workflow_instance_id = ''
workflow_proxy_type = ''
workflow_proxy_id = ''
executor_resource_id = ''  # 当前访问的资源和服务名
service_name = ''
service_input = ''
next_workflow_proxy_ids = ''  # 后续流程代理服务


# 接口 传入初始参数
@app.route('/init', methods=['POST'])
def init():
    global user_id
    global workflow_instance_id
    global workflow_proxy_type
    global workflow_proxy_id
    global executor_resource_id
    global service_name
    global service_input
    global next_workflow_proxy_ids
    user_id = request.values.get("user_id")
    workflow_instance_id = request.values.get("workflow_instance_id")
    workflow_proxy_type = request.values.get("workflow_proxy_type")
    workflow_proxy_id = request.values.get("workflow_proxy_id")
    executor_resource_id = request.values.get("executor_resource_id")
    service_name = request.values.get("service_name")
    service_input = request.values.get("service_input")
    next_workflow_proxy_ids = request.values.get("next_workflow_proxy")
    logging.info("parameters are initialized.")
    return "success"


# 接口 等待被调用 执行当前任务
# 若为第一个任务 则由流程转化服务调用 否则 由上一步任务的服务调用
@app.route('/run', methods=['POST'])
def executetask():
    # 判断是初始节点，还是事件任务，还是普通任务
    if workflow_proxy_type == "StartNoneEvent":
        logging.info("StartNoneEvent is run")
    elif workflow_proxy_type == "DefaultEvent":
        logging.info("DefaultEvent is run")
    else:
        logging.info("Task is run")
        # 获取执行对象
        executor_resource_instance_id = get_resource_instance_id(executor_resource_id)
        # 暂时使用：获取服务的英文名
        service_name_en = get_service_name_en()
        # 从输入对象中获取执行对象执行服务所需要的参数
        params = get_params()
        data = {'func_name': service_name_en, 'params': params}
        logging.info("workflow engine is calling resource service orderCoffee")
        # 访问对应资源的代理服务 传入参数
        url = 'http://' + executor_resource_instance_id + '-proxy.default:8888/'
        logging.info("request result: " + requests.post(url, data).text)
        # 等待结果返回
        receive_result(executor_resource_instance_id)
    # 访问后续流程代理服务
    if next_workflow_proxy_ids == 'null':
        return
    else:
        # 访问下一个流程代理服务
        logging.info("request next workflow proxy service.")
        action_ips = find_app_instance_action_ip_by_instance_id(workflow_instance_id).get("action_ip")
        # url = 'http://' + next_workflow_proxy_ids + '-proxy.default:8888/'
        next_workflow_proxy_id_list = next_workflow_proxy_ids.split(",")
        for next_workflow_proxy_id in next_workflow_proxy_id_list:
            if next_workflow_proxy_id == "":
                continue
            next_ip = action_ips.get(next_workflow_proxy_id)
            next_run_address = "http://" + next_ip + ":8888/run"
            # next_run_address = "http://localhost:5001/init"
            print(next_run_address)
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            params = {}
            response = requests.post(next_run_address, data=params, headers=headers)
            print("response Code: " + str(response.status_code))
            print("response content: " + response.text)
            logging.info("next_run_address:"+next_run_address+"---response:" + str(response))
    return "success"


def get_resource_instance_id(resource_id):
    app_instance_resources = find_app_instance_resource_by_instance_id().get("resource")
    # 获取执行者实例id
    resource_instance_id = app_instance_resources.get(resource_id)
    return resource_instance_id


def find_app_instance_action_ip_by_instance_id(instance_id):
    action_ip = t_app_instance.find_one({'_id': ObjectId(instance_id)}, {"action_ip": 1})
    return action_ip


def get_service_name_en():
    operation_map = {
        "点咖啡服务": "CoffeeConfigure/getcoffeelink",
        "making coffee": "makeCoffee",
        "咖啡制作完成": "get-coffee-status",
        "送咖啡": "simplecrowdsourcing/publish/publishtask",
        "端热水": "get-water",
        "获取体重数据": "get-weight",
        "开启空气净化": "start-purify",
        "获取当前空气状态": "get-air-condition",
        "播放锻炼视频": "play-video",
        "烧水": "boil-water",
        "播放语音通知": "speak",
        "视频播放完成": "stop-video",
        "准备订单": "get-order",
        "星巴克下单": "order-coffee",
    }
    service_name_en = operation_map.get(service_name)
    return service_name_en


def get_params():
    service_inputs = service_input.split('&')
    params = {}
    for input0 in service_inputs:
        if input0 is "":
            return params
        input_resource_id = input0.split('.')[0]
        input_resource_param_name = input0.split('.')[1]
        input_service_param_name = input0.split('.')[2]
        input_resource_instance_id = get_resource_instance_id(input_resource_id)
        # 根据对象id和参数名获取对象参数值
        input_param_value = get_input_param_value(input_resource_instance_id, input_resource_param_name)
        params[input_service_param_name] = input_param_value
    return params


# 获取资源实例属性信息
def get_input_param_value(input_resource_instance_id, input_resource_param_name):
    # todo 状态空间获取参数
    resource_param = find_resource_param_by_instance_id().get("resource_param")
    # 获取执行者实例id
    input_param_value = resource_param.get(input_resource_instance_id).get(input_resource_param_name)
    return input_param_value


def find_app_instance_resource_by_instance_id():
    app_instance_resource = t_app_instance.find_one({'_id': ObjectId(workflow_instance_id)}, {"resource": 1})
    return app_instance_resource


def find_resource_param_by_instance_id():
    resource_param = t_app_instance.find_one({'_id': ObjectId(workflow_instance_id)}, {"resource_param": 1})
    return resource_param


# 同步等待结果返回
def receive_result(resource_name):
    resource_name = resource_name.split('-service')[0]
    if resource_name == 'ordercoffee':
        exchange_name = 'cyber_' + resource_name + '_exchange'
    elif resource_name == 'coffeemaker':
        exchange_name = 'physical_' + resource_name + '_exchange'
    elif resource_name == 'crowdsourcing':
        exchange_name = 'social_' + resource_name + '_exchange'

    # 创建一个连接对象,对象中绑定了rabbitmq的IP
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.141.221.88'))
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq.default'))
    # 创建一个频道对象
    channel = connection.channel()
    # 创建临时队列，consumer关闭后，队列自动删除
    result = channel.queue_declare('', exclusive=True)
    # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建
    # durable = True 代表exchange持久化存储，False 非持久化存储
    channel.exchange_declare(exchange=exchange_name, durable=False, exchange_type='direct')
    # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去
    print(exchange_name, result.method.queue, workflow_instance_id)
    channel.queue_bind(exchange=exchange_name, queue=result.method.queue, routing_key=workflow_instance_id)

    # 定义一个回调函数来处理消息队列中的消息，这里是打印出来
    def callback(ch, method, properties, body):
        logging.info('[x] Recieved %r' % body)
        # 根据返回结果 填充参数？
        resource_instance_id = resource_name + time.strftime("%Y%m%d%H%M%S", time.localtime())
        insert_app_instance_resource(resource_name, resource_instance_id)
        insert_app_instance_resource_param(resource_instance_id, body)
        # self.body = body
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 收到消息后通知mq
        channel.close()  # !!!在执行引擎中使用 表示只接收一次即关闭监听 接着执行流程的下一步

    channel.basic_consume(result.method.queue,
                          callback,
                          # no_ack 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列
                          # True，无论调用callback成功与否，消息都被消费掉
                          False)

    logging.info('[*] Waiting for msg, routing_key: ' + workflow_instance_id)
    channel.start_consuming()


def insert_app_instance_resource(resource_id, resource_instance_id):
    myquery = {"_id": workflow_instance_id}
    newvalues = {"$set": {"resource." + resource_id: resource_instance_id}}
    t_app_instance.update_one(myquery, newvalues)


def insert_app_instance_resource_param(resource_instance_id, param):
    myquery = {"_id": workflow_instance_id}
    newvalues = {"$set": {"resource_param." + resource_instance_id: param}}
    t_app_instance.update_one(myquery, newvalues)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5001')
