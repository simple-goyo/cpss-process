# -*- coding: utf-8 -*-
'''
@Time    :   2019/11/01 20:47:50
@Author  :   Tianyi Wu 
@Contact :   wutianyi@hotmail.com
@File    :   coffee_ flow.py
@Version :   1.0
@Desc    :   None
'''

# here put the import lib
# from prefect import Task, Flow
from bson.json_util import loads
from threading import Thread
from service.app_service import *
from service.serviceDeployment_remote import *
from util.utctime import *
import requests
import time
import pika
import logging


class Scp_StartEvent_Task():
    def __init__(self, name, **config):
        # super().__init__(name=name)
        self.action_desc = ''
        self.instance_id = config.get('instance_id')
        self.t_app_class = config.get('t_app_class')
        self.t_app_instance = config.get('t_app_instance')
        self.task_name = config.get('task_name')
        self.task_id = config.get('task_id')
        self.task_type = config.get('task_type')

    def run(self, **input):
        # update_stop_task(self)
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "2")
        create_time = get_time()
        update_app_instance_action_time(self.t_app_instance, self.instance_id, self.task_id, "2", create_time)
        # pub_to_MQ({'msg': 'flow start', 'flow id': ''})


class Scp_EndEvent_Task():
    def run(self):
        pub_to_MQ({'msg': 'flow end', 'flow id': ''})


class Scp_Task():
    def __init__(self, name, **config):
        # super().__init__(name=name)
        self.body = ''
        self.action_desc = ''
        self.instance_id = config.get('instance_id')
        self.t_app_class = config.get('t_app_class')
        self.t_app_instance = config.get('t_app_instance')
        self.task_name = config.get('task_name')
        self.task_type = config.get('task_type')
        self.task_id = config.get('task_id')
        self.task_executor = config.get('task_executor')
        self.task_input = config.get('task_input')


def get_task_url(self):
    app_instance_resources = find_app_instance_resource_by_instance_id(
        self.t_app_instance, self.instance_id).get("resource")
    # 获取执行者实例id
    app_instance_resource = app_instance_resources.get(self.task_executor)
    operation_map = {
        "点咖啡服务": "CoffeeConfigure/getcoffeelink",
        "制作咖啡": "makeCoffee",
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
    need_operation = operation_map.get(self.task_name)
    return [app_instance_resource, need_operation]


logging.basicConfig(level=logging.INFO, filename='logger.log',
                    format="%(levelname)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s")


def call_resource(self, resource_name, service_name):
    params = ""
    user_id = find_app_instance_by_id(
        self.t_app_instance, self.instance_id).get("user_id")
    if service_name == 'CoffeeConfigure/getcoffeelink':
        params = '{"workflow_instance_id": "' + str(self.instance_id) + '", "userId": "' + user_id + '"}'
    elif service_name == 'makeCoffee':
        params = '{"action": "start", "mode": "0", "level": "0", "num": "0"}'
    elif service_name == 'simplecrowdsourcing/publish/publishtask':
        params = '{"workflow_instance_id": "' + str(self.instance_id) + '",' \
                                                                        ' "userId": "' + user_id + '", "taskDesc": "get coffee",' \
                                                                                                   ' "locationDesc": "Meeting room", "bonus": "2", "duration": "5"}'
    data = {'func_name': service_name, 'params': params}
    logging.info("workflow engine is calling resource service orderCoffee")
    url = 'http://' + resource_name + '-proxy.default:8888/'
    logging.info("request result: " + requests.post(url, data).text)


def receive_from_monitor(self, resource_name):
    workflow_instance_id = str(self.instance_id)
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
        self.body = body
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 收到消息后通知mq
        channel.close()  # !!!在执行引擎中使用 表示只接收一次即关闭监听 接着执行流程的下一步

    channel.basic_consume(result.method.queue,
                          callback,
                          # no_ack 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列
                          # True，无论调用callback成功与否，消息都被消费掉
                          False)

    logging.info('[*] Waiting for msg, routing_key: ' + workflow_instance_id)
    channel.start_consuming()


def update_start_task(self):
    # 更新action状态
    update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "1")
    # 更新action时间
    create_time = get_time()
    update_app_instance_action_time(self.t_app_instance, self.instance_id, self.task_id, "1", create_time)


def update_stop_task(self, state):
    # 更新数据库状态
    update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, state)
    # 更新action时间
    create_time = get_time()
    update_app_instance_action_time(self.t_app_instance, self.instance_id, self.task_id, state, create_time)


def pub_to_MQ(msg):
    '''
    向MQ推送消息
    '''
    print(msg.get('msg'))


def exception_handler(state, cur_task):
    '''
    流程的异常处理
    '''
    task_state = state.result[cur_task]
    assert task_state.is_successful()


# class Coffee_Flow():
#     def run_predefined_flow(self):
#         flow = Flow("Run a Prefect Flow in Docker")
#
#         # 点咖啡 task
#         # task_config_1 = {
#         #     'task_type':'action',
#         #     'task_name':'',
#         #     'task_id':'',
#         #     'coffee order id': '',
#         # }
#         # task_1 = Scp_Task('点咖啡', task_config_1)
#
#         task_config_1 = {
#             'task_type': 'action',
#             'task_name': 'make coffee',
#             'task_id': '',
#         }
#         task_1 = Scp_Task(name='make coffee', **task_config_1)
#
#         task_config_2 = {
#             'task_type': 'event',
#             'task_name': 'coffee finished',
#             'task_id': '',
#         }
#         # task_2 = Scp_Task.copy()
#         task_2 = Scp_Event_Task(name='coffee finished', **task_config_2)
#
#         task_config_3 = {
#             'task_type': 'action',
#             'task_name': 'send coffee',
#             'task_id': '',
#         }
#         task_3 = Scp_Task(name='send coffee', **task_config_3)
#
#         # add tasks to the flow
#         flow.add_task(Scp_StartEvent_Task())
#         flow.add_task(task_1)
#         flow.add_task(task_2)
#         flow.add_task(task_3)
#         flow.add_task(Scp_EndEvent_Task())
#
#         # create non-data dependencies
#         task_2.set_upstream(task_1, flow=flow)
#         task_3.set_upstream(task_2, flow=flow)
#         # task_4.set_upstream(task_3, flow=flow)
#
#         # create data bindings
#         task_input_1 = {
#             'msg': 'test_task_1',
#             'url': 'http://39.106.6.6:8080/SCIDE/SCManager?action=executeContract&contractID=CoffeeFDU&operation=postMakeCoffee&arg=%22%22',
#         }
#         task_input_3 = {
#             'msg': 'test_task_2',
#             'url': '',
#         }
#         task_1.bind(**task_input_1, flow=flow)
#         task_3.bind(**task_input_3, flow=flow)
#
#         # start flow
#         state = flow.run()


# 通用流程
class CommonFlow:
    def __init__(self, instance_id, **mongodb):
        # self.flow = Flow("Run a Prefect Flow in Docker")
        self.instance_id = instance_id
        self.t_app_class = mongodb.get('t_app_class')
        self.t_app_instance = mongodb.get('t_app_instance')
        self.app_class_child_shapes = find_app_class_by_instance_id(self.t_app_instance, self.instance_id).get(
            "app_class").get("childShapes")
        self.user_id = find_app_instance_by_id(
            self.t_app_instance, self.instance_id).get("user_id")
        self.all_action_count = 0
        self.start_tasks = []
        self.get_all_action_count()

    def get_all_action_count(self):
        # 获取流程信息
        for child_shape in self.app_class_child_shapes:
            stencil = child_shape.get("stencil").get("id")
            if stencil in ["StartNoneEvent", "DefaultEvent", "SocialAction", "PhysicalAction", "CyberAction"]:
                self.all_action_count += 1

    def run_flow(self):
        t = Thread(target=self.new_thread, )
        t.start()

    def new_thread(self):

        # 获取流程信息
        self.get_flow(None)
        # 拼接流程
        # todo 开始执行
        action_ips = find_app_instance_action_ip_by_instance_id(self.t_app_instance, self.instance_id).get("action_ip")
        print(action_ips)
        for start_task in self.start_tasks:
            start_ip = action_ips.get(start_task.task_id)
            start_run_address = "http://" + start_ip + ":8888/run"
            print(start_run_address)
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            params = {}
            response = requests.post(start_run_address, data=params, headers=headers)
            print("response Code: " + str(response.status_code))
            print("response content: " + response.text)

        # state = self.flow.run()
        print("执行完成")

    def get_flow(self, pre_task):
        if pre_task == None:
            # 从初始节点开始
            for child_shape in self.app_class_child_shapes:
                stencil = child_shape.get("stencil").get("id")
                if stencil == 'StartNoneEvent':
                    # 获取开始task
                    task_config = {
                        'instance_id': self.instance_id,
                        't_app_class': self.t_app_class,
                        't_app_instance': self.t_app_instance,
                        'task_type': 'StartNoneEvent',
                        'task_name': 'StartNoneEvent',
                        'task_id': child_shape.get("resourceId"),
                    }
                    task = Scp_StartEvent_Task(name="StartNoneEvent", **task_config)
                    # todo 加入初始任务节点
                    self.start_tasks.append(task)
                    # 配置初始输入库
                    # task_input = self.app_instance_resource
                    # task.bind(**task_input, flow=self.flow)
                    # 开始加入其他task
                    self.get_flow(task)
                elif stencil == 'DefaultEvent':
                    # 获取开始task
                    task = self.get_event_task(child_shape)
                    # todo 加入初始任务节点
                    self.start_tasks.append(task)
                    # self.flow.add_task(task)
                    # 配置初始输入库
                    # task_input = self.app_instance_resource
                    # task.bind(**task_input, flow=self.flow)
                    # 开始加入其他task
                    self.get_flow(task)
        else:
            # 判断是否所有的节点都已经生成完了，如果都完了就开始执行
            if self.all_action_count == 0:
                return "success"
            self.all_action_count -= 1
            # 判断当前节点还有没有下一个节点
            child_shape = self.get_child_shape_by_id(pre_task.task_id)
            next_nodes = self.get_next_nodes(child_shape)
            next_workflow_proxy = ""
            if len(next_nodes) > 0:
                for next_node in next_nodes:
                    task = self.get_task(next_node)
                    next_workflow_proxy += task.task_id + ","
                    self.get_flow(task)
            else:
                next_workflow_proxy = "null"
            if pre_task.task_type == "StartNoneEvent":
                # deployment_and_service_name = "i-"+str(pre_task.instance_id) + pre_task.task_id+"-t"
                # deployment_and_service_name = deployment_and_service_name.lower()
                deployment_and_service_name = str(hash(str(pre_task.instance_id) + pre_task.task_id))
                deployment_and_service_name = "s" + deployment_and_service_name + "s"
                clusterIP = create_service_main(deployment_and_service_name, deployment_and_service_name)
                update_app_instance_action_ip(self.t_app_instance, self.instance_id, pre_task.task_id, clusterIP)
                params = {'workflow_instance_id': str(self.instance_id),
                          'workflow_proxy_type': 'StartNoneEvent',
                          'workflow_proxy_id': pre_task.task_id,
                          'next_workflow_proxy': next_workflow_proxy}
                print("------params0-------" + str(params))
                init_address = "http://" + clusterIP + ":8888/init"
                # init_address = "http://106.15.102.123:31507/init"
                print("------init_address-------" + init_address)
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                time.sleep(10)  # 等待部署好
                response = requests.post(init_address, data=params, headers=headers)
                print("response Code: " + str(response.status_code))
                print("response content: " + response.text)
            else:
                deployment_and_service_name = str(hash(str(pre_task.instance_id) + pre_task.task_id))
                deployment_and_service_name = "s" + deployment_and_service_name + "s"
                clusterIP = create_service_main(deployment_and_service_name, deployment_and_service_name)
                update_app_instance_action_ip(self.t_app_instance, self.instance_id, pre_task.task_id, clusterIP)
                params = {'workflow_instance_id': str(self.instance_id),
                          'workflow_proxy_type': pre_task.task_type,
                          'workflow_proxy_id': pre_task.task_id,
                          'executor_resource_id': pre_task.task_executor,
                          'service_input': pre_task.task_input,
                          'service_name': pre_task.task_name,
                          'next_workflow_proxy': next_workflow_proxy}
                print("------params1-------" + str(params))
                # body = {"payload": "{\"action\":\"start\",\"mode\":\"0\",\"level\":\"0\",\"num\":\"0\"}",
                #         "topic": "lab/lab401_coffee1/switch"}
                init_address = "http://" + clusterIP + ":8888/init"
                # init_address = "http://106.15.102.123:31507/init"
                print("------init_address-------" + init_address)
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                time.sleep(10)  # 等待部署好
                response = requests.post(init_address, data=params, headers=headers)
                print("response Code: " + str(response.status_code))
                print("response content: " + response.text)

    def get_child_shape_by_id(self, target_id):
        for child_shape in self.app_class_child_shapes:
            resource_id = child_shape.get("resourceId")
            if resource_id == target_id:
                return child_shape

    def get_next_nodes(self, child_shape):
        next_nodes = []
        outgoings = child_shape.get("outgoing")
        for outgoing in outgoings:
            resource_id = outgoing.get("resourceId")
            sequence_flow = self.get_child_shape_by_id(resource_id)
            sequence_flow_outgoings = sequence_flow.get("outgoing")
            for sequence_flow_outgoing in sequence_flow_outgoings:
                next_node_id = sequence_flow_outgoing.get("resourceId")
                next_nodes.append(self.get_child_shape_by_id(next_node_id))
        return next_nodes

    def get_task(self, node):
        task_type = node.get("stencil").get("id")
        task_name = node.get("properties").get("name")
        task_id = node.get("resourceId")
        task_executor = node.get("properties").get("activityelement").get("id")
        task_inputs = node.get("properties").get("input")
        task_input = ""
        for input0 in task_inputs:
            task_input += input0.get("id") + "." + input0.get("resource_param") + "." + input0.get(
                "service_param") + "$"
        task_config = {
            'instance_id': self.instance_id,
            't_app_class': self.t_app_class,
            't_app_instance': self.t_app_instance,
            'task_type': task_type,
            'task_name': task_name,
            'task_id': task_id,
            'task_executor': task_executor,
            'task_input': task_input
        }
        task = Scp_Task(name=task_name, **task_config)
        return task
        # if task_type == 'DefaultEvent':
        #     task = Scp_Event_Task(name=task_name, **task_config)
        #     return task
        # else:
        #     task = Scp_Task(name=task_name, **task_config)
        #     return task


if __name__ == "__main__":
    cf = CommonFlow("5dca774eefb59bfd7f4ad03e")
    cf.run_predefined_flow()
