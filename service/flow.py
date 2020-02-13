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
from prefect import Task, Flow
from bson.json_util import loads
from threading import Thread
from service.app_service import *
from service.resource_service import *
from util.utctime import *
import requests
import time
import pika
import logging


class Scp_StartEvent_Task(Task):
    def __init__(self, name: str, **config):
        super().__init__(name=name)
        self.action_desc = ''
        self.instance_id = config.get('instance_id')
        self.t_app_class = config.get('t_app_class')
        self.t_app_instance = config.get('t_app_instance')
        self.task_name = config.get('task_name')
        self.task_id = config.get('task_id')

    def run(self, **input):
        # update_stop_task(self)
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "2")
        create_time = get_time()
        update_app_instance_action_time(self.t_app_instance, self.instance_id, self.task_id, "2", create_time)
        # pub_to_MQ({'msg': 'flow start', 'flow id': ''})


class Scp_EndEvent_Task(Task):
    def run(self):
        pub_to_MQ({'msg': 'flow end', 'flow id': ''})


class Scp_Task(Task):
    def __init__(self, name: str, **config):
        super().__init__(name=name)
        self.action_desc = ''
        self.instance_id = config.get('instance_id')
        self.t_app_class = config.get('t_app_class')
        self.t_app_instance = config.get('t_app_instance')
        self.task_name = config.get('task_name')
        self.task_id = config.get('task_id')
        self.task_executor = config.get('task_executor')

    def run(self, **input):
        '''
        任务执行
        '''
        # 获取访问地址
        task_url = get_task_url(self)
        # 获取输入
        # 初始化输出
        output = None
        # 更新任务状态
        update_start_task(self)
        # 绑定资源
        if self.name != '获取体重数据':
            time.sleep(10)
        try:
            if self.name == '星巴克下单':
                r = requests.get("https://xiaomi.xuwang.tech/res", verify=False)
                output = r.json()
                number = output.get("number")
                task_url = task_url[:-1] + str(number)
                # update_stop_task(self, "2")
                # return None
            if self.name == '送咖啡':
                r = requests.get(task_url, verify=False)
                update_stop_task(self, "2")
                return None
            if self.name == '端热水':
                r = requests.get(task_url, verify=False)
                update_stop_task(self, "2")
                return None
            r = requests.get(task_url, verify=False)
        except:
            print(self.task_name, "服务请求错误")
            update_stop_task(self, "3")
            return output
        output = r.json()
        data = output.get("data")
        number = output.get("number")
        if data is None and number is None:
            print(self.task_name, "智能合约请求错误:", output.get('msg'))
            update_stop_task(self, "3")
            return output
        # data1 = loads(loads(loads(loads(data).get("result")).get("response")).get("returnJSONStr"))
        # data1 = loads(loads(data).get("result"))z/
        # 更新任务状态
        if self.task_name == '烧水':
            time.sleep(30)
        update_stop_task(self, "2")
        # 解绑资源
        return output


class Scp_Event_Task(Task):
    def __init__(self, name, **config):
        super().__init__(name=name)
        self.action_desc = ''
        self.instance_id = config.get('instance_id')
        self.t_app_class = config.get('t_app_class')
        self.t_app_instance = config.get('t_app_instance')
        self.task_type = config.get('task_type')
        self.task_name = config.get('task_name')
        self.task_id = config.get('task_id')
        self.task_executor = config.get('task_executor')

    def run(self, **input):
        '''
        任务执行
        '''
        # 获取访问地址
        task_url = get_task_url(self)
        # 获取输入
        # 初始化输出
        output = None
        # 推送任务开始消息到中间件
        # 更新任务状态
        update_start_task(self)
        # 等待MQ消息
        print('wait for event')

        if self.name == '视频播放完成':
            time.sleep(30)

        b_event = False
        # b_event = True
        # time.sleep(20)
        count = 0
        weight = 0
        while not b_event:
            r = requests.get(task_url)
            output = r.json()
            # 咖啡机做好了事件
            data = output.get("data")
            if data is None:
                print(self.task_name, "智能合约请求错误:", output.get('msg'))
                update_stop_task(self, "3")
                return output
            # data1 = loads(loads(loads(loads(data).get("result")).get("response")).get("returnJSONStr"))
            # data1 = loads(loads(loads(data).get("result")).get("returnJSONStr"))
            # data1 = loads(loads(loads(loads(data).get("result")).get("response")).get("returnJSONStr"))
            if self.task_name == '咖啡制作完成':
                try:
                    data1 = loads(loads(loads(data).get("result")).get("returnJSONStr"))
                    state_value = data1.get("data").get("data").get("State").get("value")
                    if state_value == "3":
                        b_event = True
                except:
                    print(self.task_name, "智能合约解析请求错误:", output.get('data'))
                    update_stop_task(self, "3")
                    return output
            # 体重秤事件
            elif self.task_name == '获取体重数据':
                try:
                    data1 = loads(loads(loads(data).get("result")).get("returnJSONStr"))
                    weight_new = data1.get("weight")
                    if weight == 0:
                        weight = weight_new
                    else:
                        if weight != weight_new:
                            b_event = True
                        else:
                            b_event = False
                    if count > 3:
                        b_event = True
                except:
                    print(self.task_name, "智能合约解析请求错误:", output.get('data'))
                    update_stop_task(self, "3")
                    return output
            elif self.task_name == '视频播放完成':
                b_event = True
            # 继续
            count += 1
            if count > 15:
                b_event = True
            time.sleep(10)

        # 更新任务状态
        update_stop_task(self, "2")
        return output


def get_task_url(self):
    app_instance_resources = find_app_instance_resource_by_instance_id(
        self.t_app_instance, self.instance_id).get("resource")
    # 获取执行者实例id
    app_instance_resource = app_instance_resources.get(self.task_executor)
    # 根据执行者实例和任务名称获取执行地址
    # data = {
    #     "refResource": app_instance_resource
    # }
    # data_json = json.dumps(data)
    # reponse = requests.get("http://221.228.66.83:32022/apis/resource", data=data_json)
    # output = reponse.json()
    output = get_resource_by_id(app_instance_resource)
    operation_map = {
        "制作咖啡": "make-coffee",
        "咖啡制作完成": "get-coffee-status",
        "送咖啡": "get-coffee",
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
    resource_operations = output[0].get("operation")
    need_operation = operation_map.get(self.task_name)
    task_url = ""
    for resource_operation in resource_operations:
        if need_operation == resource_operation.get("name"):
            task_url = resource_operation.get("httpAction").get("url")
    return task_url


# def get_url(resource_instance_resource, task_name):
#     output = get_resource_by_id(resource_instance_resource)
#     operation_map = {
#         "制作咖啡": "make-coffee",
#         "咖啡制作完成": "get-coffee-status",
#         "送咖啡": "get-coffee",
#         "端热水": "get-water",
#         "获取体重数据": "get-weight",
#         "开启空气净化": "start-purify",
#         "获取当前空气状态": "get-air-condition",
#         "播放锻炼视频": "play-video",
#         "烧水": "boil-water",
#         "播放语音通知": "speak",
#         "视频播放完成": "stop-video",
#         "准备订单": "get-order",
#         "星巴克下单": "order-coffee",
#     }
#     resource_operations = output[0].get("operation")
#     need_operation = operation_map.get(task_name)
#     task_url = ""
#     for resource_operation in resource_operations:
#         if need_operation == resource_operation.get("name"):
#             task_url = resource_operation.get("httpAction").get("url")
#     return task_url


logging.basicConfig(level=logging.INFO, filename='logger.log',
                    format="%(levelname)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s")


def call_resource(service_name, func_name):
    # func_name = 'order'
    data = {'func_name': func_name, 'params': 'params'}
    logging.info("workflow engine is calling resource service orderCoffee")
    url = 'http://' + service_name + '-proxy.default:8888/'
    logging.info("request result: " + requests.post(url, data).text)


def receive_from_monitor(workflow_instance_id):
    global resource_name
    global resource_type
    exchange_name = resource_type + '_' + resource_name + '_exchange'

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
    channel.queue_bind(exchange=exchange_name, queue=result.method.queue, routing_key=workflow_instance_id)

    # 定义一个回调函数来处理消息队列中的消息，这里是打印出来
    def callback(ch, method, properties, body):
        logging.info('[x] Recieved %r' % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 收到消息后通知mq
        # channel.close()   # !!!在执行引擎中使用 表示只接收一次即关闭监听 接着执行流程的下一步

    channel.basic_consume(result.method.queue,
                          callback,
                          # no_ack 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列
                          # True，无论调用callback成功与否，消息都被消费掉
                          False)

    logging.info('[*] Waiting for msg')
    channel.start_consuming()


def update_start_task(self):
    # 更新action状态
    update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "1")
    # 更新action时间
    create_time = get_time()
    update_app_instance_action_time(self.t_app_instance, self.instance_id, self.task_id, "1", create_time)
    # 解绑资源
    app_instance_resources = find_app_instance_resource_by_instance_id(
        self.t_app_instance, self.instance_id).get("resource")
    # 获取执行者实例id
    app_instance_resource = app_instance_resources.get(self.task_executor)
    data = bind_resource(app_instance_resource)
    if self.task_name == "咖啡制作完成" or self.task_name == "播放语音通知" or self.task_name == "视频播放完成":
        return
    voice_content = "开始执行" + self.task_name
    voice_url = "http://39.104.154.79:6000/tts/" + voice_content
    r = requests.get(voice_url, verify=False)
    # 推送任务结束消息到中间件
    pub_to_MQ({
        'msg': 'task start',
        'task step': self.task_name,
        'flow_id': self.task_id
    })


def update_stop_task(self, state):
    # 更新数据库状态
    update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, state)
    # 更新action时间
    create_time = get_time()
    update_app_instance_action_time(self.t_app_instance, self.instance_id, self.task_id, state, create_time)
    # 解绑资源
    app_instance_resources = find_app_instance_resource_by_instance_id(
        self.t_app_instance, self.instance_id).get("resource")
    # 获取执行者实例id
    app_instance_resource = app_instance_resources.get(self.task_executor)
    data = delete_bind_resource(app_instance_resource)
    if self.task_name == "咖啡制作完成" or self.task_name == "播放语音通知" or self.task_name == "制作咖啡" or self.task_name == "播放锻炼视频" or self.task_name == "视频播放完成" or self.task_name == "送咖啡" or self.name == '端热水':
        return
    voice_content = self.task_name + "执行完成"
    voice_url = "http://39.104.154.79:6000/tts/" + voice_content
    # 推送任务结束消息到中间件
    r = requests.get(voice_url, verify=False)
    pub_to_MQ({
        'msg': 'task end',
        'task step': self.task_name,
        'flow_id': self.task_id
    })


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
        self.flow = Flow("Run a Prefect Flow in Docker")
        self.instance_id = instance_id
        self.t_app_class = mongodb.get('t_app_class')
        self.t_app_instance = mongodb.get('t_app_instance')
        self.app_class_child_shapes = find_app_class_by_instance_id(self.t_app_instance, self.instance_id).get(
            "app_class").get("childShapes")
        self.all_action_count = 0
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
        state = self.flow.run()
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
                    self.flow.add_task(task)
                    # 配置初始输入库
                    # task_input = self.app_instance_resource
                    # task.bind(**task_input, flow=self.flow)
                    # 开始加入其他task
                    self.all_action_count -= 1
                    self.get_flow(task)
        else:
            # 判断是否所有的节点都已经生成完了，如果都完了就开始执行
            if self.all_action_count == 0:
                return "success"
            # 判断当前节点还有没有下一个节点
            child_shape = self.get_child_shape_by_id(pre_task.task_id)
            next_nodes = self.get_next_nodes(child_shape)
            for next_node in next_nodes:
                task = self.get_task(next_node)
                self.flow.add_task(task)
                task.set_upstream(pre_task, flow=self.flow)
                self.all_action_count -= 1
                self.get_flow(task)

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
        task_config = {
            'instance_id': self.instance_id,
            't_app_class': self.t_app_class,
            't_app_instance': self.t_app_instance,
            'task_type': task_type,
            'task_name': task_name,
            'task_id': task_id,
            'task_executor': task_executor,
        }
        if task_type == 'DefaultEvent':
            task = Scp_Event_Task(name=task_name, **task_config)
            return task
        else:
            task = Scp_Task(name=task_name, **task_config)
            return task


if __name__ == "__main__":
    cf = CommonFlow("5dca774eefb59bfd7f4ad03e")
    cf.run_predefined_flow()
