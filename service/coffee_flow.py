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
from service.app_service import *
import requests
import time


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
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "2")
        pub_to_MQ({'msg': 'flow start', 'flow id': ''})


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
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "1")
        # 推送任务开始消息到中间件
        pub_to_MQ({
            'msg': 'task start',
            'task step': self.task_name,
            'flow_id': self.task_id
        })
        r = requests.get(task_url)
        output = r.json()
        # 更新任务状态
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "2")
        # 推送任务结束消息到中间件
        pub_to_MQ({
            'msg': 'task end',
            'task step': self.task_name,
            'flow_id': self.task_id
        })
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
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "1")
        pub_to_MQ({
            'msg': 'task start',
            'task step': self.task_name,
            'flow_id': self.task_id
        })

        # 等待MQ消息
        print('wait for event')

        b_event = False
        while not b_event:
            r = requests.get(task_url)
            output = r.json()
            # 咖啡机做好了事件
            if self.task_name == '咖啡制作完成':
                data = output.get("data")
                data1 = loads(loads(loads(loads(data).get("result")).get("response")).get("returnJSONStr"))
                state_value = data1.get("data").get("data").get("State").get("value")
                if state_value == "3":
                    b_event = True
            # 体重秤事件
            time.sleep(5)

        # 更新任务状态
        update_app_instance_action_state(self.t_app_instance, self.instance_id, self.task_id, "2")
        # 推送任务结束消息到中间件
        pub_to_MQ({
            'msg': 'task end',
            'task step': self.task_name,
            'flow_id': self.task_id
        })
        return output


def get_task_url(self):
    app_instance_resources = find_app_instance_resource_by_instance_id(
        self.t_app_instance, self.instance_id).get("resource")
    # 获取执行者实例id
    app_instance_resource = app_instance_resources.get(self.task_executor)
    # 根据执行者实例和任务名称获取执行地址
    data = {
        "entity": app_instance_resource,
        "service": self.task_name,
    }
    reponse = requests.get("https://www.fastmock.site/mock/374c841f0e189e8e9917011c1d3fa5dc/ec/get_task_url", data)
    task_url = loads(reponse.content).get("url")
    return task_url


def pub_to_MQ(self, **msg):
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


class Coffee_Flow():
    def run_predefined_flow(self):
        flow = Flow("Run a Prefect Flow in Docker")

        # 点咖啡 task
        # task_config_1 = {
        #     'task_type':'action',
        #     'task_name':'',
        #     'task_id':'',
        #     'coffee order id': '',
        # }
        # task_1 = Scp_Task('点咖啡', task_config_1)

        task_config_1 = {
            'task_type': 'action',
            'task_name': 'make coffee',
            'task_id': '',
        }
        task_1 = Scp_Task(name='make coffee', **task_config_1)

        task_config_2 = {
            'task_type': 'event',
            'task_name': 'coffee finished',
            'task_id': '',
        }
        # task_2 = Scp_Task.copy()
        task_2 = Scp_Event_Task(name='coffee finished', **task_config_2)

        task_config_3 = {
            'task_type': 'action',
            'task_name': 'send coffee',
            'task_id': '',
        }
        task_3 = Scp_Task(name='send coffee', **task_config_3)

        # add tasks to the flow
        flow.add_task(Scp_StartEvent_Task())
        flow.add_task(task_1)
        flow.add_task(task_2)
        flow.add_task(task_3)
        flow.add_task(Scp_EndEvent_Task())

        # create non-data dependencies
        task_2.set_upstream(task_1, flow=flow)
        task_3.set_upstream(task_2, flow=flow)
        # task_4.set_upstream(task_3, flow=flow)

        # create data bindings
        task_input_1 = {
            'msg': 'test_task_1',
            'url': 'http://39.106.6.6:8080/SCIDE/SCManager?action=executeContract&contractID=CoffeeFDU&operation=postMakeCoffee&arg=%22%22',
        }
        task_input_3 = {
            'msg': 'test_task_2',
            'url': '',
        }
        task_1.bind(**task_input_1, flow=flow)
        task_3.bind(**task_input_3, flow=flow)

        # start flow
        state = flow.run()


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
        # 获取流程信息
        self.get_flow(None)
        # 拼接流程
        state = self.flow.run()
        print("run")

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
        task_executor = node.get("activityelement")
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
            task = Scp_Task(name=task_type, **task_config)
            return task


if __name__ == "__main__":
    cf = CommonFlow("5dca774eefb59bfd7f4ad03e")
    cf.run_predefined_flow()
