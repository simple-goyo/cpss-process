# coding=utf-8
import pika
import logging
import requests

logging.basicConfig(level=logging.INFO, filename='logger.log',
                    format="%(levelname)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s")

resource_type = 'cyber'
resource_name = 'orderCoffee'
# service_name = 'order-coffee-service'


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
        channel.close()   # !!!在执行引擎中使用 表示只接收一次即关闭监听 接着执行流程的下一步

    channel.basic_consume(result.method.queue,
                          callback,
                          # no_ack 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列
                          # True，无论调用callback成功与否，消息都被消费掉
                          False)

    logging.info('[*] Waiting for msg')
    channel.start_consuming()


if __name__ == '__main__':
    call_resource()
    receive_from_monitor('workflow_1')
