# coding=utf-8
import pika

credentials = pika.PlainCredentials('guest', 'guest')
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.19.109.143'))
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.141.221.88'))
connection = pika.BlockingConnection(pika.ConnectionParameters(host='139.196.228.210'))
channel = connection.channel()
# channel.queue_declare(queue='cc')   # 如果有cc的队列,略过;如果没有,创建cc的队列
# for test
# channel.queue_declare(queue='physical_coffeemaker_monitor')
# channel.basic_publish(exchange='', routing_key='physical_coffeemaker_monitor', body='hello!world!!!')
#
# # channel.basic_publish(exchange='', routing_key='cc', body='hello!world!!!')
# print("[x] sent 'hello,world!'")
# connection.close()

exchange_name = 'cyber_ordercoffee_exchange'
result = {"state": "1",
          "data": {
              "action": "2",
              "mode": "3",
              "level": "4",
              "num": "5"
          }}
result = str(result)
workflow_instance_id = '5e58eb2ee4b1c3ae777bd363'
channel.exchange_declare(exchange=exchange_name, durable=False, exchange_type='direct')
channel.basic_publish(exchange=exchange_name, routing_key=workflow_instance_id, body=result)
print("[x] sent '" + result + "' to workflow engine")
connection.close()
