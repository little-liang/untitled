import pika
credentials = pika.PlainCredentials('admin','123123')
connection = pika.BlockingConnection(pika.ConnectionParameters('172.18.126.52',5672,'/',credentials))
channel = connection.channel()

# 声明queue
channel.queue_declare(queue='balance')

# n RabbitMQ a message can never be sent directly to the queue, it always needs to go through an exchange.
channel.basic_publish(exchange='',routing_key='balance',body='Hello World!')
print(" [x] Sent 'Hello World!'")
connection.close()