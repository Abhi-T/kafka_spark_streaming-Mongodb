'''
produce data into kafka
'''

from kafka.client_async import KafkaClient
import json
import random
import threading
import time
import global_vals
users=['t1','t2']

def produce_data():
    hosts='localhost:9092'
    num_user=2
    assert len(users)==num_user #if condition returns True, then nothing happens

    client=KafkaClient(hosts=hosts)
    print('client topics', client.topics)
    topic=client.topics[bytes(global_vals.kafka_topic, encoding='utf-8')]
    producer=topic.get_producer()
    def work(user_number):
        while True:
            msg=json.dumps({
                'timestamp': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'uid': users[user_number],
                'heart_rate': random.randint(50, 70),
                'steps': random.randint(100, 1000)
            })
            print('msg:',msg)
            producer.producer(bytes(msg, encoding='utf-8'))
            time.sleep(global_vals.data_produce_duration)

    thread_list=[threading.Thread(target=work, args=(i,)) for i in range(num_user)]
    for thread in thread_list:
        thread.setDaemon(True)
        thread.start()

    #block it to run forever
    while True:
        time.sleep(6000)
        pass

if __name__ == '__main__':
    produce_data()