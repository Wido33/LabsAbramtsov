from confluent_kafka import Producer
import json
import time
from datetime import datetime
import sys

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def main():

    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)

    try:
        while True:
            # Чтение ввода пользователя
            user_input = input("Введите сообщение (user_id,action) или 'exit' для выхода: ")
            if user_input.lower() == 'exit':
                break
            
            try:
                user_id, action = user_input.split(',')
                message = {
                    'user_id': int(user_id),
                    'action': action.strip(),
                    'timestamp': datetime.now().isoformat()
                }
                # Отправка сообщения
                producer.produce(
                    'user_actions',
                    key=str(message['user_id']),
                    value=json.dumps(message),
                    callback=delivery_report
                )
                producer.flush()
            except ValueError:
                print("Неверный формат. Используйте: user_id,action")
    except KeyboardInterrupt:
        print("\nProducer остановлен")

if __name__ == '__main__':
    main()