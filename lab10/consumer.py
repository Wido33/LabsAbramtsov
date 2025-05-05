from confluent_kafka import Consumer, KafkaException, Producer
import json
from collections import defaultdict
import psycopg2
import sys
from datetime import datetime

# Конфигурация продюсера для Dead Letter Topic
producer_conf = {'bootstrap.servers': 'localhost:9092'}
error_producer = Producer(producer_conf)

def get_db_connection():
    """Установка соединения с PostgreSQL"""
    return psycopg2.connect(
        host="localhost",
        database="kafka_events",
        user="postgres",
        password="1111",
        port="5432"
    )

def delivery_report(err, msg):
    """Callback для отслеживания доставки сообщений в DLT"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def save_to_db(data):
    """Сохранение данных в PostgreSQL"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Проверяем, есть ли уже такое сообщение
        cur.execute(
            """INSERT INTO user_actions 
            (user_id, action, timestamp) 
            VALUES (%s, %s, %s)""",
            (data['user_id'], data['action'], data['timestamp'])
        )
        conn.commit()
        print(f"Сохранено в БД: {data}")
        
    except Exception as e:
        print(f"Ошибка при сохранении в БД: {e}")
        # Отправляем в Dead Letter Topic
        error_producer.produce(
            'user_actions_dlt',
            key=str(data.get('user_id', 'unknown')),
            value=json.dumps({
                "original_data": data,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }),
            headers={'error_type': 'db_error'},
            callback=delivery_report
        )
        error_producer.flush()
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'user_actions_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['user_actions'])

    stats = {
        'total_messages': 0,
        'actions_count': defaultdict(int),
        'users_count': defaultdict(int)
    }

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            try:
                data = json.loads(msg.value())
                stats['total_messages'] += 1
                stats['actions_count'][data['action']] += 1
                stats['users_count'][data['user_id']] += 1

                # Фильтрация покупок
                if data['action'] == 'purchase':
                    print(f" Purchase detected: User {data['user_id']}")
                
                save_to_db(data)
                
                if stats['total_messages'] % 10 == 0:
                    print("\n=== Статистика ===")
                    print(f"Всего обработано: {stats['total_messages']}")
                    print("Топ действий:", dict(sorted(
                        stats['actions_count'].items(), 
                        key=lambda x: x[1], 
                        reverse=True
                    )[:3]))
                    print("Активные пользователи:", dict(sorted(
                        stats['users_count'].items(),
                        key=lambda x: x[1],
                        reverse=True
                    )[:3]))
                    print("=================\n")
                
                # Подтверждение обработки
                consumer.commit(asynchronous=False)
                
            except json.JSONDecodeError as e:
                print(f" Ошибка декодирования JSON: {msg.value()}")
                error_producer.produce(
                    'user_actions_dlt',
                    value=msg.value(),
                    headers={'error_type': 'json_decode'}
                )
                error_producer.flush()
                
    except KeyboardInterrupt:
        print("\nConsumer остановлен")
    finally:
        consumer.close()
        error_producer.flush()
        print("Ресурсы освобождены")

if __name__ == '__main__':
    main()