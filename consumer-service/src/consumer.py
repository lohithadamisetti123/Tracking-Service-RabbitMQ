import json
import os
import signal
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict

import mysql.connector
import pika
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "user_activity_events")

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root_password")
MYSQL_DB = os.getenv("MYSQL_DB", "user_activity_db")

connection = None
channel = None
consumer_thread = None
stop_event = threading.Event()
db_conn = None


def get_db_connection():
    global db_conn
    if db_conn and db_conn.is_connected():
        return db_conn
    db_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=True,
    )
    return db_conn


def insert_user_activity(event: Dict[str, Any]):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        dt = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        sql = """
            INSERT INTO user_activities (user_id, event_type, timestamp, metadata)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(
            sql,
            (
                int(event["user_id"]),
                str(event["event_type"]),
                dt,
                json.dumps(event.get("metadata", {})),
            ),
        )
    finally:
        cursor.close()


def get_rabbitmq_connection():
    global connection, channel

    # If we already have a good open connection, reuse it
    if connection and channel and connection.is_open and channel.is_open:
        return connection, channel

    # Otherwise, create a fresh connection + channel
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=120,                # relax heartbeat
        blocked_connection_timeout=120
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    return connection, channel



def close_rabbitmq():
    global connection, channel
    try:
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()
    finally:
        channel = None
        connection = None


def close_db():
    global db_conn
    try:
        if db_conn and db_conn.is_connected():
            db_conn.close()
    finally:
        db_conn = None


def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))
        insert_user_activity(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}", file=sys.stderr)
        ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    global connection, channel
    while not stop_event.is_set():
        try:
            # Ensure fresh healthy connection
            connection, channel = get_rabbitmq_connection()
            if not connection or not channel or not connection.is_open or not channel.is_open:
                raise Exception("RabbitMQ channel not open")

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"RabbitMQ connection error, retrying in 5s: {e}", file=sys.stderr)
            close_rabbitmq()
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error in consumer loop: {e}", file=sys.stderr)
            close_rabbitmq()
            time.sleep(5)



def stop_consumer():
    stop_event.set()
    try:
        if channel and channel.is_open:
            channel.stop_consuming()
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_thread
    try:
        get_db_connection()
        get_rabbitmq_connection()
    except Exception as e:
        print(f"Initial connection error in consumer: {e}", file=sys.stderr)

    stop_event.clear()
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()
    yield

    stop_consumer()
    if consumer_thread:
        consumer_thread.join(timeout=10)
    close_rabbitmq()
    close_db()


app = FastAPI(title="User Activity Consumer Service", lifespan=lifespan)


@app.get("/health")
async def health():
    status = {}
    try:
        conn = get_db_connection()
        status["mysql"] = "connected" if conn.is_connected() else "disconnected"
    except Exception as e:
        status["mysql"] = f"error: {e}"

    try:
        _, ch = get_rabbitmq_connection()
        ch.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        status["rabbitmq"] = "connected"
    except Exception as e:
        status["rabbitmq"] = f"error: {e}"

    if status.get("mysql") == "connected" and status.get("rabbitmq") == "connected":
        return {"status": "ok", **status}
    else:
        raise HTTPException(status_code=503, detail=status)


def handle_sigterm(*_args):
    stop_consumer()
    close_rabbitmq()
    close_db()
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "consumer:app",
        host="0.0.0.0",
        port=int(os.getenv("CONSUMER_PORT", "8001")),
        reload=False,
    )
