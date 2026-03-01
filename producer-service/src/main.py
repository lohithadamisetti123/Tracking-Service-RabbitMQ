import json
import os
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict

import pika
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, field_validator

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "user_activity_events")

connection = None
channel = None


class UserActivityEvent(BaseModel):
    user_id: int = Field(..., ge=1)
    event_type: str = Field(..., min_length=1, max_length=50)
    timestamp: str
    metadata: Dict[str, Any]

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("timestamp must be a valid ISO 8601 string")
        return v


def get_rabbitmq_connection():
    global connection, channel
    if connection and channel and connection.is_open and channel.is_open:
        return connection, channel

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=credentials,
    heartbeat=120,
    blocked_connection_timeout=120,
)


    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    return connection, channel


def close_rabbitmq_connection():
    global connection, channel
    try:
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()
    finally:
        connection = None
        channel = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        get_rabbitmq_connection()
    except Exception as e:
        print(f"Failed to connect to RabbitMQ at startup: {e}", file=sys.stderr)
    yield
    close_rabbitmq_connection()


app = FastAPI(title="User Activity Tracking Producer API", lifespan=lifespan)


@app.post("/api/v1/events/track", status_code=status.HTTP_202_ACCEPTED)
async def track_event(event: UserActivityEvent):
    try:
        _, ch = get_rabbitmq_connection()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"RabbitMQ unavailable: {e}",
        )

    try:
        body = json.dumps(event.model_dump()).encode("utf-8")
        ch.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2
            ),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish event: {e}",
        )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"message": "Event accepted for processing"},
    )


@app.get("/health")
async def health():
    try:
        _, ch = get_rabbitmq_connection()
        ch.queue_declare(queue=RABBITMQ_QUEUE, durable=True, passive=False)
        return {"status": "ok", "rabbitmq": "connected"}
    except Exception as e:
        # Return 503 and clear connections so next call recreates them cleanly
        close_rabbitmq_connection()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"RabbitMQ health failed: {e}",
        )



@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc: ValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": exc.errors()},
    )


def handle_sigterm(*_args):
    close_rabbitmq_connection()
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PRODUCER_PORT", "8000")),
        reload=False,
    )
