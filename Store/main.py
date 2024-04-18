import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    delete,
    update
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)

# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

# FastAPI app setup
app = FastAPI()
# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()


# FastAPI WebSocket endpoint
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    with SessionLocal() as current_session:
        for data_unit in data:
            que = processed_agent_data.insert().values(
                road_state=data_unit.road_state,
                x=data_unit.agent_data.accelerometer.x,
                y=data_unit.agent_data.accelerometer.y,
                z=data_unit.agent_data.accelerometer.z,
                latitude=data_unit.agent_data.gps.latitude,
                longitude=data_unit.agent_data.gps.longitude,
                timestamp=data_unit.agent_data.timestamp,
            )
            current_session.execute(que)
        current_session.commit()

    await send_data_to_subscribers(data)


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as current_session:
        que = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        requested_unit = current_session.execute(que).first()

        if requested_unit is None:
            raise HTTPException(status_code=404, detail="Requested unit not found")

        return requested_unit


@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    with SessionLocal() as current_session:
        requested_units = current_session.execute(select(processed_agent_data))

        if requested_units is None:
            raise HTTPException(status_code=404, detail="Requested units not found")

        return requested_units


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    with SessionLocal() as current_session:
        que = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        requested_unit = current_session.execute(que).first()

        if requested_unit is None:
            raise HTTPException(status_code=404, detail="Requested unit not found")

        que = update(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).values(
            road_state=data.road_state,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp,
        )

        current_session.execute(que)
        current_session.commit()

        que = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        requested_unit = current_session.execute(que).first()

        return requested_unit


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as current_session:
        que = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        requested_unit = current_session.execute(que).first()

        if requested_unit is None:
            raise HTTPException(status_code=404, detail="Requested unit not found")

        que = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)

        current_session.execute(que)
        current_session.commit()
        return requested_unit


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)