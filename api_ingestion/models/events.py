from pydantic import BaseModel

class TelemetryEvent(BaseModel):
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    
    # Other fields are flexible, we allow anything
    model_config = {"extra": "allow"}

class SimulatorCommand(BaseModel):
    command: str  # START, STOP
