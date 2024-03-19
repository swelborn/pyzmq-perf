from pydantic import BaseModel

class TestConfig(BaseModel):
    count: int
    size: int
    zero_copy: bool
    pub: bool
    rcvhwm: int
    sndhwm: int
    sndtimeo: int
    rcvtimeo: int
