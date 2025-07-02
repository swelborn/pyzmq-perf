from pydantic import BaseModel
class TestConfig(BaseModel):
    count: int  # number of messages to send
    size: int  # size of messages
    zero_copy: bool  # use pyzmq zero-copy
    pub: bool  # true, use pub/sub. false, use push/pull
    rcvhwm: int  # puller hwm
    sndhwm: int  # sender hwm
    test_number: int
