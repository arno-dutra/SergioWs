use crate::OpCode;

pub enum ControlFrame {
    Ping(Vec<u8>), // len < 125
    Pong(Vec<u8>), // len < 125
    Close(Vec<u8>), // len < 125
}

impl ControlFrame {
    pub(crate) fn new(op_code: OpCode, payload: Vec<u8>) -> Self {
        match op_code {
            OpCode::Ping => ControlFrame::Ping(payload),
            OpCode::Pong => ControlFrame::Pong(payload),
            OpCode::Close => ControlFrame::Close(payload),
            _ => panic!("Invalid opcode for control frame"),
        }
    }
}