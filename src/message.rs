use crate::OpCode;
use bytes::Bytes;

pub enum Message {
    Text(String),
    Binary(Bytes),
}

pub(crate) enum MessageBuffer {
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

impl From<MessageBuffer> for Message {
    fn from(buffer: MessageBuffer) -> Self {
        match buffer {
            MessageBuffer::Text(vec) => {
                let string = String::from_utf8(vec).unwrap_or_default();
                Message::Text(string)
            }
            MessageBuffer::Binary(vec) => Message::Binary(Bytes::from(vec)),
        }
    }
}

impl MessageBuffer {
    pub(crate) fn with_capacity(op_code: OpCode, capacity: usize) -> Self {
        match op_code {
            OpCode::Text => MessageBuffer::Text(Vec::with_capacity(capacity)),
            OpCode::Binary => MessageBuffer::Binary(Vec::with_capacity(capacity)),
            _ => panic!("Invalid op code for message buffer"),
        }
    }
    pub(crate) fn get_inner(&mut self) -> &mut Vec<u8> {
        match self {
            MessageBuffer::Binary(vec) => vec,
            MessageBuffer::Text(vec) => vec,
        }
    }
}