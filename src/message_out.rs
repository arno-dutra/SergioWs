use crate::{Frame, Payload};
use bytes::Bytes;
use std::io::IoSlice;

pub enum MessageOut {
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Vec<u8>),
    Text(String),
    Binary(Vec<u8>),
    FragmentedBinary(Vec<Bytes>),
}

impl MessageOut {
    pub fn is_close(&self) -> bool {
        matches!(self, MessageOut::Close(_))
    }

    pub fn is_fragmented(&self) -> bool {
        matches!(self, MessageOut::FragmentedBinary(_))
    }

    pub(crate) fn to_single_frame(self) -> Frame<'static> {
        match self {
            MessageOut::Ping(payload) => Frame::ping(Payload::Owned(payload)),
            MessageOut::Pong(payload) => Frame::pong(Payload::Owned(payload)),
            MessageOut::Close(payload) => Frame::close_raw(Payload::Owned(payload)),
            MessageOut::Text(text) => Frame::text(Payload::Owned(text.into_bytes())),
            MessageOut::Binary(data) => Frame::binary(Payload::Owned(data)),
            MessageOut::FragmentedBinary(_) => panic!("Cannot convert fragmented binary message to a single frame"),
        }
    }

    pub(crate) fn build_header_for_fragmented_message(&self) -> Vec<u8> {
        match self {
            Self::FragmentedBinary(fragments) => {
                let mut header = Vec::with_capacity(10);
                let total_length: usize = fragments.iter().map(|f| f.len()).sum();

                header[0] = 0b1000_0000;
                header[0] |= 0b0000_0010; // Opcode: Binary

                if total_length < 126 {
                    header[1] = total_length as u8;
                } else if total_length < 65536 {
                    header[1] = 126;
                    header[2..4].copy_from_slice(&(total_length as u16).to_be_bytes());
                } else {
                    header[1] = 127;
                    header[2..10].copy_from_slice(&(total_length as u64).to_be_bytes());
                }
                header
            }
            _ => panic!("Header can only be built for fragmented messages"),
        }
    }

    pub(crate) fn fragmented_to_slices<'a>(&'a self, header: &'a [u8]) -> Vec<IoSlice<'a>> {
        match self {
            Self::FragmentedBinary(fragments) => {
                let mut slices = Vec::with_capacity(fragments.len() + 1);
                slices.push(IoSlice::new(&header));
                for fragment in fragments {
                    slices.push(IoSlice::new(fragment));
                }
                slices
            }
            _ => {
                panic!("Cannot convert non-fragmented message to slices");
            }
        }
    }
}