// Copyright 2023-2026 Divy Srivastava <dj.srivastava23@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! _fastwebsockets_ is a minimal, fast WebSocket server implementation.
//!
//! [https://github.com/denoland/fastwebsockets](https://github.com/denoland/fastwebsockets)
//!
//! Passes the _Autobahn|TestSuite_ and fuzzed with LLVM's _libfuzzer_.
//!
//! You can use it as a raw websocket frame parser and deal with spec compliance yourself, or you can use it as a full-fledged websocket server.
//!
//! # Example
//!
//! ```
//! use tokio::net::TcpStream;
//! use fastwebsockets::{WebSocket, OpCode, Role};
//! use anyhow::Result;
//!
//! async fn handle(
//!   socket: TcpStream,
//! ) -> Result<()> {
//!   let mut ws = WebSocket::after_handshake(socket, Role::Server);
//!   ws.set_writev(false);
//!   ws.set_auto_close(true);
//!   ws.set_auto_pong(true);
//!
//!   loop {
//!     let frame = ws.read_frame().await?;
//!     match frame.opcode {
//!       OpCode::Close => break,
//!       OpCode::Text | OpCode::Binary => {
//!         ws.write_frame(frame).await?;
//!       }
//!       _ => {}
//!     }
//!   }
//!   Ok(())
//! }
//! ```
//!
//! ## Fragmentation
//!
//! By default, fastwebsockets will give the application raw frames with FIN set. Other
//! crates like tungstenite which will give you a single message with all the frames
//! concatenated.
//!
//! For concanated frames, use `FragmentCollector`:
//! ```
//! use fastwebsockets::{FragmentCollector, WebSocket, Role};
//! use tokio::net::TcpStream;
//! use anyhow::Result;
//!
//! async fn handle(
//!   socket: TcpStream,
//! ) -> Result<()> {
//!   let mut ws = WebSocket::after_handshake(socket, Role::Server);
//!   let mut ws = FragmentCollector::new(ws);
//!   let incoming = ws.read_frame().await?;
//!   // Always returns full messages
//!   assert!(incoming.fin);
//!   Ok(())
//! }
//! ```
//!
//! _permessage-deflate is not supported yet._
//!
//! ## HTTP Upgrades
//!
//! Enable the `upgrade` feature to do server-side upgrades and client-side
//! handshakes.
//!
//! This feature is powered by [hyper](https://docs.rs/hyper).
//!
//! ```
//! use fastwebsockets::upgrade::upgrade;
//! use http_body_util::Empty;
//! use hyper::{Request, body::{Incoming, Bytes}, Response};
//! use anyhow::Result;
//!
//! async fn server_upgrade(
//!   mut req: Request<Incoming>,
//! ) -> Result<Response<Empty<Bytes>>> {
//!   let (response, fut) = upgrade(&mut req)?;
//!
//!   tokio::spawn(async move {
//!     let ws = fut.await;
//!     // Do something with the websocket
//!   });
//!
//!   Ok(response)
//! }
//! ```
//!
//! Use the `handshake` module for client-side handshakes.
//!
//! ```
//! use fastwebsockets::handshake;
//! use fastwebsockets::FragmentCollector;
//! use hyper::{Request, body::Bytes, upgrade::Upgraded, header::{UPGRADE, CONNECTION}};
//! use http_body_util::Empty;
//! use hyper_util::rt::TokioIo;
//! use tokio::net::TcpStream;
//! use std::future::Future;
//! use anyhow::Result;
//!
//! async fn connect() -> Result<FragmentCollector<TokioIo<Upgraded>>> {
//!   let stream = TcpStream::connect("localhost:9001").await?;
//!
//!   let req = Request::builder()
//!     .method("GET")
//!     .uri("http://localhost:9001/")
//!     .header("Host", "localhost:9001")
//!     .header(UPGRADE, "websocket")
//!     .header(CONNECTION, "upgrade")
//!     .header(
//!       "Sec-WebSocket-Key",
//!       fastwebsockets::handshake::generate_key(),
//!     )
//!     .header("Sec-WebSocket-Version", "13")
//!     .body(Empty::<Bytes>::new())?;
//!
//!   let (ws, _) = handshake::client(&SpawnExecutor, req, stream).await?;
//!   Ok(FragmentCollector::new(ws))
//! }
//!
//! // Tie hyper's executor to tokio runtime
//! struct SpawnExecutor;
//!
//! impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
//! where
//!   Fut: Future + Send + 'static,
//!   Fut::Output: Send + 'static,
//! {
//!   fn execute(&self, fut: Fut) {
//!     tokio::task::spawn(fut);
//!   }
//! }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]

mod close;
mod error;
mod frame;
/// Client handshake.
#[cfg(feature = "upgrade")]
#[cfg_attr(docsrs, doc(cfg(feature = "upgrade")))]
pub mod handshake;
mod mask;
/// HTTP upgrades.
#[cfg(feature = "upgrade")]
#[cfg_attr(docsrs, doc(cfg(feature = "upgrade")))]
pub mod upgrade;
pub mod message_in;
pub mod controle_frame;
pub mod message_out;

use bytes::Buf;

pub use crate::close::CloseCode;
use crate::controle_frame::ControlFrame;
pub use crate::error::WebSocketError;
pub use crate::frame::Frame;
pub use crate::frame::OpCode;
pub use crate::frame::Payload;
pub use crate::mask::unmask;
use crate::message_in::{Message, MessageBuffer};
use crate::message_out::MessageOut;
use bytes::BytesMut;
use std::future::Future;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Copy, Clone, PartialEq)]
pub enum Role {
    Server,
    Client,
}

pub(crate) struct WriteHalf {
    role: Role,
    closed: bool,
    vectored: bool,
    auto_apply_mask: bool,
    writev_threshold: usize,
    write_buffer: Vec<u8>,
}

pub(crate) struct ReadHalf {
    role: Role,
    auto_apply_mask: bool,
    writev_threshold: usize,
    max_message_size: usize,
    buffer: BytesMut,
}

pub struct WebSocketRead {
    stream: OwnedReadHalf,
    read_half: ReadHalf,
}

pub struct WebSocketWrite {
    stream: OwnedWriteHalf,
    write_half: WriteHalf,
}

/// Create a split `WebSocketRead`/`WebSocketWrite` pair from a stream that has already completed the WebSocket handshake.
pub fn after_handshake_split(
    read: OwnedReadHalf,
    write: OwnedWriteHalf,
    role: Role,
) -> (WebSocketRead, WebSocketWrite)
{
    (
        WebSocketRead {
            stream: read,
            read_half: ReadHalf::after_handshake(role),
        },
        WebSocketWrite {
            stream: write,
            write_half: WriteHalf::after_handshake(role),
        },
    )
}

impl<'f> WebSocketRead {
    /// Consumes the `WebSocketRead` and returns the underlying stream.
    #[inline]
    pub(crate) fn into_parts_internal(self) -> (OwnedReadHalf, ReadHalf) {
        (self.stream, self.read_half)
    }

    pub fn set_writev_threshold(&mut self, threshold: usize) {
        self.read_half.writev_threshold = threshold;
    }

    /// Sets the maximum message size in bytes. If a message is received that is larger than this, the connection will be closed.
    ///
    /// Default: 64 MiB
    pub fn set_max_message_size(&mut self, max_message_size: usize) {
        self.read_half.max_message_size = max_message_size;
    }

    /// Sets whether to automatically apply the mask to the frame payload.
    ///
    /// Default: `true`
    pub fn set_auto_apply_mask(&mut self, auto_apply_mask: bool) {
        self.read_half.auto_apply_mask = auto_apply_mask;
    }

    /// Reads a WebSocket message, collecting fragmented frames until the final frame is received and returns the completed message.
    ///
    /// Text frames payload is guaranteed to be valid UTF-8.
    ///
    /// # Arguments
    ///
    /// * `control_frame_handler`: Closure that receives the control frames.
    pub async fn read_message<R, E>(
        &mut self,
        control_frame_handler: &mut impl FnMut(ControlFrame) -> R,
    ) -> Result<Message, WebSocketError>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
        R: Future<Output=Result<(), E>>,
    {
        let mut message_buffer = None;
        loop {
            let (res, control_frame) =
                self.read_half.read_frame_inner(&mut self.stream, &mut message_buffer).await;
            if let Some(frame) = control_frame {
                let res = control_frame_handler(frame).await;
                res.map_err(|e| WebSocketError::SendError(e.into()))?;
            }
            let done = res?;
            if done {
                return Ok(Message::from(message_buffer.take().unwrap()));
            }
        }
    }
}

impl<'f> WebSocketWrite { // TODO: Add a `write_vectored(&mut self, Vec<Bytes>)` method.
    /// Sets whether to use vectored writes. This option does not guarantee that vectored writes will be always used.
    ///
    /// Default: `true`
    pub fn set_writev(&mut self, vectored: bool) {
        self.write_half.vectored = vectored;
    }

    pub fn set_writev_threshold(&mut self, threshold: usize) {
        self.write_half.writev_threshold = threshold;
    }

    /// Sets whether to automatically apply the mask to the frame payload.
    ///
    /// Default: `true`
    pub fn set_auto_apply_mask(&mut self, auto_apply_mask: bool) {
        self.write_half.auto_apply_mask = auto_apply_mask;
    }

    pub fn is_closed(&self) -> bool {
        self.write_half.closed
    }

    pub async fn write_frame(
        &mut self,
        frame: Frame<'f>,
    ) -> Result<(), WebSocketError> {
        self.write_half.write_frame(&mut self.stream, frame).await
    }

    pub async fn write_message(&mut self, message: MessageOut) -> Result<(), WebSocketError> {
        self.write_half.write_message(&mut self.stream, message).await
    }

    pub async fn flush(&mut self) -> Result<(), WebSocketError> {
        flush(&mut self.stream).await
    }
}

#[inline]
async fn flush<S>(stream: &mut S) -> Result<(), WebSocketError>
where
    S: AsyncWrite + Unpin,
{
    stream.flush().await.map_err(WebSocketError::IoError)
}

/// WebSocket protocol implementation over an async stream.
pub struct WebSocket {
    stream: TcpStream,
    write_half: WriteHalf,
    read_half: ReadHalf,
}

impl<'f> WebSocket {
    /// Creates a new `WebSocket` from a stream that has already completed the WebSocket handshake.
    ///
    /// Use the `upgrade` feature to handle server upgrades and client handshakes.
    ///
    /// # Example
    ///
    /// ```
    /// use tokio::net::TcpStream;
    /// use fastwebsockets::{WebSocket, OpCode, Role};
    /// use anyhow::Result;
    ///
    /// async fn handle_client(
    ///   socket: TcpStream,
    /// ) -> Result<()> {
    ///   let mut ws = WebSocket::after_handshake(socket, Role::Server);
    ///   // ...
    ///   Ok(())
    /// }
    /// ```
    pub fn after_handshake(stream: TcpStream, role: Role) -> Self
    {
        Self {
            stream,
            write_half: WriteHalf::after_handshake(role),
            read_half: ReadHalf::after_handshake(role),
        }
    }

    /// Split a [`WebSocket`] into a [`WebSocketRead`] and [`WebSocketWrite`] half. Note that the split version does not
    /// handle fragmented packets and you may wish to create a [`FragmentCollectorRead`] over top of the read half that
    /// is returned.
    pub fn split(
        self
    ) -> (WebSocketRead, WebSocketWrite)
    {
        let (stream, read, write) = self.into_parts_internal();
        let (r, w) = stream.into_split();
        (
            WebSocketRead {
                stream: r,
                read_half: read,
            },
            WebSocketWrite {
                stream: w,
                write_half: write,
            },
        )
    }

    /// Consumes the `WebSocket` and returns the underlying stream.
    #[inline]
    pub fn into_inner(self) -> TcpStream {
        // self.write_half.into_inner().stream
        self.stream
    }

    /// Consumes the `WebSocket` and returns the underlying stream.
    #[inline]
    pub(crate) fn into_parts_internal(self) -> (TcpStream, ReadHalf, WriteHalf) {
        (self.stream, self.read_half, self.write_half)
    }

    /// Sets whether to use vectored writes. This option does not guarantee that vectored writes will be always used.
    ///
    /// Default: `true`
    pub fn set_writev(&mut self, vectored: bool) {
        self.write_half.vectored = vectored;
    }

    pub fn set_writev_threshold(&mut self, threshold: usize) {
        self.read_half.writev_threshold = threshold;
        self.write_half.writev_threshold = threshold;
    }

    /// Sets the maximum message size in bytes. If a message is received that is larger than this, the connection will be closed.
    ///
    /// Default: 64 MiB
    pub fn set_max_message_size(&mut self, max_message_size: usize) {
        self.read_half.max_message_size = max_message_size;
    }

    /// Sets whether to automatically apply the mask to the frame payload.
    ///
    /// Default: `true`
    pub fn set_auto_apply_mask(&mut self, auto_apply_mask: bool) {
        self.read_half.auto_apply_mask = auto_apply_mask;
        self.write_half.auto_apply_mask = auto_apply_mask;
    }

    pub fn is_closed(&self) -> bool {
        self.write_half.closed
    }
}

const MAX_HEADER_SIZE: usize = 14;

impl ReadHalf {
    pub fn after_handshake(role: Role) -> Self {
        let buffer = BytesMut::with_capacity(8192);

        Self {
            role,
            auto_apply_mask: true,
            writev_threshold: 1024,
            max_message_size: 64 << 20,
            buffer,
        }
    }

    /// Attempt to read a single frame from the incoming stream, returning any send obligations if
    /// `auto_close` or `auto_pong` are enabled. Callers to this function are obligated to send the
    /// frame in the latter half of the tuple if one is specified, unless the write half of this socket
    /// has been closed.
    ///
    /// XXX: Do not expose this method to the public API.
    pub(crate) async fn read_frame_inner<'f, 'c, S>(
        &mut self,
        stream: &mut S,
        message_buffer: &mut Option<MessageBuffer>,
    ) -> (Result<bool, WebSocketError>, Option<ControlFrame>)
    where
        S: AsyncRead + Unpin,
    {
        let mut frame = match self.parse_frame_header(stream, message_buffer).await {
            Ok(frame) => frame,
            Err(e) => return (Err(e), None),
        };

        if self.role == Role::Server && self.auto_apply_mask {
            frame.unmask()
        };

        match frame.opcode {
            OpCode::Close => {
                let obligated_send = ControlFrame::Close(frame.payload.extract_owned());
                (Ok(false), Some(obligated_send))
            }
            OpCode::Ping => {
                (Ok(false), Some(ControlFrame::Ping(frame.payload.extract_owned())))
            }
            OpCode::Pong => {
                (Ok(false), Some(ControlFrame::Pong(frame.payload.extract_owned())))
            }
            _ => (Ok(frame.fin), None),
        }
    }

    async fn parse_frame_header<'a, S>(
        &mut self,
        stream: &mut S,
        message_buffer: &'a mut Option<MessageBuffer>,
    ) -> Result<Frame<'a>, WebSocketError>
    where
        S: AsyncRead + Unpin,
    {
        macro_rules! eof {
      ($n:expr) => {{
        if $n == 0 {
          return Err(WebSocketError::UnexpectedEOF);
        }
      }};
    }

        // Read the first two bytes
        // while self.buffer.remaining() < 2 {
        //     eof!(stream.read_buf(&mut self.buffer).await?);
        // }
        let mut first_bytes: [u8; 2] = [0; 2];
        eof!(stream.read_exact(&mut first_bytes).await?);
        let first_byte = first_bytes[0];
        let second_byte = first_bytes[1];

        let fin = first_byte & 0b10000000 != 0;
        let rsv = first_byte & 0b01110000 != 0;

        if rsv {
            return Err(WebSocketError::ReservedBitsNotZero);
        }

        let opcode = frame::OpCode::try_from(first_byte & 0b00001111)?;
        let masked = second_byte & 0b10000000 != 0;

        let length_code = second_byte & 0x7F;
        let extra = match length_code {
            126 => 2,
            127 => 8,
            _ => 0,
        };

        // self.buffer.advance(2);
        // while self.buffer.remaining() < extra + masked as usize * 4 {
        //     eof!(stream.read_buf(&mut self.buffer).await?);
        // }

        let payload_len: usize = match extra {
            0 => usize::from(length_code),
            2 => {
                let mut extra_bytes: [u8; 2] = [0; 2];
                eof!(stream.read_exact(&mut extra_bytes).await?);
                u16::from_be_bytes(extra_bytes) as usize
            }
            #[cfg(target_pointer_width = "64")]
            8 => {
                let mut extra_bytes: [u8; 8] = [0; 8];
                eof!(stream.read_exact(&mut extra_bytes).await?);
                u64::from_be_bytes(extra_bytes) as usize
            }
            // On 32bit systems, usize is only 4bytes wide so we must check for usize overflowing
            #[cfg(any(target_pointer_width = "16", target_pointer_width = "32"))]
            8 => {
                let mut extra_bytes: [u8; 8] = [0; 8];
                eof!(stream.read_exact(&mut extra_bytes).await?);
                match usize::try_from(u64::from_be_bytes(extra_bytes)) {
                    Ok(length) => length,
                    Err(_) => return Err(WebSocketError::FrameTooLarge),
                }
            }
            _ => unreachable!(),
        };

        let mask = if masked {
            Some({
                let mut mask_bytes: [u8; 4] = [0; 4];
                eof!(stream.read_exact(&mut mask_bytes).await?);
                mask_bytes
            })
        } else {
            None
        };

        if frame::is_control(opcode) {
            if !fin {
                return Err(WebSocketError::ControlFrameFragmented);
            }

            if opcode == OpCode::Ping && payload_len > 125 {
                return Err(WebSocketError::ControlFrameTooLarge);
            }
        }

        if payload_len >= self.max_message_size {
            return Err(WebSocketError::FrameTooLarge);
        }

        // // Reserve a bit more to try to get next frame header and avoid a syscall to read it next time
        // self.buffer.reserve(payload_len + MAX_HEADER_SIZE);
        // while payload_len > self.buffer.remaining() {
        //     eof!(stream.read_buf(&mut self.buffer).await?);
        // }

        if frame::is_control(opcode) {
            let mut payload = Vec::with_capacity(payload_len);
            eof!(stream.read_exact(&mut payload).await?);
            Ok(Frame::new(fin, opcode, mask, Payload::Owned(payload)))
        } else {
            if let Some(current_message_buffer) = message_buffer {
                let inner = current_message_buffer.get_inner();
                let len = inner.len();
                inner.resize(len + payload_len, 0);
                let current_message_buffer_slice = &mut current_message_buffer.get_inner()[len..];
                eof!(stream.read_exact(current_message_buffer_slice).await?);
                Ok(Frame::new(fin, opcode, mask, Payload::BorrowedMut(current_message_buffer_slice)))
            } else {
                let current_message_buffer = message_buffer.insert(MessageBuffer::with_capacity(opcode, payload_len));
                let inner = current_message_buffer.get_inner();
                let current_message_buffer_slice = &mut inner[..];
                eof!(stream.read_exact(current_message_buffer_slice).await?);
                Ok(Frame::new(fin, opcode, mask, Payload::BorrowedMut(&mut inner[..])))
            }
        }
    }
}

impl WriteHalf {
    pub fn after_handshake(role: Role) -> Self {
        Self {
            role,
            closed: false,
            auto_apply_mask: true,
            vectored: true,
            writev_threshold: 1024,
            write_buffer: Vec::with_capacity(2),
        }
    }

    /// Writes a frame to the provided stream.
    pub async fn write_frame<'a, S>(
        &'a mut self,
        stream: &mut S,
        mut frame: Frame<'a>,
    ) -> Result<(), WebSocketError>
    where
        S: AsyncWrite + Unpin,
    {
        if self.role == Role::Client && self.auto_apply_mask {
            frame.mask();
        }

        if self.closed {
            if frame.opcode == OpCode::Close {
                return Ok(()); // Already sent close, this is a no-op
            }
            return Err(WebSocketError::ConnectionClosed);
        }
        let is_close = frame.opcode == OpCode::Close;
        if is_close {
            self.closed = true;
        }

        if self.vectored && frame.payload.len() > self.writev_threshold {
            frame.writev(stream).await?;
        } else {
            let text = frame.write(&mut self.write_buffer);
            stream.write_all(text).await?;
        }

        Ok(())
    }

    pub async fn write_message(
        &mut self,
        stream: &mut OwnedWriteHalf,
        mut message: MessageOut,
    ) -> Result<(), WebSocketError>
    {
        if self.closed {
            if message.is_close() {
                return Ok(()); // Already sent close, this is a no-op
            }
            return Err(WebSocketError::ConnectionClosed);
        }
        if message.is_close() {
            self.closed = true;
        }

        if !message.is_fragmented() {
            let mut frame = message.to_single_frame();
            frame.writev(stream).await?;
            Ok(())
        } else {
            let header = message.build_header_for_fragmented_message();
            let slices = message.fragmented_to_slices(&header);

            let full_len = slices.iter().map(|slice| slice.len()).sum::<usize>();
            let sent_bytes = stream.write_vectored(&slices).await?;
            if sent_bytes != full_len {
                return Err(WebSocketError::SendError(format!(
                    "Failed to send all bytes of fragmented message: sent {} of {}",
                    sent_bytes, full_len
                ).into()));
            }

            Ok(())
        }
    }
}
