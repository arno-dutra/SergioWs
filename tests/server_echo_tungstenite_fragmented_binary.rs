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

use anyhow::Result;
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper::Response;
use hyper_util::rt::TokioIo;
use sergio_ws::{upgrade, Payload};
use sergio_ws::{Frame, WebSocket};
use tokio::net::TcpListener;

use hyper::header::CONNECTION;
use hyper::header::UPGRADE;
use sergio_ws::handshake;
use sergio_ws::WebSocketRead;
use sergio_ws::WebSocketWrite;
use tokio::sync::Mutex;

use sergio_ws::controle_frame::ControlFrame;
use sergio_ws::message_in::Message;
use sergio_ws::message_out::MessageOut;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use tokio::net::TcpStream;

const N_CLIENTS: usize = 1;

async fn handle_client(
    client_id: usize,
    fut: upgrade::UpgradeFut,
) -> Result<()> {
    let mut ws = fut.await?;
    ws.set_writev(false);
    let (mut r, w) = ws.split();


    let w = Arc::new(Mutex::new(w));
    let cloned_w = w.clone();
    let message = r
        .read_message(&mut move |frame| {
            let w = cloned_w.clone();
            async move {
                match frame {
                    ControlFrame::Ping(payload) => w.lock().await.write_frame(Frame::pong(Payload::Owned(payload))).await,
                    ControlFrame::Pong(_) => Ok(()),
                    ControlFrame::Close(payload) => w.lock().await.write_frame(Frame::close_raw(Payload::Owned(payload))).await,
                }
            }
        })
        .await?;
    match message {
        Message::Binary(payload) => {
            w.lock().await.write_message(MessageOut::Binary(payload.to_vec())).await.unwrap();
        }
        _ => {
            panic!("Unexpected");
        }
    }

    Ok(())
}

async fn server_upgrade(
    mut req: Request<Incoming>,
) -> Result<Response<Empty<Bytes>>> {
    let (response, fut) = upgrade::upgrade(&mut req)?;

    let client_id: usize = req
        .headers()
        .get("CLIENT-ID")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    tokio::spawn(async move {
        handle_client(client_id, fut).await.unwrap();
    });

    Ok(response)
}

async fn connect(
    client_id: usize,
) -> Result<(
    WebSocketRead,
    WebSocketWrite,
)> {
    let stream = TcpStream::connect("localhost:9001").await?;

    let req = Request::builder()
        .method("GET")
        .uri("http://localhost:9001/")
        .header("Host", "localhost:9001")
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("CLIENT-ID", &format!("{}", client_id))
        .header(
            "Sec-WebSocket-Key",
            sergio_ws::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())?;

    let tcp_stream = handshake::client(&SpawnExecutor, req, stream).await?;
    let ws = WebSocket::after_handshake(tcp_stream, sergio_ws::Role::Client);
    Ok(ws.split())
}

async fn start_client(client_id: usize) -> Result<()> {
    let (mut r, w) = connect(client_id).await?;
    let w = Rc::new(Mutex::new(w));

    let payload = vec![bytes::Bytes::from_static(b"prefix["), bytes::Bytes::from(u64::try_from(client_id).unwrap().to_be_bytes().to_vec()), bytes::Bytes::from_static(b"]suffix")];
    // w.lock().await.write_frame(Frame::binary(Payload::Owned(flat_copy(&payload)))).await;
    w.lock().await.write_message(MessageOut::FragmentedBinary(payload)).await.unwrap();

    let message = r
        .read_message(&mut move |frame| {
            let w = w.clone();
            async move {
                match frame {
                    ControlFrame::Ping(payload) => w.lock().await.write_frame(Frame::pong(Payload::Owned(payload))).await,
                    ControlFrame::Pong(_) => Ok(()),
                    ControlFrame::Close(payload) => w.lock().await.write_frame(Frame::close_raw(Payload::Owned(payload))).await,
                }
            }
        })
        .await?;
    match message {
        Message::Binary(payload) => {
            assert_eq!(payload, vec![b"prefix[".to_vec(), client_id.to_be_bytes().to_vec(), b"]suffix".to_vec()].concat());
        }
        _ => {
            panic!("Unexpected");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn server_echo_tungstenite_fragmented_binary() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server started, listening on {}", "127.0.0.1:8080");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let conn_fut = http1::Builder::new()
                    .serve_connection(io, service_fn(server_upgrade))
                    .with_upgrades();
                conn_fut.await.unwrap();
            });
        }
    });
    let mut tasks = Vec::with_capacity(N_CLIENTS);
    for client in 0..N_CLIENTS {
        tasks.push(start_client(client));
    }
    for handle in tasks {
        handle.await.unwrap();
    }
    Ok(())
}

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

pub(crate) fn flat_copy(src: &[Bytes]) -> Vec<u8> {
    let mut flatten_data: Vec<u8> = vec![];
    for chunk in src {
        flatten_data.extend_from_slice(&chunk);
    }
    flatten_data
}