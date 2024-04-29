// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::Box;
use std::pin::Pin;
use std::result::Result;
use std::sync::Arc;

use arc_cell::ArcCell;
use bazel_bep::server::{PublishBuildEvent, PublishBuildEventServer};
use bazel_bep::types::google::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
};
use tonic::{Request, Response, Status, Streaming};
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::stream::Stream;
use futures::Future;
use nativelink_error::{make_err, Code, Error, ResultExt};
use redis::aio::{ConnectionLike, ConnectionManager};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;
use tracing::{error_span, Instrument};

pub struct BepServer<T: ConnectionLike + Unpin + Clone + Send + Sync = ConnectionManager> {
    redis_conn: ArcCell<LazyConnection<T>>,
}

pub enum LazyConnection<T: ConnectionLike + Unpin + Clone + Send + Sync> {
    Connection(Result<T, Error>),
    Future(Shared<BoxFuture<'static, Result<T, Error>>>),
}

impl BepServer {
    pub fn new(
        redis_url: &str,
    ) -> Result<PublishBuildEventServer<BepServer<ConnectionManager>>, redis::RedisError> {
        let redis_url = redis_url.to_owned();
        let conn_fut = async move {
            redis::Client::open(redis_url)
                .map_err(from_redis_err)?
                .get_connection_manager()
                .await
                .map_err(from_redis_err)
        }
        .boxed()
        .shared();

        let conn_fut_clone = conn_fut.clone();
        // Start connecting to redis, but don't block our construction on it.
        tokio::spawn(
            async move {
                if let Err(e) = conn_fut_clone.await {
                    make_err!(Code::Unavailable, "Failed to connect to Redis: {:?}", e);
                }
            }
            .instrument(error_span!("redis_initial_connection")),
        );

        let lazy_conn = LazyConnection::Future(conn_fut);

        Ok(PublishBuildEventServer::new(BepServer {
            redis_conn: ArcCell::new(Arc::new(lazy_conn)),
        }))
    }
}

impl<T: ConnectionLike + Unpin + Clone + Send + Sync + 'static> BepServer<T> {
    async fn get_conn(&self) -> Result<T, Error> {
        let result = match self.redis_conn.get().as_ref() {
            LazyConnection::Connection(conn_result) => return conn_result.clone(),
            LazyConnection::Future(fut) => fut.clone().await,
        };
        self.redis_conn
            .set(Arc::new(LazyConnection::Connection(result.clone())));
        result
    }
}

#[async_trait]
impl PublishBuildEvent for BepServer {
    type PublishBuildToolEventStreamStream = Pin<
        Box<
            dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    fn publish_lifecycle_event<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<PublishLifecycleEventRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<tonic::Response<()>, tonic::Status>> + Send>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
    {
        Box::pin(async move {
            println!("Received lifecycle event: {:?}", request.get_ref());
            Ok(tonic::Response::new(()))
        })
    }

    async fn publish_build_tool_event_stream(
        &self,
        grpc_request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
            let mut conn = self.get_conn().await?;
            let mut pipe = redis::pipe();
            pipe.atomic();

            let mut stream = grpc_request.into_inner();

            let (tx, rx) = mpsc::channel(128);

            tokio::spawn(async move {
                while let Some(req) = stream.message().await.unwrap() {
                    println!("Received build event: {:?}", req);

                    let build_event = req.ordered_build_event.unwrap();

                    let build_id = build_event.clone().stream_id.unwrap().build_id;

                    let key = format!("{}-{}", build_id, build_event.sequence_number);

                    let ack = PublishBuildToolEventStreamResponse {
                        stream_id: build_event.clone().stream_id,
                        sequence_number: build_event.sequence_number,
                    };

                    pipe.cmd("SET").arg(key).arg(format!("{:?}", build_event));

                    tx.send(Ok(ack)).await.unwrap();
                }

                println!("Flushing pipeline");

                pipe.query_async::<_, Vec<usize>>(&mut conn)
                    .await
                    .map_err(from_redis_err)
                    .err_tip(|| "In RedisStore::update::query_async")
                    .unwrap();
                pipe.clear();
            });

            Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn from_redis_err(call_res: redis::RedisError) -> Error {
    make_err!(Code::Internal, "Redis Error: {call_res}")
}
