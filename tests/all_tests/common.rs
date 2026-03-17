use bytes::{Buf, BufMut, BytesMut};
use proto_kafka::protocol::encode_request_header_into_buffer;
use proto_kafka::{
    messages::{RequestHeader, ResponseHeader},
    protocol::{Decodable, Encodable, HeaderVersion},
};
use std::{
    io::{Read, Write},
    net::TcpStream,
};
use testcontainers::{
    core::{wait::LogWaitStrategy, ContainerPort, WaitFor},
    runners::SyncRunner,
    Container, GenericImage, ImageExt,
};

pub fn start_kafka() -> Container<GenericImage> {
    GenericImage::new("bitnamilegacy/kafka", "3.6.1-debian-11-r24")
        .with_exposed_port(ContainerPort::Tcp(9092))
        .with_wait_for(WaitFor::Log(LogWaitStrategy::stdout(
            b"Kafka Server started",
        )))
        .with_env_var("KAFKA_CFG_LISTENERS", "BROKER://:9092,CONTROLLER://:9093")
        .with_env_var("KAFKA_CFG_ADVERTISED_LISTENERS", "BROKER://127.0.0.1:9092")
        .with_env_var(
            "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT",
        )
        .with_env_var("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "BROKER")
        .with_env_var("KAFKA_CFG_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_CFG_PROCESS_ROLES", "controller,broker")
        .with_env_var("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS", "0@localhost:9093")
        .with_env_var("KAFKA_CFG_NODE_ID", "0")
        .with_env_var("ALLOW_PLAINTEXT_LISTENER", "yes")
        .start()
        .unwrap()
}

pub fn connect_to_kafka(container: &Container<GenericImage>) -> TcpStream {
    let host = container.get_host().unwrap();
    let port = container.get_host_port_ipv4(9092).unwrap();
    TcpStream::connect((host.to_string(), port)).unwrap()
}

pub fn send_request<T: Encodable + HeaderVersion>(
    socket: &mut TcpStream,
    header: RequestHeader,
    body: T,
) {
    let mut bytes = BytesMut::new();

    encode_request_header_into_buffer(&mut bytes, &header).unwrap();
    body.encode(&mut bytes, header.request_api_version).unwrap();

    let size = bytes.len() as i32;
    socket.write_all(&size.to_be_bytes()).unwrap();
    socket.write_all(&bytes).unwrap();
}

pub fn receive_response<T: Decodable + HeaderVersion>(
    socket: &mut TcpStream,
    version: i16,
) -> (ResponseHeader, T) {
    let mut buffer = BytesMut::new();

    let message_size = loop {
        read(socket, &mut buffer);
        if buffer.len() >= 4 {
            break buffer.get_u32();
        }
    };

    loop {
        if buffer.len() == message_size as usize {
            return (
                ResponseHeader::decode(&mut buffer, T::header_version(version)).unwrap(),
                T::decode(&mut buffer, version).unwrap(),
            );
        }
        read(socket, &mut buffer);
    }
}

fn read(socket: &mut TcpStream, dest: &mut BytesMut) {
    let mut tmp = [0; 1000];
    let read = socket.read(&mut tmp).unwrap();
    dest.put_slice(&tmp[..read]);
}
