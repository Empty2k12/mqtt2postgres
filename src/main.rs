#![feature(ascii_char)]

mod error;
mod query;
mod vars;

pub use error::Error;
pub use query::{create_table::CreateTable, Query, QueryType, ValidQuery};

use rumqttc::v5::mqttbytes::QoS;

use slugify::slugify;

use rumqttc::v5::{AsyncClient, MqttOptions};
use std::time::Duration;

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::Event;

use bytes::Bytes;

use tokio_postgres::NoTls;

use serde_json::Value;
use std::collections::HashMap;

use serde_json::Value::Bool;
use serde_json::Value::Number;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", &vars::mqtt_broker_ip(), 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_max_packet_size(Some(100000));

    println!("Connecting to MQTT broker at {}", &vars::mqtt_broker_ip());

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    mqtt_client
        .subscribe("#", QoS::AtMostOnce) // zigbee2mqtt/#
        .await
        .unwrap();

    let (client, connection) = tokio_postgres::connect(
        "postgresql://postgres:postgres@localhost:5432/mqtt2postgres",
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => match v {
                Event::Incoming(Packet::Publish(publish)) => {
                    let topic = slugify_topic(&publish.topic);

                    if topic.len() >= 2
                        && topic[1] != "bridge"
                        && topic[0] != "homeassistant"
                        && !publish.dup
                        // TODO: create / read schema for retained messages, but don't write entries at startup
                        && !publish.retain
                    {
                        let table_name = topic.join("_"); // FIXME: turn back to . and make use of postgres schemata

                        let schema_query = CreateTable::new(&table_name, &publish.payload).build();

                        match schema_query {
                            Ok(schema_query) => {
                                // TODO: keep a copy of them schema in RAM; if it is unchanged, do not submit this query
                                let _table = client.query(&schema_query.get(), &[]).await?;

                                let insert = build_insert_query(&table_name, &publish.payload);
                                println!("insert: {:?}", insert);
                                let _insertresult = client.query(&insert.unwrap(), &[]).await?;
                            }
                            Err(_) => {}
                        }

                        // let rows = client
                        //     .query("SELECT $1::TEXT", &[&"hello world"])
                        //     .await?;
                    }
                }
                _ => {}
            },
            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}

fn build_insert_query(table_name: &String, payload: &Bytes) -> anyhow::Result<String> {
    if payload.is_json() {
        let m: HashMap<String, Value> = serde_json::from_slice(payload)?;

        let mut keys = Vec::with_capacity(m.len());
        let mut values = Vec::with_capacity(m.len());

        for (k, v) in m {
            let datatype = extract_datatype(&v);

            // TODO: refactor to use Some/None and to support nested objects
            if datatype != "other" {
                keys.push(k);
                if v.is_string() {
                    values.push(format!(r#"'{}'"#, v.as_str().unwrap()));
                } else {
                    values.push(v.to_string());
                }
            }
        }

        return Ok(format!(
            "INSERT INTO {} ({}) VALUES ({});",
            table_name,
            keys.join(", "),
            values.join(", ")
        ));
    } else {
        return Ok(format!(
            "INSERT INTO {} ({}) VALUES ('{}');",
            table_name,
            table_name,
            payload.escape_ascii().to_string()
        ));
    }
}

fn slugify_topic(topic: &Bytes) -> Vec<String> {
    let parts = topic.escape_ascii().to_string();
    parts
        .split("/")
        .map(|part| slugify!(part, separator = "_"))
        .collect::<Vec<String>>()
}

fn extract_datatype(value: &Value) -> &str {
    match value {
        Number(_) => "numeric",
        Bool(_) => "boolean",
        serde_json::Value::String(_) => "text",
        // TODO: how to handle this properly?
        serde_json::Value::Null => "text",
        _ => "other",
    }
}

trait IsJson {
    fn is_json(&self) -> bool;
}

impl IsJson for Bytes {
    fn is_json(&self) -> bool {
        return self.first() == Some(&b"{"[0]) && self.last() == Some(&b"}"[0]);
    }
}