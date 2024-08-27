#![feature(ascii_char)]

use rumqttc::v5::mqttbytes::QoS;
use tokio::task;

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
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_max_packet_size(Some(100000));

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    mqtt_client
        .subscribe("zigbee2mqtt/#", QoS::AtMostOnce)
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

                    if topic.len() >= 2 && topic[1] != "bridge" && !publish.dup {
                        let table_name = topic.join("_"); // FIXME: turn back to . and make use of postgres schemata
                                                          // println!("Properties = {:?}", publish.properties);

                        let schema = build_schema_from_payload(&table_name, &publish.payload);

                        // let rows = client
                        //     .query("SELECT $1::TEXT", &[&"hello world"])
                        //     .await?;

                        match schema {
                            Ok(schema) => {
                                // TODO: keep a copy of them schema in RAM; if it is unchanged, do not submit this query
                                let _table = client.query(&schema, &[]).await?;

                                let insert = build_insert_query(&table_name, &publish.payload);
                                let _insertresult = client.query(&insert.unwrap(), &[]).await?;
                            }
                            Err(e) => println!("Error: {:?}", e),
                        }
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

fn build_schema_from_payload(table_name: &String, payload: &Bytes) -> anyhow::Result<String> {
    let m: HashMap<String, Value> = serde_json::from_slice(payload)?;

    let mut fields = Vec::with_capacity(m.len());

    for (k, v) in m {
        let datatype = extract_datatype(&v);

        // TODO: refactor to use Some/None and to support nested objects
        if datatype != "other" {
            fields.push(format!("{} {}", k, datatype));
        }
    }

    return Ok(format!("CREATE TABLE IF NOT EXISTS {} (timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, {});", table_name, fields.join(", ")));
}

fn build_insert_query(table_name: &String, payload: &Bytes) -> anyhow::Result<String> {
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

fn slugify_topic(topic: &Bytes) -> Vec<String> {
    let parts = topic.escape_ascii().to_string();
    parts
        .split("/")
        .map(|part| slugify!(part, separator = "_"))
        .collect::<Vec<String>>()
}
