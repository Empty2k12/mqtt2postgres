#![feature(ascii_char)]

mod config;
mod error;
mod helpers;
mod query;

use anyhow::Context;
pub use config::Config;
pub use error::Error;
pub use helpers::IsJson;
use query::insert_record::InsertRecord;
pub use query::{create_table::CreateTable, Query, QueryType, ValidQuery};

use rumqttc::v5::mqttbytes::QoS;

use slugify::slugify;

use rumqttc::v5::{AsyncClient, MqttOptions};
use std::{fs, time::Duration};

use rumqttc::v5::{mqttbytes::v5::Packet, Event};

use bytes::Bytes;

use tokio_postgres::{Client, NoTls};

use tracing::info;
use tracing_subscriber;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_target(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let config = fs::read("./config.toml")
        .with_context(|| format!("Failed to read {:?}", "./config.toml"))
        .unwrap_or_else(|err| panic!("{err:?}"));
    let config: Config = toml::from_slice(&config)
        .with_context(|| "Failed to deserialize config")
        .unwrap_or_else(|err| panic!("{err:?}"));

    let mut mqttoptions = MqttOptions::new(
        &config.mqtt.client_name,
        &config.mqtt.broker_ip,
        config.mqtt.broker_port
    );
    mqttoptions.set_keep_alive(Duration::from_secs(config.mqtt.keep_alive_seconds.into()));
    mqttoptions.set_max_packet_size(Some(config.mqtt.max_packet_size.into()));

    info!(broker_ip = &config.mqtt.broker_ip, broker_port = config.mqtt.broker_port, client_name = &config.mqtt.client_name, "Connecting to MQTT");

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    for subscribe in config.topics.subscribe {
        mqtt_client
            .subscribe(subscribe.topic, QoS::AtMostOnce)
            .await
            .unwrap();
    }

    let (client, connection) = tokio_postgres::connect(
        &config.postgres.connection_string,
        NoTls
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
                    handle_publish(&client, publish).await?
                },
                _ => {}
            },
            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}

#[tracing::instrument(name = "handle_publish", skip(client, publish))]
async fn handle_publish(
    client: &Client,
    publish: &rumqttc::v5::mqttbytes::v5::Publish
) -> anyhow::Result<()> {
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

                if let Ok(insert_record) =
                    InsertRecord::new(&table_name, &publish.payload).build()
                {
                    let query = &insert_record.get();
                    let insert_record2 = client.query(query, &[]).await;

                    if insert_record2.is_err() {
                        println!("{:?}", query);
                    }

                    info!(table_name = &table_name);
                }
            },
            Err(_) => {}
        }
    }

    Ok(())
}

fn slugify_topic(topic: &Bytes) -> Vec<String> {
    let parts = topic.escape_ascii().to_string();
    parts
        .split("/")
        .map(|part| slugify!(part, separator = "_"))
        .collect::<Vec<String>>()
}
