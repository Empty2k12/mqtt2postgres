#![feature(ascii_char)]
#![allow(clippy::needless_return, clippy::single_match)]

mod config;
mod error;
mod helpers;
mod query;

use anyhow::Context;
pub use config::Config;
pub use error::Error;
pub use helpers::IsJson;
pub use query::{create_table::CreateTable, Query, QueryType, ValidQuery};
use query::{insert_record::InsertRecord, pg_datatype::PGDatatype};

use rumqttc::v5::mqttbytes::QoS;

use slugify::slugify;

use rumqttc::v5::{AsyncClient, MqttOptions};
use std::{
    collections::{HashMap, HashSet},
    fs,
    time::Duration
};

use rumqttc::v5::{mqttbytes::v5::Packet, Event};

use bytes::Bytes;

use tokio_postgres::{Client, NoTls};

use tracing::info;

pub type KeyValueType = (String, PGDatatype);
pub type KnownTableSchema = HashSet<(String, PGDatatype)>;
pub type KnownTableSchemata = HashMap<String, KnownTableSchema>;

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
    mqttoptions
        .set_keep_alive(Duration::from_secs(config.mqtt.keep_alive_seconds.into()));
    mqttoptions.set_max_packet_size(Some(config.mqtt.max_packet_size.into()));

    info!(
        broker_ip = &config.mqtt.broker_ip,
        broker_port = config.mqtt.broker_port,
        client_name = &config.mqtt.client_name,
        "Connecting to MQTT"
    );

    if config.postgres.timescale {
        info!("Timescale Integration is enabled!");
    }

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    for subscribe in config.topics.subscribe {
        mqtt_client
            .subscribe(subscribe.topic, QoS::AtMostOnce)
            .await
            .unwrap();
    }

    let (mut client, connection) =
        tokio_postgres::connect(&config.postgres.connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mut known_schemata: KnownTableSchemata = HashMap::new();

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => match v {
                Event::Incoming(Packet::Publish(publish)) => {
                    handle_publish(&mut client, publish, &mut known_schemata).await?
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

#[tracing::instrument(name = "handle_publish", skip(client, publish, known_schemata))]
async fn handle_publish(
    client: &mut Client,
    publish: &rumqttc::v5::mqttbytes::v5::Publish,
    known_schemata: &mut KnownTableSchemata
) -> anyhow::Result<()> {
    let topic = slugify_topic(&publish.topic);

    if topic.len() >= 2
        // TODO: use config provided ignores
        && topic[1] != "bridge"
        && topic[0] != "homeassistant"
        && !publish.dup
        // TODO: create / read schema for retained messages, but don't write entries at startup
        && !publish.retain
    {
        let table_name = topic.join("_"); // FIXME: turn back to . and make use of postgres schemata

        create_table(client, publish, &table_name, known_schemata).await?;
        insert_row(client, publish, &table_name, known_schemata).await?;
    }

    Ok(())
}

#[tracing::instrument(name = "create_table", skip(client, publish, known_schemata))]
async fn create_table(
    client: &mut Client,
    publish: &rumqttc::v5::mqttbytes::v5::Publish,
    table_name: &String,
    known_schemata: &mut KnownTableSchemata
) -> anyhow::Result<()> {
    let schema_query =
        CreateTable::new(table_name, &publish.payload).build(known_schemata)?;

    // TODO: keep a copy of the schema in RAM; if it is unchanged, do not submit this query

    let transaction = client.transaction().await?;
    for query in schema_query {
        transaction.query(&query.get(), &[]).await?;
    }
    transaction.commit().await?;

    return Ok(());
}

#[tracing::instrument(name = "insert_row", skip(client, publish, known_schemata))]
async fn insert_row(
    client: &mut Client,
    publish: &rumqttc::v5::mqttbytes::v5::Publish,
    table_name: &String,
    known_schemata: &mut KnownTableSchemata
) -> anyhow::Result<()> {
    let insert_record =
        InsertRecord::new(table_name, &publish.payload).build(known_schemata)?;

    let transaction = client.transaction().await?;
    for query in insert_record {
        transaction.query(&query.get(), &[]).await?;
    }
    transaction.commit().await?;

    return Ok(());
}

fn slugify_topic(topic: &Bytes) -> Vec<String> {
    let parts = topic.escape_ascii().to_string();
    parts
        .split("/")
        .map(|part| slugify!(part, separator = "_"))
        .collect::<Vec<String>>()
}
