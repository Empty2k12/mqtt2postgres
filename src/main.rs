#![feature(ascii_char)]

mod error;
mod query;
mod vars;
mod helpers;

pub use error::Error;
use query::insert_record::InsertRecord;
pub use query::{create_table::CreateTable, Query, QueryType, ValidQuery};
pub use helpers::IsJson;

use rumqttc::v5::mqttbytes::QoS;

use slugify::slugify;

use rumqttc::v5::{AsyncClient, MqttOptions};
use std::time::Duration;

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::Event;

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
                Event::Incoming(Packet::Publish(publish)) => handle_publish(&client, publish).await?,
                _ => {}
            },
            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}

#[tracing::instrument(
    name = "handle_publish",
    skip(client, publish),
)]
async fn handle_publish(client: &Client, publish: &rumqttc::v5::mqttbytes::v5::Publish) -> anyhow::Result<()> {
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
                    let _insertresult =
                        client.query(&insert_record.get(), &[]).await?;
                    
                    info!(table_name = &table_name);
                }
            }
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