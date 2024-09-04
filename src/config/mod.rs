use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub mqtt: MqttConfig,
    pub postgres: PostgresConfig,
    pub topics: TopicsConfig
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    pub broker_ip: String,
    pub broker_port: u16,
    pub client_name: String,

    #[serde(default)]
    pub keep_alive_seconds: KeepAliveSeconds,

    #[serde(default)]
    pub max_packet_size: MaxPacketSize
}

/// Keep alive in seconds.
#[derive(Deserialize, Debug)]
pub struct KeepAliveSeconds(u64);
impl Default for KeepAliveSeconds {
    fn default() -> Self {
        KeepAliveSeconds(5)
    }
}
impl Into<u64> for KeepAliveSeconds {
    fn into(self) -> u64 {
        return self.0;
    }
}

/// Keep alive in seconds.
#[derive(Deserialize, Debug)]
pub struct MaxPacketSize(u32);
impl Default for MaxPacketSize {
    fn default() -> Self {
        MaxPacketSize(100000)
    }
}
impl Into<u32> for MaxPacketSize {
    fn into(self) -> u32 {
        return self.0;
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub connection_string: String,

    #[serde(default = "default_as_false")]
    pub timescale: bool
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicsConfig {
    pub subscribe: Vec<TopicSubscribe>,
    #[serde(default = "Default::default")]
    pub ignore: Vec<TopicIgnore>
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicSubscribe {
    pub topic: String
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicIgnore {
    pub topic: String
}

fn default_as_false() -> bool {
    false
}
