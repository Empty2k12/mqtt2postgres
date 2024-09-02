<div align="center">
    <br/>
    <h1>mqtt2postgres</h1>
    <strong>This project aims to implement a MQTT Message Broker to Postgres interface.</strong>
</div>
<br/>
<p align="center">
    <a href="https://www.rust-lang.org/en-US/">
        <img src="https://img.shields.io/badge/Made%20with-Rust-orange.svg" alt='Build with Rust' />
    </a>
    <img src="https://img.shields.io/badge/rustc-1.82+-yellow.svg" alt='Minimum Rust Version: 1.82.0-nightly' />
    <img src="https://img.shields.io/badge/Postgres-9.1+-green.svg" alt='Minimum Postgres Version: 9.1' />
</p>

Pull requests are always welcome. For a list of past changes, see [CHANGELOG.md][__link2].

### Currently Supported Features

 - Generate Tables based on the MQTT Topic and Data-Type of the Payload
 - Unmarshal JSON payloads into a single table
 - Some things configurable via the TOML config file

## Quickstart

```toml
[mqtt]
broker_ip = "localhost"
broker_port = 1883
client_name = "mqtt2postgres"

[postgres]
connection_string = "postgresql://postgres:postgres@localhost:5432/mqtt2postgres"

# [[topics.subscribe]]
# topic = "#"

# avoid subscribing to topics multiple times, there is no deduplication yet

[[topics.subscribe]]
topic = "zigbee2mqtt/#"

[[topics.subscribe]]
topic = "awtrix/eceb30/#"
```

## License

[![License: MIT][__link16]][__link17]

@ 2024 Gero Gerke and [contributors].

 [contributors]: https://github.com/influxdb-rs/influxdb-rust/graphs/contributors
 [__link16]: https://img.shields.io/badge/License-MIT-yellow.svg
 [__link17]: https://opensource.org/licenses/MIT