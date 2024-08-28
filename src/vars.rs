macro_rules! var {
	($name:literal) => {{
		_ = dotenv::dotenv();
		std::env::var($name).expect(concat!($name, " not set."))
	}};
}

pub fn mqtt_broker_ip() -> String {
	var!("MQTT_BROKER_IP")
}
