use std::collections::HashMap;

struct DeviceHashTable {
    table: HashMap<String, String>,
}

impl DeviceHashTable {
    fn new() -> Self {
        DeviceHashTable {
            table: HashMap::new(),
        }
    }

    fn insert(&mut self, key: String, value: String) {
        self.table.insert(key, value);
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.table.get(key)
    }
}

fn main() {
    let mut device_table = DeviceHashTable::new();
    device_table.insert("device1".to_string(), "192.168.1.1".to_string());

    if let Some(ip) = device_table.get("device1") {
        println!("IP address of device1: {}", ip);
    } else {
        println!("Device not found");
    }
}