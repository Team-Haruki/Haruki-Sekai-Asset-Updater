use yaml_serde::{Mapping, Value};

pub fn migrate_legacy_config_shape(root: &mut Value) {
    let Value::Mapping(map) = root else {
        return;
    };

    migrate_legacy_tools_config(map);
    migrate_legacy_resource_config(map);
}

fn migrate_legacy_tools_config(root: &mut Mapping) {
    let Some(tools_value) = root.remove(Value::String("tools".to_string())) else {
        return;
    };
    let Value::Mapping(mut tools) = tools_value else {
        return;
    };

    let backends = mapping_child(root, "backends");
    let asset_studio = mapping_child(backends, "asset_studio");
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_library_path",
        "library_path",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_library_path",
        "library_path",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_call_mode",
        "call_mode",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_call_mode",
        "call_mode",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_worker_path",
        "worker_path",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_worker_path",
        "worker_path",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_process_concurrency",
        "process_concurrency",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_process_concurrency",
        "process_concurrency",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_worker_max_calls",
        "worker_max_calls",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_worker_max_calls",
        "worker_max_calls",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_read_batch_size",
        "read_batch_size",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_read_batch_size",
        "read_batch_size",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_image_format",
        "image_format",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_image_format",
        "image_format",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_read_kinds",
        "read_kinds",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_read_kinds",
        "read_kinds",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_ffi_cli_parity_mode",
        "cli_parity_mode",
    );
    move_mapping_value(
        &mut tools,
        asset_studio,
        "asset_studio_native_cli_parity_mode",
        "cli_parity_mode",
    );

    let media = mapping_child(backends, "media");
    move_mapping_value(&mut tools, media, "media_backend", "backend");
    move_mapping_value(&mut tools, media, "ffmpeg_path", "ffmpeg_path");

    if !tools.is_empty() {
        root.insert(Value::String("tools".to_string()), Value::Mapping(tools));
    }
}

fn migrate_legacy_resource_config(root: &mut Mapping) {
    let Some(concurrency_value) = root.remove(Value::String("concurrency".to_string())) else {
        migrate_legacy_execution_memory_config(root);
        return;
    };
    let Value::Mapping(mut concurrency) = concurrency_value else {
        migrate_legacy_execution_memory_config(root);
        return;
    };

    let resources = mapping_child(root, "resources");
    let cpu = mapping_child(resources, "cpu");
    move_mapping_value(&mut concurrency, cpu, "cpu_budget_auto", "budget_auto");
    move_mapping_value(&mut concurrency, cpu, "cpu_budget_ratio", "budget_ratio");
    move_mapping_value(&mut concurrency, cpu, "cpu_reserved", "reserved");

    let throttle = mapping_child(cpu, "throttle");
    move_mapping_value(
        &mut concurrency,
        throttle,
        "cpu_throttle_enabled",
        "enabled",
    );
    move_mapping_value(
        &mut concurrency,
        throttle,
        "cpu_throttle_sample_ms",
        "sample_ms",
    );

    if !concurrency.is_empty() {
        root.insert(
            Value::String("concurrency".to_string()),
            Value::Mapping(concurrency),
        );
    }

    migrate_legacy_execution_memory_config(root);
}

fn migrate_legacy_execution_memory_config(root: &mut Mapping) {
    let Some(execution_value) = root.remove(Value::String("execution".to_string())) else {
        return;
    };
    let Value::Mapping(mut execution) = execution_value else {
        return;
    };
    let resources = mapping_child(root, "resources");
    let memory = mapping_child(resources, "memory");
    move_mapping_value(
        &mut execution,
        memory,
        "max_in_flight_bundle_bytes",
        "max_in_flight_bundle_bytes",
    );

    if !execution.is_empty() {
        root.insert(
            Value::String("execution".to_string()),
            Value::Mapping(execution),
        );
    }
}

fn mapping_child<'a>(map: &'a mut Mapping, key: &str) -> &'a mut Mapping {
    let value = map
        .entry(Value::String(key.to_string()))
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    if !matches!(value, Value::Mapping(_)) {
        *value = Value::Mapping(Mapping::new());
    }
    let Value::Mapping(child) = value else {
        unreachable!("value was normalized into a mapping")
    };
    child
}

fn move_mapping_value(source: &mut Mapping, target: &mut Mapping, old_key: &str, new_key: &str) {
    if target.contains_key(Value::String(new_key.to_string())) {
        return;
    }
    if let Some(value) = source.remove(Value::String(old_key.to_string())) {
        target.insert(Value::String(new_key.to_string()), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrates_legacy_tools_resources_and_memory() {
        let mut value: Value = yaml_serde::from_str(
            r#"
config_version: 2
execution:
  max_in_flight_bundle_bytes: 1048576
tools:
  ffmpeg_path: ffmpeg
  media_backend: cli
  asset_studio_ffi_library_path: /tmp/libffi.so
  asset_studio_ffi_call_mode: process
  asset_studio_ffi_read_batch_size: 16
concurrency:
  cpu_budget_auto: true
  cpu_budget_ratio: 0.5
  cpu_reserved: 2
  cpu_throttle_enabled: true
  cpu_throttle_sample_ms: 500
  download: 8
"#,
        )
        .unwrap();

        migrate_legacy_config_shape(&mut value);

        let migrated = yaml_serde::to_string(&value).unwrap();
        assert!(!migrated.contains("tools:"));
        assert!(!migrated.contains("asset_studio_ffi_"));
        assert!(!migrated.contains("cpu_budget_"));
        assert!(migrated.contains("backends:"));
        assert!(migrated.contains("asset_studio:"));
        assert!(migrated.contains("library_path: /tmp/libffi.so"));
        assert!(migrated.contains("read_batch_size: 16"));
        assert!(migrated.contains("media:"));
        assert!(migrated.contains("backend: cli"));
        assert!(migrated.contains("resources:"));
        assert!(migrated.contains("budget_ratio: 0.5"));
        assert!(migrated.contains("sample_ms: 500"));
        assert!(migrated.contains("max_in_flight_bundle_bytes: 1048576"));
        assert!(migrated.contains("concurrency:"));
        assert!(migrated.contains("download: 8"));
    }
}
