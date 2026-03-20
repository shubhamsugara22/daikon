use crate::error::{KvStoreError, Result};
use crate::kv_store::{KvStore, Value};
use crate::wal::{Wal, WalEntry, WalOperation};
use mlua::{Lua, MultiValue, Value as LuaValue};
use std::cell::RefCell;

pub fn execute_script(store: &mut KvStore, wal: Option<&Wal>, script: &str) -> Result<String> {
    let lua = Lua::new();
    let output = RefCell::new(String::new());

    lua.scope(|scope| {
        let globals = lua.globals();

        let get_fn = scope.create_function_mut(|_, key: String| {
            let value = store.get(&key).map(|v| v.to_string());
            Ok(value)
        })?;
        globals.set("get", get_fn)?;

        let set_fn = scope.create_function_mut(|_, (key, value): (String, String)| {
            if let Some(wal) = wal {
                let entry = WalEntry::new(WalOperation::Set {
                    key: key.clone(),
                    value: serde_json::to_string(&Value::Str(value.clone()))
                        .unwrap_or_else(|_| format!("\"{}\"", value)),
                    ttl_secs: None,
                });
                wal.append(&entry)
                    .map_err(|e| mlua::Error::external(e.to_string()))?;
            }
            store
                .set(key, value)
                .map_err(|e| mlua::Error::external(e.to_string()))?;
            Ok(true)
        })?;
        globals.set("set", set_fn)?;

        let del_fn = scope.create_function_mut(|_, key: String| {
            if let Some(wal) = wal {
                let entry = WalEntry::new(WalOperation::Delete { key: key.clone() });
                wal.append(&entry)
                    .map_err(|e| mlua::Error::external(e.to_string()))?;
            }
            Ok(store.delete(&key).is_some())
        })?;
        globals.set("delete", del_fn)?;

        let incr_fn = scope.create_function_mut(|_, key: String| {
            if let Some(wal) = wal {
                let entry = WalEntry::new(WalOperation::Incr { key: key.clone() });
                wal.append(&entry)
                    .map_err(|e| mlua::Error::external(e.to_string()))?;
            }
            store
                .incr(&key)
                .map_err(|e| mlua::Error::external(e.to_string()))
        })?;
        globals.set("incr", incr_fn)?;

        let exists_fn = scope.create_function_mut(|_, key: String| Ok(store.exists(&key)))?;
        globals.set("exists", exists_fn)?;

        let print_fn = scope.create_function_mut(|_, message: String| {
            let mut out = output.borrow_mut();
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&message);
            Ok(())
        })?;
        globals.set("print", print_fn)?;

        let chunk = lua.load(script);
        let result: MultiValue = chunk
            .eval()
            .map_err(|e| KvStoreError::OperationFailed(format!("Lua execution failed: {}", e)))?;

        let mut out = output.into_inner();
        if !result.is_empty() {
            let rendered: Vec<String> = result.into_iter().map(lua_value_to_string).collect();
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&rendered.join(" "));
        }

        Ok(out)
    })
    .map_err(|e| KvStoreError::OperationFailed(format!("Lua setup failed: {}", e)))
}

fn lua_value_to_string(value: LuaValue) -> String {
    match value {
        LuaValue::Nil => "nil".to_string(),
        LuaValue::Boolean(v) => v.to_string(),
        LuaValue::Integer(v) => v.to_string(),
        LuaValue::Number(v) => v.to_string(),
        LuaValue::String(v) => v
            .to_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "<binary>".to_string()),
        _ => "<complex>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_store::KvStore;

    #[test]
    fn test_lua_read_write_script() {
        let mut store = KvStore::new();
        let output = execute_script(
            &mut store,
            None,
            "set('counter', '1'); local v = get('counter'); return v",
        )
        .expect("lua script failed");

        assert_eq!(output, "1");
        assert_eq!(store.get("counter"), Some(&Value::Str("1".to_string())));
    }

    #[test]
    fn test_lua_incr_and_exists() {
        let mut store = KvStore::new();
        let output = execute_script(
            &mut store,
            None,
            "set('n', '0'); incr('n'); return exists('n'), get('n')",
        )
        .expect("lua script failed");

        assert_eq!(output, "true 1");
    }
}
