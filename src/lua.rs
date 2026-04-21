use crate::error::{KvStoreError, Result};
use crate::kv_store::{KvStore, Value};
use crate::wal::{Wal, WalEntry, WalOperation};
use mlua::{Lua, MultiValue, Value as LuaValue};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

pub fn execute_script(store: &mut KvStore, wal: Option<&Wal>, script: &str) -> Result<String> {
    let lua = Lua::new();
    let output = Rc::new(RefCell::new(String::new()));
    let store_ref = Rc::new(RefCell::new(store));

    lua.scope(|scope| {
        let globals = lua.globals();

        let store_for_get = Rc::clone(&store_ref);
        let get_fn = scope.create_function_mut(move |_, key: String| {
            let store = store_for_get.borrow();
            let value = store.get(&key).map(|v| v.to_string());
            Ok(value)
        })?;
        globals.set("get", get_fn)?;

        let store_for_set = Rc::clone(&store_ref);
        let set_fn = scope.create_function_mut(move |_, (key, value): (String, String)| {
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
            let mut store = store_for_set.borrow_mut();
            store
                .set(key, value)
                .map_err(|e| mlua::Error::external(e.to_string()))?;
            Ok(true)
        })?;
        globals.set("set", set_fn)?;

        let store_for_setex = Rc::clone(&store_ref);
        let setex_fn = scope.create_function_mut(
            move |_, (key, value, ttl_secs): (String, String, u64)| {
                if let Some(wal) = wal {
                    let entry = WalEntry::new(WalOperation::Set {
                        key: key.clone(),
                        value: serde_json::to_string(&Value::Str(value.clone()))
                            .unwrap_or_else(|_| format!("\"{}\"", value)),
                        ttl_secs: Some(ttl_secs),
                    });
                    wal.append(&entry)
                        .map_err(|e| mlua::Error::external(e.to_string()))?;
                }

                let mut store = store_for_setex.borrow_mut();
                store
                    .set_with_ttl(key, value, Duration::from_secs(ttl_secs))
                    .map_err(|e| mlua::Error::external(e.to_string()))?;
                Ok(true)
            },
        )?;
        globals.set("setex", setex_fn)?;

        let store_for_delete = Rc::clone(&store_ref);
        let del_fn = scope.create_function_mut(move |_, key: String| {
            if let Some(wal) = wal {
                let entry = WalEntry::new(WalOperation::Delete { key: key.clone() });
                wal.append(&entry)
                    .map_err(|e| mlua::Error::external(e.to_string()))?;
            }
            let mut store = store_for_delete.borrow_mut();
            Ok(store.delete(&key).is_some())
        })?;
        globals.set("delete", del_fn)?;

        let store_for_incr = Rc::clone(&store_ref);
        let incr_fn = scope.create_function_mut(move |_, key: String| {
            if let Some(wal) = wal {
                let entry = WalEntry::new(WalOperation::Incr { key: key.clone() });
                wal.append(&entry)
                    .map_err(|e| mlua::Error::external(e.to_string()))?;
            }
            let mut store = store_for_incr.borrow_mut();
            store
                .incr(&key)
                .map_err(|e| mlua::Error::external(e.to_string()))
        })?;
        globals.set("incr", incr_fn)?;

        let store_for_exists = Rc::clone(&store_ref);
        let exists_fn = scope.create_function_mut(move |_, key: String| {
            let store = store_for_exists.borrow();
            Ok(store.exists(&key))
        })?;
        globals.set("exists", exists_fn)?;

        let store_for_ttl = Rc::clone(&store_ref);
        let ttl_fn = scope.create_function_mut(move |_, key: String| {
            let store = store_for_ttl.borrow();
            Ok(store.ttl_seconds(&key))
        })?;
        globals.set("ttl", ttl_fn)?;

        let store_for_pttl = Rc::clone(&store_ref);
        let pttl_fn = scope.create_function_mut(move |_, key: String| {
            let store = store_for_pttl.borrow();
            Ok(store.pttl_millis(&key))
        })?;
        globals.set("pttl", pttl_fn)?;

        let output_for_print = Rc::clone(&output);
        let print_fn = scope.create_function_mut(move |_, message: String| {
            let mut out = output_for_print.borrow_mut();
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
            .map_err(|e| mlua::Error::external(format!("Lua execution failed: {}", e)))?;

        let mut out = output.borrow().clone();
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
        store.set("n".to_string(), 0i64).expect("seed set failed");
        let output = execute_script(&mut store, None, "incr('n'); return exists('n'), get('n')")
            .expect("lua script failed");

        assert_eq!(output, "true 1");
    }

    #[test]
    fn test_lua_setex_with_positive_ttl() {
        let mut store = KvStore::new();

        let output = execute_script(
            &mut store,
            None,
            "setex('session', 'abc123', 60); return exists('session'), get('session')",
        )
        .expect("lua script failed");

        assert_eq!(output, "true abc123");
    }

    #[test]
    fn test_lua_setex_zero_ttl_expires_on_cleanup() {
        let mut store = KvStore::new();

        execute_script(&mut store, None, "setex('temp', 'v', 0)").expect("lua script failed");

        let cleaned = store.cleanup_expired();
        assert_eq!(cleaned, 1);
        assert!(!store.exists("temp"));
    }

    #[test]
    fn test_lua_ttl_and_pttl_for_expiring_key() {
        let mut store = KvStore::new();

        let output = execute_script(
            &mut store,
            None,
            "setex('session', 'abc123', 60); return ttl('session') >= 0, pttl('session') > 0",
        )
        .expect("lua script failed");

        assert_eq!(output, "true true");
    }

    #[test]
    fn test_lua_ttl_and_pttl_missing_and_persistent_keys() {
        let mut store = KvStore::new();

        let output = execute_script(
            &mut store,
            None,
            "set('k', 'v'); return ttl('missing'), pttl('missing'), ttl('k'), pttl('k')",
        )
        .expect("lua script failed");

        assert_eq!(output, "-2 -2 -1 -1");
    }
}
