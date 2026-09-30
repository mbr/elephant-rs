//! Maps adjacently tagged jobs onto Absurd's name and parameter fields.

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, error::Category, json};
use serde_path_to_error::Segment;

use crate::{
    error::{Error, Result},
    types::TaskName,
};

/// Serializes and validates a job before any enqueue database access.
pub(crate) fn encode<J: Serialize>(job: J) -> Result<(TaskName, Value)> {
    let Value::Object(mut envelope) = serde_json::to_value(job).map_err(Error::json)? else {
        return Err(Error::InvalidJobEnvelope {
            reason: "must be an object with task and optional params fields",
        });
    };
    let Some(Value::String(name)) = envelope.remove("task") else {
        return Err(Error::InvalidJobEnvelope {
            reason: "task must be a string",
        });
    };
    let params = envelope.remove("params").unwrap_or(Value::Null);
    if !envelope.is_empty() {
        return Err(Error::InvalidJobEnvelope {
            reason: "only task and params fields are permitted",
        });
    }
    Ok((name.parse()?, params))
}

/// Reconstructs a job, distinguishing unsupported tags from invalid parameters.
pub(crate) fn decode<J: DeserializeOwned>(name: &TaskName, params: Value) -> Result<Option<J>> {
    let envelope = json!({"task": name.as_str(), "params": params});
    match serde_path_to_error::deserialize(envelope) {
        Ok(job) => Ok(Some(job)),
        Err(source) => {
            let mut path = source.path().iter();
            let unknown = source.inner().classify() == Category::Data
                && matches!(path.next(), Some(Segment::Map { key }) if key == "task")
                && path.next().is_none();
            if unknown {
                Ok(None)
            } else {
                Err(Error::JobDecode { source })
            }
        }
    }
}
