use std::collections::HashMap;

// redis library doesn't seem to expect fields from 7.0+, so we have
// to do this, and parse it manually...
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct StreamInfoGroup {
    pub name: String,
    pub consumers: u64,
    pub pending: u64,
    pub last_delivered_id: String,
    // keep it an option here for 6.2 compatibility.  If it's null, we know
    // that it's not 7.0+, and we can handle it appropriately.
    pub entries_read: Option<u64>,
    // this can be null because the lag could not be calculated, or because
    // the group is not 7.0+.
    pub lag: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct StreamInfoGroupsReply {
    pub groups: Vec<StreamInfoGroup>,
}

impl redis::FromRedisValue for StreamInfoGroupsReply {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        type RawGroup = HashMap<String, redis::Value>;

        fn get<'g>(group: &'g RawGroup, name: &str) -> redis::RedisResult<&'g redis::Value> {
            group.get(name).ok_or_else(|| {
                redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "missing key from server while xinfo groups",
                    format!("missing {name}"),
                ))
            })
        }

        fn into<T: redis::FromRedisValue>(v: &redis::Value) -> redis::RedisResult<T> {
            redis::FromRedisValue::from_redis_value(v)
        }

        let groups: Vec<RawGroup> = redis::FromRedisValue::from_redis_value(v)?;
        let mut new = Vec::with_capacity(groups.len());

        for group in groups {
            let name = into::<String>(get(&group, "name")?)?;
            let consumers = into::<u64>(get(&group, "consumers")?)?;
            let pending = into::<u64>(get(&group, "pending")?)?;
            let last_delivered_id = into::<String>(get(&group, "last-delivered-id")?)?;
            let entries_read = group
                .get("entries-read")
                .map(into::<Option<u64>>)
                .transpose()?
                .flatten();
            let lag = group
                .get("lag")
                .map(into::<Option<u64>>)
                .transpose()?
                .flatten();

            new.push(StreamInfoGroup {
                name,
                consumers,
                pending,
                last_delivered_id,
                entries_read,
                lag,
            });
        }

        Ok(StreamInfoGroupsReply { groups: new })
    }
}
