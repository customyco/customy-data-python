# Customy Data SDK for Python

Server-side Python SDK for governed `track`, `identify`, `group`, `page`,
`screen` and `alias` collection into Customy Data.

```python
from customy_data import CustomyData

data = CustomyData(
    collect_url="https://data.customy.ai",
    write_key="cdw_your_source_write_key",
    redact_fields={"password", "cardNumber"},
)

data.track(
    "Product Viewed",
    {"sku": "A-1", "price": 29.9},
    anonymous_id="anon_123",
    consent={"analytics": True},
)
```

The source write key resolves organization, project and environment inside
Customy Data. The public payload never accepts tenant identifiers. Redaction
and `before_send` run locally before serialization or network I/O.

Customy Data owns collection, identity, consent, audiences and activation.
Customy Analytics consumes governed aggregate read models; this SDK never
writes directly to Analytics.
