from datetime import datetime
from unittest import mock


from quixstreams_extensions.sinks.gcp import FlatGoogleFirestoreSink


def test_flat_sink(topic):
    messages = [
        {"key": "k1", "value": "v1"},
        {"key": "k2", "value": "v2"},
    ]
    sink = FlatGoogleFirestoreSink("test_collection", client=mock.Mock())
    for idx, message in enumerate(messages):
        sink.add(message["value"], message["key"], int(datetime.now().timestamp()), [], topic, 0, idx)
    sink.flush(topic, 0)
    sink._db.collection.assert_called_with("test_collection")
    sink._db.collection.return_value.document.assert_has_calls([mock.call("k1"), mock.call("k2")])
    sink._db.collection.return_value.document.return_value.set.assert_not_called()
    sink._db.batch.return_value.set.assert_has_calls([mock.call(mock.ANY, "v1"), mock.call(mock.ANY, "v2")])
