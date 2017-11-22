from __future__ import unicode_literals
from django.db import models
from threading import Thread, Event, Timer

class Entry(models.Model):
    key = models.CharField(max_length=200)
    val = models.TextField()
    causal_payload = models.TextField()
    node_id = models.IntegerField()
    timestamp = models.PositiveIntegerField()

    @classmethod
    # for entering a key, value pair
    def create_entry(cls, key, value, causal_payload, node_id, timestamp):
        entry = cls(key, value, causal_payload, node_id, timestamp)
        return entry

    def __str__(self):
        return "Key: %s Val: %s CP: %s NID: %s TS: %s" % (self.key, self.val, self.causal_payload, str(self.node_id), str(self.timestamp))
