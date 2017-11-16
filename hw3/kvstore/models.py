from __future__ import unicode_literals
from django.db import models
from threading import Thread, Event, Timer

class Entry(models.Model):
    key   = models.CharField(max_length=200)
    value = models.TextField()
    causal_payload = models.TextField()
    node_id = models.IntegerField()
    timestamp = models.PositiveIntegerField()

    @classmethod
    # for entering a key, value pair
    def create_entry(cls, key, value, causal_payload, node_id, timestamp):
        entry = cls(key, value, causal_payload, node_id, timestamp)
        return entry

    def __str__(self):
        return "Key: %s Val: %s CP: %s NID: %s TS: %s" % (self.key, self.value, self.causal_payload, str(self.node_id), str(self.timestamp))

""" Probably don't need this class. """
class BThread(models.Model, Thread):
    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    """
    Take our most recent entry, go through our replica_nodes list, and if the IP is not our IP,
    and do....something.
    """
    def run(self, entry):
        while not self.stopped.wait(3.0):
            existing_entry = None
            try:
                # existing_entry = Entry.objects.get(key=key)
                existing_entry = Entry.objects.latest('timestamp')
                existing_cp = existing_entry.causal_payload
                existing_timestamp = existing_entry.timestamp
            except:
                new_entry = True