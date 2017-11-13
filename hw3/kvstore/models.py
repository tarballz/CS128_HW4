from __future__ import unicode_literals
from django.db import models

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
