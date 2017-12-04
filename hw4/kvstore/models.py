from __future__ import unicode_literals
from django.db import models
import bisect

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

# DEFINITION FOR CLUSTERS OF DOCKER INSTANCES

# Code for consistent hashing: http://techspot.zzzeek.org/2012/07/07/the-absolutely-simplest-consistent-hashing-example/

class HashRing(object):

    def __init__(self, replicas):
        self.replicas = replicas
        self._keys = []
        self._nodes = {}

    # SINGLE UNDERSCORE -> PRIVATE USE.
    def _hash(self, key):
        """ Given a string key, return a hash value. """
        return hash(key) % 256

    def _repl_iterator(self, nodename):
        """ Given a node name, return an iterable of replica hashes. """
        return (self._hash("%s:%s" % (nodename, i))
                for i in range(self.replicas))

    """ Usage: cluster["10.0.0.20:8080"] = foo """
    def __setitem__(self, nodename, node):
        """ Add a node, given its name.
        The given nodename is hashed among the number of replicas """
        for hash_ in self._repl_iterator(nodename):
            if hash_ in self._nodes:
                raise ValueError("Node name %r is already present" % nodename)
            self._nodes[hash_] = node
            bisect.insort(self._keys, hash_)

    def __delitem__(self, nodename):
        """ Remove a node, given it's name """
        for hash_ in self._repl_iterator(nodename):
            # Raises KeyError for nonexistent nodename
            del self._nodes[hash_]
            index = bisect.bisect_left(self._keys, hash_)
            del self._keys[index]

    """ Usage: data = cluster["10.0.0.20:8080"] """
    def __getitem__(self, key):
        """ Return a node, given it's key.
        The node replica with a hash value nearest but not less than that
        of the given name is returned.  If the hash of the given name is
        greater than the greatest hash, returns the lowest hashed node. """
        hash_ = self._hash(key)
        start = bisect.bisect(self._keys, hash_)
        if start == len(self._keys):
            start = 0
        return self._nodes[self._keys[start]]

