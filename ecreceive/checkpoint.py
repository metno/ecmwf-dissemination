import logging
import json


CHECKPOINT_DATASET_NOFLAGS = 0
CHECKPOINT_DATASET_EXISTS = 1
CHECKPOINT_DATASET_MOVED = 2
CHECKPOINT_DATASET_LOCKED = 4


class Checkpoint(object):
    """
    This class creates a state file, which keeps a key/value store of Datasets
    and their states.
    """
    def __init__(self, path):
        self._states = {}
        self._path = path
        self.load()

    def save(self):
        data = json.dumps(self._states, sort_keys=True, indent=4, separators=(',', ': '))
        try:
            with open(self._path, 'w') as f:
                f.write(data)
        except IOError:
            logging.error('State file %s cannot be written' % self._path)
            raise

    def load(self):
        self._states = {}
        try:
            with open(self._path, 'r') as f:
                data = f.read()
            if data:
                self._states = json.loads(data)
        except IOError:
            logging.info('State file %s does not exist, starting from scratch' % self._path)

    def keys(self):
        return self._states.keys()

    def get(self, key):
        if key not in self._states:
            return 0
        return self._states[key]

    def add(self, key, state):
        if key not in self._states:
            self._states[key] = CHECKPOINT_DATASET_NOFLAGS
        self._states[key] |= state
        self.save()

    def subtract(self, key, state):
        if key not in self._states:
            self._states[key] = CHECKPOINT_DATASET_NOFLAGS
        self._states[key] &= ~state
        self.save()

    def lock(self, key):
        if self.get(key) & CHECKPOINT_DATASET_LOCKED:
            return False
        self.add(key, CHECKPOINT_DATASET_LOCKED)
        return True

    def unlock(self, key):
        self.subtract(key, CHECKPOINT_DATASET_LOCKED)

    def unlock_all(self):
        self.load()
        for key in self._states.keys():
            self.unlock(key)

    def delete(self, key):
        if key in self._states:
            del self._states[key]
            self.save()
