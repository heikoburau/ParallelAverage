from .json_numpy import NumpyEncoder, NumpyDecoder
import numpy as np
import json


class WeightedSample:
    def __init__(self, sample, weight):
        self.sample = sample
        self.weight = weight


class Dataset:
    def __init__(self):
        self.data = 0
        self.data_squared = 0
        self.total_weight = 0
        self.num_samples = 0

    def add_sample(self, sample):
        if isinstance(sample, WeightedSample):
            weight = sample.weight
            sample = sample.sample
        else:
            weight = 1

        self.data += weight * sample if weight > 0 else 0
        self.data_suared += weight * abs(sample)**2 if weight > 0 else 0
        self.total_weight += weight
        self.num_samples += 1

    def __iadd__(self, other):
        if other == 0:
            return self
        assert isinstance(other, Dataset)

        self.data += other.total_weight * other.data
        self.data_squared += other.total_weight * other.data_squared
        self.total_weight += other.total_weight
        self.num_samples += other.num_samples

        return self

    def __add__(self, other):
        result = Dataset()
        result += self
        result += other
        return result

    @property
    def mean(self):
        return self.data / self.total_weight

    @property
    def mean_squared(self):
        return self.data_squared / self.total_weight

    @property
    def estimated_error(self):
        if self.num_samples <= 1:
            return None

        return np.sqrt((self.mean_squared - abs(self.mean)**2) / (self.num_samples - 1))

    @property
    def estimated_variance(self):
        if self.num_samples <= 1:
            return None

        return self.num_samples / (self.num_samples - 1) * (self.mean_squared - abs(self.mean)**2)

    def to_json(self):
        return dict(
            data=encode_array(self.data),
            data_squared=encode_array(self.data_squared),
            total_weight=self.total_weight,
            num_samples=self.num_samples
        )

    @staticmethod
    def from_json(self, obj):
        result = Dataset()
        result.data = decode_array(obj["data"])
        result.data_squared = decode_array(obj["data_squared"])
        result.total_weight = obj["total_weight"]
        result.num_samples = obj["num_samples"]
        return result


def decode_array(json_obj):
    return json.loads(json.dumps(json_obj), cls=NumpyDecoder)


def encode_array(arr):
    return json.dumps(arr, cls=NumpyEncoder)
