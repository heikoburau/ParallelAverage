import json
import numpy as np


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if type(obj) is np.ndarray:
            output = {
                "type": "ndarray",
                "dtype": str(obj.dtype),
                "complex": np.iscomplexobj(obj)
            }
            if output["complex"]:
                output["real"] = obj.real.tolist()
                output["imag"] = obj.imag.tolist()
            else:
                output["data"] = obj.tolist()

            return output

        return super().default(obj)


class NumpyDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if "type" in obj and obj["type"] == "ndarray":
            dtype = np.dtype(obj["dtype"])
            if obj["complex"]:
                return (
                    np.array(obj["real"], dtype=dtype) +
                    1j * np.array(obj["imag"], dtype=dtype)
                )
            return np.array(obj["data"], dtype=dtype)

        return obj
