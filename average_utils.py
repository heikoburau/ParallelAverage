from copy import deepcopy
import json


def averages_match(averageA, averageB):
    return all(averageA[key] == averageB[key] for key in [
        "function_name", "args", "kwargs", "N_runs", "average_results"
    ])


def cleaned_average(average, encoder):
    cleaned_args = json.loads(
        json.dumps(average["args"], cls=encoder),
    )
    cleaned_kwargs = json.loads(
        json.dumps(average["kwargs"], cls=encoder),
    )

    result = deepcopy(average)
    result["args"] = cleaned_args
    result["kwargs"] = cleaned_kwargs

    return result
