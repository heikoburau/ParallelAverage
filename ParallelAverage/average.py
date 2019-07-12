from copy import deepcopy
import json


def cleaned_average(average, encoder):
    cleaned_args = json.loads(
        json.dumps(average["args"], cls=encoder),
    )
    cleaned_kwargs = json.loads(
        json.dumps(average["kwargs"], cls=encoder),
    )

    result = Average(deepcopy(average))
    result["args"] = cleaned_args
    result["kwargs"] = cleaned_kwargs

    return result


class Average(dict):
    def __eq__(self, other):
        try:
            return all(self[key] == other[key] for key in [
                "function_name", "args", "kwargs", "N_runs", "average_results"
            ])
        except KeyError:
            return all(self[key if key != "average_results" else "average_arrays"] == other[key] for key in [
                "function_name", "args", "kwargs", "N_runs", "average_results"
            ])

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        average_results = self['average_results'] if 'average_results' in self else (
            self['average_arrays'] if 'average_arrays' in self else None
        )

        return (
            f"function_name: {self['function_name']}\n"
            f"args: {self['args']}\n"
            f"kwargs: {self['kwargs']}\n"
            f"N_runs: {self['N_runs']}\n"
            f"average_results: {average_results}"
        )

    def distance_to(self, other):
        result = 0
        if self["function_name"] != other["function_name"]:
            return 100
        result += Average.__compute_distance_between_args(self["args"], other["args"])
        result += Average.__compute_distance_between_kwargs(self["kwargs"], other["kwargs"])
        if self["N_runs"] != other["N_runs"]:
            result += 1

        average_resultsA = self["average_results"] if "average_results" in self else self["average_arrays"]
        average_resultsB = other["average_results"] if "average_results" in other else other["average_arrays"]
        if average_resultsA != average_resultsB:
            result += 1

        return result

    @staticmethod
    def __compute_distance_between_args(argsA, argsB):
        result = 0
        result += abs(len(argsA) - len(argsB))
        result += sum(1 if argA != argB else 0 for argA, argB in zip(argsA, argsB))
        return result

    @staticmethod
    def __compute_distance_between_kwargs(kwargsA, kwargsB):
        result = 0
        result += len(set(kwargsA) ^ set(kwargsB))
        result += sum(1 if kwargsA[key] != kwargsB[key] else 0 for key in set(kwargsA) & set(kwargsB))
        return result
