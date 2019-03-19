from pathlib import Path
from collections import defaultdict
import numpy as np
import json
import dill


task_dirs = [d for d in Path(".").iterdir() if d.is_dir() and str(d).isdigit()]

with open("input/collector_arguments.d", 'rb') as f:
    arguments = dill.load(f)

average_results = arguments["average_results"]
encoder = arguments["encoder"]
decoder = arguments["decoder"]

result = defaultdict(lambda: 0)
square_result = defaultdict(lambda: 0)

to_be_averaged = lambda i: average_results == 'all' or i in average_results

failed_runs = []
error_message = ""
error_run_id = -1
N_total_runs = 0
total_weights = defaultdict(lambda: 0)

for task_dir in task_dirs:
    task_id = int(str(task_dir))
    task_output_file = task_dir / f"output_{task_id}.json"

    try:
        output = json.load(open(task_output_file, 'r'), cls=decoder)
    except FileNotFoundError:
        continue
    if output["failed_runs"]:
        failed_runs += output["failed_runs"]
        error_message = output["error_message"]["message"]
        error_run_id = output["error_message"]["run_id"]

    if output["N_local_runs"] == 0:
        continue

    local_weights = defaultdict(lambda: 0)
    for i, local_weight in output["local_weights"].items():
        local_weights[int(i)] = local_weight

    for i, r in enumerate(output["task_result"]):
        if to_be_averaged(i):
            result[i] += r * local_weights[i] if local_weights[i] > 0 else 0
        else:
            result[i] = r

        total_weights[i] += local_weights[i]

    for i, r2 in output["task_square_result"].items():
        i = int(i)
        square_result[i] += r2 * local_weights[i] if local_weights[i] > 0 else 0

    N_total_runs += output["N_local_runs"]

if N_total_runs > 0:
    result = {
        i: (r / total_weights[i] if to_be_averaged(i) and total_weights[i] > 0 else r)
        for i, r in result.items()
    }
    square_result = {
        i: (r2 / total_weights[i] if total_weights[i] > 0 else 0)
        for i, r2 in square_result.items()
    }
    if N_total_runs > 1:
        estimated_error = [
            np.sqrt((square_result[i] - abs(result[i])**2) / (N_total_runs - 1))
            for i in sorted(square_result)
        ]
        estimated_variance = [
            N_total_runs / (N_total_runs - 1) * (square_result[i] - abs(result[i])**2)
            for i in sorted(square_result)
        ]
    else:
        estimated_error = None
    total_weights = [total_weights[i] for i in sorted(total_weights)]

    result = [result[i] for i in sorted(result)]
    if len(result) == 1:
        result = result[0]

    if len(estimated_error) == 1:
        estimated_error = estimated_error[0]
    if len(estimated_variance) == 1:
        estimated_variance = estimated_variance[0]
else:
    result = None
    estimated_error = None
    estimated_variance = None

with open("output.json", 'w') as f:
    json.dump(
        {
            "result": result,
            "estimated_error": estimated_error,
            "estimated_variance": estimated_variance,
            "total_weights": total_weights,
            "failed_runs": failed_runs,
            "error_message": error_message,
            "error_run_id": error_run_id,
            "N_total_runs": N_total_runs
        },
        f,
        indent=2,
        cls=encoder
    )
