from pathlib import Path
from collections import defaultdict
import numpy as np
import json
from json_numpy import NumpyEncoder, NumpyDecoder


task_dirs = [d for d in Path(".").iterdir() if d.is_dir() and str(d).isdigit()]

with open("collector_arguments.json", 'r') as f:
    average_arrays = json.load(f)

result = defaultdict(lambda: 0)

to_be_averaged = lambda i: average_arrays == 'all' or i in average_arrays

failed_tasks = []
num_successful_tasks = 0

for task_dir in task_dirs:
    task_id = int(str(task_dir))
    task_output_file = task_dir / f"output_{task_id}.json"

    output = json.load(open(task_output_file, 'r'), cls=NumpyDecoder)
    if "failed" in output:
        failed_tasks.append({
            "task_id": task_id,
            "run_id": output["run_id"],
            "error message": output["error message"]
        })
        continue

    for i, r in enumerate(output["task_result"]):
        if to_be_averaged(i):
            result[i] += r
        else:
            result[i] = r

    num_successful_tasks += 1

result = {i: r / num_successful_tasks if to_be_averaged(i) else r for i, r in result.items()}
result = [result[i] for i in sorted(result)]
if len(result) == 1:
    result = result[0]

with open("output.json", 'w') as f:
    json.dump(
        {
            "failed_tasks": failed_tasks,
            "result": result
        },
        f,
        cls=NumpyEncoder
    )
