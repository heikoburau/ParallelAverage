from pathlib import Path
from collections import defaultdict
import numpy as np
import json

task_dirs = [d for d in Path(".").iterdir() if d.is_dir() and str(d).isdigit()]
num_tasks = len(task_dirs)

with open("collector_arguments.json", 'r') as f:
    average_arrays = json.load(f)

result = defaultdict(lambda: 0)

def must_average(index):
    if is_numpy_array is True:
        return average_arrays == 'all' or 0 in average_arrays
    elif isinstance(is_numpy_array, (tuple, list)):
        return is_numpy_array[index] and (average_arrays == 'all' or index in average_arrays)
    return False

must_average = lambda i: average_arrays == 'all' or i in average_arrays

for task_dir in task_dirs:
    task_id = int(str(task_dir))
    task_output_file = task_dir / f"output_{task_id}.json"
    output = json.load(open(task_output_file, 'r'))

    global is_numpy_array
    is_numpy_array = output["is_numpy_array"]
    if isinstance(is_numpy_array, (tuple, list)):
        output = output["result"]
    else:
        output = [output["result"]]

    for i, r in enumerate(output):
        if must_average(i):
            result[i] += np.array(r)
        else:
            result[i] = r
    

result = {i: (r / num_tasks).tolist() if must_average(i) else r for i, r in result.items()}
result = [result[i] for i in sorted(result)]
if not isinstance(is_numpy_array, (tuple, list)):
    result = result[0]

output = {
    "is_numpy_array": is_numpy_array,
    "result": result
}

with open("output.json", 'w') as f:
    json.dump(output, f)