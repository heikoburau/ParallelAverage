from .json_numpy import NumpyEncoder, NumpyDecoder
import json
import pickle


def load_collective_result(database_entry, path, encoding):
    output_path = database_entry.output_path(path)

    with open(output_path) as f:
        output = json.load(f)

    return CollectiveResult(
        output["successful_runs"],
        output_path.parent,
        output["raw_results_map"],
        database_entry["job_name"],
        encoding
    )


class CollectiveResult:
    def __init__(self, run_ids, job_path, raw_results_map, job_name, encoding):
        self.run_ids = run_ids
        if isinstance(self.run_ids[0], str):
            self.run_ids = [eval(run_id) for run_id in self.run_ids]
        self.run_ids = sorted(self.run_ids)

        self.job_path = job_path
        self.raw_results_map = raw_results_map
        self.job_name = job_name
        self.encoding = encoding

    def __iter__(self):
        return iter(self.run_ids)

    def __getitem__(self, run_id):
        if not isinstance(run_id, str):
            run_id = repr(run_id)

        file_id = self.raw_results_map[run_id]
        file_path = self.job_path / "data_output" / f"{file_id}_raw_results.{self.encoding}"

        if self.encoding == "json":
            with open(file_path) as f:
                return json.load(f, cls=NumpyDecoder)[run_id]
        elif self.encoding == "pickle":
            if file_path.stat().st_size > 0:
                with open(file_path, 'rb') as f:
                    return pickle.load(f)[run_id]
            else:
                return None

    def __len__(self):
        return len(self.run_ids)

    def __repr__(self):
        return repr(dict(self.items()))

    def __str__(self):
        return str(dict(self.items()))

    def keys(self):
        return self.run_ids

    def values(self):
        for run_id in self:
            yield self[run_id]

    def items(self):
        for run_id in self:
            yield run_id, self[run_id]

    def replace_output(self, new_dict, new_encoding=None):
        new_encoding = new_encoding or self.encoding

        for run_id in new_dict:
            if not isinstance(run_id, str):
                run_id = repr(run_id)

            file_id = self.raw_results_map[run_id]
            file_path = self.job_path / "data_output" / f"{file_id}_raw_results.{self.encoding}"

            if self.encoding == "json":
                with open(file_path) as f:
                    raw_results = json.load(f, cls=NumpyDecoder)
            elif self.encoding == "pickle":
                if file_path.stat().st_size > 0:
                    with open(file_path, 'rb') as f:
                        raw_results = pickle.load(f)
                else:
                    raw_results = {}

            raw_results[run_id] = new_dict[eval(run_id)]

            file_path = self.job_path / "data_output" / f"{file_id}_raw_results.{new_encoding}"

            if new_encoding == "json":
                with open(file_path, "w") as f:
                    json.dump(raw_results, f, cls=NumpyEncoder, indent=2)
            elif new_encoding == "pickle":
                with open(file_path, "wb") as f:
                    pickle.dump(raw_results, f)

        if new_encoding != self.encoding:
            for file_id in self.raw_results_map.values():
                file_path = self.job_path / "data_output" / f"{file_id}_raw_results.{self.encoding}"
                file_path.unlink()

        self.encoding = new_encoding
