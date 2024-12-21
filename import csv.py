import csv
import pickle
from datetime import datetime

class Table:
    def __init__(self, data=None):
        self.data = data or {"headers": [], "rows": []}

    @staticmethod
    def load_table_csv(filepath):
        try:
            with open(filepath, mode='r', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader)
                rows = [row for row in reader]
            return Table({"headers": headers, "rows": rows})
        except FileNotFoundError:
            raise FileNotFoundError(f"File {filepath} not found.")

    @staticmethod
    def save_table_csv(table, filepath, max_rows=None):
        try:
            if max_rows and max_rows > 0:
                for i, start_row in enumerate(range(0, len(table.data["rows"]), max_rows)):
                    chunk = table.data["rows"][start_row:start_row + max_rows]
                    chunk_filepath = f"{filepath.split('.')[0]}_part{i + 1}.csv"
                    with open(chunk_filepath, mode='w', encoding='utf-8', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(table.data["headers"])
                        writer.writerows(chunk)
            else:
                with open(filepath, mode='w', encoding='utf-8', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(table.data["headers"])
                    writer.writerows(table.data["rows"])
        except Exception as e:
            raise IOError(f"An error occurred while saving to CSV: {e}")

    @staticmethod
    def load_table_pickle(filepath):
        try:
            with open(filepath, mode='rb') as file:
                data = pickle.load(file)
            return Table(data)
        except FileNotFoundError:
            raise FileNotFoundError(f"File {filepath} not found.")
        except pickle.PickleError:
            raise ValueError("Invalid pickle file format.")

    @staticmethod
    def save_table_pickle(table, filepath, max_rows=None):
        try:
            if max_rows and max_rows > 0:
                for i, start_row in enumerate(range(0, len(table.data["rows"]), max_rows)):
                    chunk = table.data["rows"][start_row:start_row + max_rows]
                    chunk_filepath = f"{filepath.split('.')[0]}_part{i + 1}.pkl"
                    with open(chunk_filepath, mode='wb') as file:
                        pickle.dump({"headers": table.data["headers"], "rows": chunk}, file)
            else:
                with open(filepath, mode='wb') as file:
                    pickle.dump(table.data, file)
        except Exception as e:
            raise IOError(f"An error occurred while saving to pickle: {e}")

    @staticmethod
    def save_table_text(table, filepath):
        try:
            with open(filepath, mode='w', encoding='utf-8') as file:
                file.write("\t".join(table.data["headers"]) + "\n")
                for row in table.data["rows"]:
                    file.write("\t".join(map(str, row)) + "\n")
        except Exception as e:
            raise IOError(f"An error occurred while saving to text: {e}")

    def get_rows_by_number(self, start, stop=None, copy_table=False):
        stop = stop or start + 1
        rows = self.data["rows"][start:stop]
        if copy_table:
            return Table({"headers": self.data["headers"], "rows": rows.copy()})
        return Table({"headers": self.data["headers"], "rows": rows})

    def get_rows_by_index(self, *values, copy_table=False):
        rows = [row for row in self.data["rows"] if row[0] in values]
        if copy_table:
            return Table({"headers": self.data["headers"], "rows": rows.copy()})
        return Table({"headers": self.data["headers"], "rows": rows})

    def get_column_types(self, by_number=True):
        result = {}
        for i, header in enumerate(self.data["headers"]):
            column_values = [row[i] for row in self.data["rows"]]
            column_type = self._detect_type(column_values)
            key = i if by_number else header
            result[key] = column_type
        return result

    def set_column_types(self, types_dict, by_number=True):
        for key, col_type in types_dict.items():
            index = key if by_number else self.data["headers"].index(key)
            for row in self.data["rows"]:
                try:
                    if col_type == datetime:
                        row[index] = datetime.fromisoformat(row[index])
                    else:
                        row[index] = col_type(row[index])
                except ValueError:
                    raise ValueError(f"Cannot convert value {row[index]} in column {key} to {col_type}")

    def get_values(self, column=0):
        index = column if isinstance(column, int) else self.data["headers"].index(column)
        return [row[index] for row in self.data["rows"]]

    def get_value(self, column=0):
        if len(self.data["rows"]) != 1:
            raise ValueError("Table must have exactly one row to use get_value.")
        return self.get_values(column)[0]

    def set_values(self, values, column=0):
        index = column if isinstance(column, int) else self.data["headers"].index(column)
        if len(values) != len(self.data["rows"]):
            raise ValueError("Length of values does not match number of rows.")
        for row, value in zip(self.data["rows"], values):
            row[index] = value

    def set_value(self, value, column=0):
        if len(self.data["rows"]) != 1:
            raise ValueError("Table must have exactly one row to use set_value.")
        self.set_values([value], column)

    def print_table(self):
        print("\t".join(self.data["headers"]))
        for row in self.data["rows"]:
            print("\t".join(map(str, row)))

    def _detect_type(self, column_values):
        for value in column_values:
            if value.isdigit():
                return int
            try:
                float(value)
                return float
            except ValueError:
                try:
                    datetime.fromisoformat(value)
                    return datetime
                except ValueError:
                    continue
        return str
