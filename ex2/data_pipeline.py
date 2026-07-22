import typing
from typing import Any, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        actual_value = self._storage.pop(0)
        actual_rank = self._rank
        self._rank += 1
        return (actual_rank, actual_value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")
        else:
            if isinstance(data, (int, float)):
                self._storage.append(str(data))
            else:
                for item in data:
                    self._storage.append(str(item))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper string data")
        else:
            if isinstance(data, str):
                self._storage.append(str(data))
            else:
                for item in data:
                    self._storage.append(str(item))


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(key, str)
                       and isinstance(value, str)
                       for key, value in data.items())
        elif isinstance(data, list):
            return all(isinstance(item, dict)
                       and all(isinstance(key, str) and isinstance(value, str)
                               for key, value in item.items())
                       for item in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")
        else:
            if isinstance(data, dict):
                self._storage.append(data["log_level"]
                                     + ": " + data["log_message"])
            else:
                for item in data:
                    self._storage.append(item["log_level"]
                                         + ": " + item["log_message"])


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            found = False
            for processor in self._processors:
                if processor.validate(element):
                    found = True
                    processor.ingest(element)
                    break
            if not found:
                print("DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._processors) == 0:
            print("No processor found, no data")
        else:
            for processor in self._processors:
                total = processor._rank + len(processor._storage)
                remaining = len(processor._storage)
                print(f"{type(processor).__name__}: total {total} "
                      f"items processed, remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            collected: list[tuple[int, str]] = []
            for _ in range(min(nb, len(processor._storage))):
                collected.append(processor.output())
            plugin.process_output(collected)


class CSVExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(value for _, value in data))


class JSONExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        pairs = ", ".join(f'"item_{rank}": "{value}"' for rank, value in data)
        print("{" + pairs + "}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")

    batch1 = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    print("\nInitialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Processors")
    data_stream.register_processor(NumericProcessor())
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())

    print(f"\nSend first batch of data on stream: {batch1}")
    data_stream.process_stream(batch1)
    data_stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, CSVExport())
    data_stream.print_processors_stats()

    print(f"\nSend another batch of data: {batch2}")
    data_stream.process_stream(batch2)
    data_stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, JSONExport())
    data_stream.print_processors_stats()
