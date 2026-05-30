import typing
from typing import Any
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage:list[str] = []
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
    def validate(self, data: Any):
        if isinstance(data, dict): 
            return all(isinstance(key, str) 
                       and isinstance(value, str) for key, value in data.items())
        elif isinstance(data, list):
            return all(isinstance(item, dict) 
                       and all(isinstance(key, str) and isinstance(value, str) for key, value in item.items()) for item in data)#on check si c est une list si oui on check si elle contient bien que des dict for dans un for
        return False
    

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")
        else:
            if isinstance(data, dict):
                self._storage.append(data["log_level"] + ": " + data["log_message"])
            else:
                for item in data:
                    self._storage.append(item["log_level"] + ": " + item["log_message"])

class DataStream:
    def __init__(self) -> None:
        self._processors :list[DataProcessor] = []


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
                print(f"DataStream error - Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._processors) == 0:
            print("No processor found, no data")
        else:
            for processor in self._processors:
                items_treated = processor._rank + len(processor._storage)
                remaining = len(processor._storage)
                print(f"{type(processor).__name__}: total {items_treated} items processed, remaining {remaining} on processor")



if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 
         42,
        ['Hi', 'five']
    ]

    print("\nInitialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    data_stream.register_processor(NumericProcessor())

    print(f"\nSend first batch of data on stream: {batch}")
    data_stream.process_stream(batch)
    data_stream.print_processors_stats()

    print("\nRegistering other data processors")
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())
    print("Send the same batch again")
    data_stream.process_stream(batch)
    data_stream.print_processors_stats()

    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    for _ in range(3):
        data_stream._processors[0].output()
    for _ in range(2):
        data_stream._processors[1].output()
    data_stream._processors[2].output()
    data_stream.print_processors_stats()
