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


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    num = NumericProcessor()
    print(f" Trying to validate input '42': {num.validate(42)}")
    print(f" Trying to validate input 'Hello': {num.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest("foo")
    except Exception as e:
        print(f" Got exception: {e}")
    data_num = [1, 2, 3, 4, 5]
    print(f" Processing data: {data_num}")
    num.ingest(data_num)
    print(" Extracting 3 values...")
    for _ in range(3):
        rank, value = num.output()
        print(f" Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    txt = TextProcessor()
    print(f" Trying to validate input '42': {txt.validate(42)}")
    data_txt = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {data_txt}")
    txt.ingest(data_txt)
    print(" Extracting 1 value...")
    rank, value = txt.output()
    print(f" Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    data_log = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f" Processing data: {data_log}")
    log.ingest(data_log)
    print(" Extracting 2 values...")
    for _ in range(2):
        rank, value = log.output()
        print(f" Log entry {rank}: {value}")
