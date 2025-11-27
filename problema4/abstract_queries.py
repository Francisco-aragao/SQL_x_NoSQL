from abc import ABC, abstractmethod
from typing import List, Dict, Any
import time
from datetime import datetime

class SensorData:
    def __init__(self, sensor_id: str, timestamp: datetime, temperature: float, humidity: float):
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.humidity = humidity

class AbstractIoTDb(ABC):
    """
    Abstract class defining queries for the IoT scenario.
    """
    
    def __init__(self):
        self.conn = None

    @abstractmethod
    def connect(self):
        pass
        
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def insert_reading(self, data: SensorData) -> bool:
        """
        Insert a single sensor reading.
        """
        pass

    @abstractmethod
    def get_latest_reading(self, sensor_id: str) -> Dict[str, Any]:
        """
        Get the most recent reading for a specific sensor.
        """
        pass

    @abstractmethod
    def get_readings_by_range(self, sensor_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Get all readings for a sensor within a time range.
        """
        pass

    @abstractmethod
    def get_all_readings(self, sensor_id: str) -> List[Dict[str, Any]]:
        """
        Get ALL readings for a specific sensor.
        """
        pass

    @abstractmethod
    def get_average_temperature(self, sensor_id: str, start_time: datetime, end_time: datetime) -> float:
        """
        Calculate the average temperature for a sensor within a time range.
        """
        pass

    def run_all_queries(self, sensor_id: str, start_time: datetime, end_time: datetime):
        """
        Run all queries and measure time.
        """
        timings = {}
        results = {}
        
        # 1. Insert (Simulated new reading)
        new_data = SensorData(sensor_id, datetime.now(), 25.0, 60.0)
        start = time.perf_counter()
        results["insert_reading"] = self.insert_reading(new_data)
        timings["insert_reading"] = time.perf_counter() - start
        
        # 2. Get Latest
        start = time.perf_counter()
        results["get_latest_reading"] = self.get_latest_reading(sensor_id)
        timings["get_latest_reading"] = time.perf_counter() - start
        
        # 3. Get Range
        start = time.perf_counter()
        results["get_readings_by_range"] = self.get_readings_by_range(sensor_id, start_time, end_time)
        timings["get_readings_by_range"] = time.perf_counter() - start
        
        # 4. Get Average
        start = time.perf_counter()
        results["get_average_temperature"] = self.get_average_temperature(sensor_id, start_time, end_time)
        timings["get_average_temperature"] = time.perf_counter() - start
        
        return {
            "results": results,
            "timings": timings,
            "total_time": sum(timings.values())
        }