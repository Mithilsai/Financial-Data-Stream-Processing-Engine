"""
Real-time Financial Data Stream Processing Engine Simulation
Demonstrates the use of design patterns (Singleton, Adapter, Factory Method, Observer)
for building a modular and extensible data processing system using asyncio.
Integrates Alpha Vantage for real financial data.
"""

import asyncio
import random
import time
import csv
import io
import uuid # Used for unique IDs if needed
from abc import ABC, abstractmethod
from collections import deque
import copy # For deep copying data if needed
import aiohttp # For asynchronous HTTP requests

# --- Configuration Management (Singleton Pattern) ---
class ConfigurationManager:
    """
    Manages application configuration using the Singleton pattern.
    """
    _instance = None
    _config = {
        # Alpha Vantage Configuration - REPLACE WITH YOUR KEY
        "alpha_vantage_api_key": "YOUR_ALPHA_VANTAGE_API_KEY",
        "alpha_vantage_endpoint_template": "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={apikey}",
        "alpha_vantage_symbols": ["IBM", "MSFT", "AAPL", "GOOGL"], # Symbols to fetch in round-robin
        "fetch_interval_alpha_vantage_s": 15.0, # Interval for Alpha Vantage (be mindful of free tier limits)

        # Mock WebSocket and CSV still available for demonstration
        "websocket_uri": "ws://mockfinance.com/stream",
        "csv_filepath": "mock_data.csv",
        "fetch_interval_ws_s": 0.5,
        "fetch_interval_csv_s": 2.0,

        "moving_average_window": 5,
        "price_alert_threshold": 108.0, # Adjust as needed based on real data
        "log_level": "INFO",
        "processing_delay_ms": 50,
    }

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            print("[ConfigManager] Creating Singleton instance")
            cls._instance = cls()
        return cls._instance

    def get(self, key, default=None):
        return self._config.get(key, default)

    def set(self, key, value):
        print(f"[ConfigManager] Setting {key} = {value}")
        self._config[key] = value

# --- Observer Pattern ---
class Observer(ABC):
    @abstractmethod
    def update(self, subject, event_data):
        pass

class Subject:
    def __init__(self):
        self._observers = set()

    def attach(self, observer: Observer):
        print(f"[{self.__class__.__name__}] Attaching observer: {observer.__class__.__name__}")
        self._observers.add(observer)

    def detach(self, observer: Observer):
        print(f"[{self.__class__.__name__}] Detaching observer: {observer.__class__.__name__}")
        self._observers.discard(observer)

    def notify(self, event_data=None):
        if not self._observers:
            return
        for observer in self._observers:
            try:
                observer.update(self, copy.deepcopy(event_data))
            except Exception as e:
                print(f"Error notifying observer {observer.__class__.__name__}: {e}")

class ConsoleNotifier(Observer):
    def update(self, subject, event_data):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}][ConsoleNotifier] Event from {subject.__class__.__name__}: {event_data}")

class LoggingModule(Observer):
    def update(self, subject, event_data):
        config = ConfigurationManager.get_instance()
        if config.get("log_level") == "INFO":
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}][Logger] INFO: Event detected from {subject.__class__.__name__}. Details: {event_data}")

class AlertGenerator(Observer):
    def update(self, subject, event_data):
        config = ConfigurationManager.get_instance()
        threshold = config.get('price_alert_threshold')
        if isinstance(subject, MovingAverageCalculator) and 'moving_average' in event_data:
            ma = event_data['moving_average']
            current_price = event_data.get('price', 'N/A')
            if ma > threshold:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"*** [{timestamp}][ALERT] *** {event_data.get('symbol', 'N/A')} Moving Average {ma:.2f} (Price: {current_price}) crossed threshold {threshold}!")
        elif 'price' in event_data and event_data['price'] > threshold and not ('moving_average' in event_data and event_data['moving_average'] > threshold) : # Avoid double alert
            # This part might be redundant if MA alerts are preferred, or adjust logic
            # print(f"*** [{timestamp}][ALERT] *** Raw Price {event_data['price']:.2f} for {event_data.get('symbol', 'N/A')} crossed threshold {threshold}!")
            pass


# --- Adapter Pattern (Data Ingestion) ---
class DataSourceAdapter(ABC):
    @abstractmethod
    async def fetch_data(self):
        pass

    async def close(self):
        """Optional method to clean up resources, like HTTP sessions."""
        pass

# --- Alpha Vantage Adapter ---
class AlphaVantageAdapter(DataSourceAdapter):
    """Adapts Alpha Vantage API to the DataSourceAdapter interface."""
    def __init__(self):
        self.config = ConfigurationManager.get_instance()
        self.api_key = self.config.get("alpha_vantage_api_key")
        self.endpoint_template = self.config.get("alpha_vantage_endpoint_template")
        self.symbols = self.config.get("alpha_vantage_symbols", ["IBM"]) # Default to IBM if not configured
        self.current_symbol_index = 0
        self._session = None # Initialize session to None
        print(f"[AlphaVantageAdapter] Initialized for symbols: {self.symbols}")
        if self.api_key == "YOUR_ALPHA_VANTAGE_API_KEY":
            print("\n" + "*"*60)
            print("WARNING: Alpha Vantage API key is not set!")
            print("Please replace 'YOUR_ALPHA_VANTAGE_API_KEY' in ConfigurationManager.")
            print("Alpha Vantage adapter will likely fail to fetch real data.")
            print("*"*60 + "\n")


    async def _get_session(self):
        """Creates an aiohttp.ClientSession if one doesn't exist."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            print("[AlphaVantageAdapter] Created new aiohttp.ClientSession")
        return self._session

    async def fetch_data(self):
        if self.api_key == "YOUR_ALPHA_VANTAGE_API_KEY":
            print("[AlphaVantageAdapter] Skipping fetch: API key not configured.")
            return None # Don't attempt to fetch if API key isn't set

        if not self.symbols:
            print("[AlphaVantageAdapter] No symbols configured to fetch.")
            return None

        session = await self._get_session()
        symbol_to_fetch = self.symbols[self.current_symbol_index]
        self.current_symbol_index = (self.current_symbol_index + 1) % len(self.symbols)

        url = self.endpoint_template.format(symbol=symbol_to_fetch, apikey=self.api_key)

        try:
            print(f"[AlphaVantageAdapter] Fetching data for {symbol_to_fetch} from {url.split('apikey=')[0]}...") # Hide API key in log
            async with session.get(url) as response:
                response.raise_for_status() # Raise an exception for HTTP errors 4xx/5xx
                raw_data = await response.json()
                # print(f"[AlphaVantageAdapter] Raw response for {symbol_to_fetch}: {raw_data}") # For debugging

                if "Global Quote" not in raw_data or not raw_data["Global Quote"]:
                    if "Note" in raw_data: # Alpha Vantage API limit note
                         print(f"[AlphaVantageAdapter] API Note for {symbol_to_fetch}: {raw_data['Note']}")
                    else:
                         print(f"[AlphaVantageAdapter] 'Global Quote' not found or empty in response for {symbol_to_fetch}.")
                    return None

                quote = raw_data["Global Quote"]
                price_str = quote.get("05. price")
                volume_str = quote.get("06. volume")

                if price_str is None or volume_str is None:
                    print(f"[AlphaVantageAdapter] Price or Volume missing in quote for {symbol_to_fetch}: {quote}")
                    return None

                normalized_data = {
                    "id": f"alphavantage_{uuid.uuid4()}",
                    "symbol": quote.get("01. symbol"),
                    "price": float(price_str),
                    "volume": int(volume_str),
                    "timestamp": time.time(), # Use current time as ingestion time
                                              # quote.get("07. latest trading day") gives date, not precise time
                    "source": "AlphaVantage"
                }
                return normalized_data
        except aiohttp.ClientError as e:
            print(f"[AlphaVantageAdapter] HTTP ClientError for {symbol_to_fetch}: {e}")
        except asyncio.TimeoutError:
            print(f"[AlphaVantageAdapter] Timeout fetching data for {symbol_to_fetch}")
        except Exception as e:
            print(f"[AlphaVantageAdapter] Error fetching or parsing data for {symbol_to_fetch}: {e} (Raw: {raw_data if 'raw_data' in locals() else 'N/A'})")
        return None

    async def close(self):
        """Closes the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            print("[AlphaVantageAdapter] Closed aiohttp.ClientSession")
        self._session = None


# --- Mock Adapters (WebSocket, CSV) for continued demonstration ---
class MockWebSocketClient:
    def __init__(self):
        self.connected = False
    async def connect(self, uri):
        print(f"Simulating WebSocket connect to {uri}")
        await asyncio.sleep(0.1)
        self.connected = True
    async def receive_message(self):
        if not self.connected: raise ConnectionError("WebSocket not connected")
        await asyncio.sleep(random.uniform(0.05, 0.2))
        return {"instrument": "GOOGL_mock", "value": random.uniform(1450,1550), "vol": random.randint(5000,20000), "ts": time.time()}

class WebSocketAdapter(DataSourceAdapter):
    def __init__(self):
        self.config = ConfigurationManager.get_instance()
        self.client = MockWebSocketClient()
        self.uri = self.config.get("websocket_uri")
        asyncio.create_task(self.client.connect(self.uri))
    async def fetch_data(self):
        try:
            raw_data = await self.client.receive_message()
            return {"id": f"ws_{uuid.uuid4()}", "symbol": raw_data.get("instrument"), "price": raw_data.get("value"),
                    "volume": raw_data.get("vol"), "timestamp": raw_data.get("ts"), "source": "WebSocketMock"}
        except Exception as e:
            print(f"Error in WebSocketAdapter: {e}")
            await asyncio.sleep(1)
            if hasattr(self.client, 'connect') and asyncio.iscoroutinefunction(self.client.connect):
                 asyncio.create_task(self.client.connect(self.uri))
            return None

class MockCsvFileReader:
    def __init__(self, filepath):
        self.filepath = filepath
        csv_content = "symbol,price,volume\nMSFT_mock,205.50,30000\nTSLA_mock,880.10,60000\nNVDA_mock,550.25,45000"
        self.file_simulator = io.StringIO(csv_content)
        self.reader = csv.DictReader(self.file_simulator)
        try:
            self.rows = list(self.reader)
            for row in self.rows: row['price'] = float(row['price']); row['volume'] = int(row['volume'])
        except Exception as e: print(f"Error parsing simulated CSV: {e}"); self.rows = []
        self.index = 0
    async def read_next_row(self):
        if not self.rows: return None
        await asyncio.sleep(random.uniform(0.1, 0.2))
        row = self.rows[self.index].copy()
        row['timestamp'] = time.time()
        self.index = (self.index + 1) % len(self.rows)
        return row

class CsvAdapter(DataSourceAdapter):
    def __init__(self):
        self.config = ConfigurationManager.get_instance()
        self.filepath = self.config.get("csv_filepath")
        self.reader = MockCsvFileReader(self.filepath)
    async def fetch_data(self):
        try:
            raw_data = await self.reader.read_next_row()
            if raw_data:
                normalized_data = raw_data
                normalized_data["id"] = f"csv_{uuid.uuid4()}"; normalized_data["source"] = "CSVMock"
                return normalized_data
            return None
        except Exception as e: print(f"Error in CsvAdapter: {e}"); return None

# --- Data Processing Pipeline (Processors & Factory Method) ---
class DataProcessor(ABC):
    @abstractmethod
    async def process(self, data):
        pass

class DataCleaner(DataProcessor):
    async def process(self, data):
        processing_delay = ConfigurationManager.get_instance().get("processing_delay_ms", 0) / 1000.0
        await asyncio.sleep(processing_delay)
        if not data or data.get('price') is None or data.get('price') <= 0:
            print(f"[DataCleaner] Filtering out invalid data ID {data.get('id', 'N/A')}: {data}")
            return None
        data['volume'] = max(0, data.get('volume', 0))
        return data

class MovingAverageCalculator(DataProcessor, Subject):
    def __init__(self):
        Subject.__init__(self)
        self.config = ConfigurationManager.get_instance()
        self.window_size = self.config.get("moving_average_window")
        self.price_histories = {}
    async def process(self, data):
        processing_delay = ConfigurationManager.get_instance().get("processing_delay_ms", 0) / 1000.0
        await asyncio.sleep(processing_delay)
        symbol = data.get('symbol'); price = data.get('price')
        if symbol and price is not None:
            if symbol not in self.price_histories:
                self.price_histories[symbol] = deque(maxlen=self.window_size)
            history = self.price_histories[symbol]
            history.append(price)
            if len(history) == self.window_size:
                moving_avg = sum(history) / self.window_size
                data['moving_average'] = moving_avg
                self.notify(event_data=data)
        return data

class ProcessorFactory:
    def __init__(self):
        self._processor_instances = {}
    def create_processor(self, processor_type: str) -> DataProcessor:
        if processor_type == "cleaner":
            if "cleaner" not in self._processor_instances: self._processor_instances["cleaner"] = DataCleaner()
            return self._processor_instances["cleaner"]
        elif processor_type == "moving_average":
            if "moving_average" not in self._processor_instances:
                print("[ProcessorFactory] Creating singleton MovingAverageCalculator instance")
                self._processor_instances["moving_average"] = MovingAverageCalculator()
            return self._processor_instances["moving_average"]
        else: raise ValueError(f"Unknown processor type: {processor_type}")

# --- Main Application Engine ---
class RealTimeProcessingEngine:
    def __init__(self):
        print("\nInitializing RealTimeProcessingEngine...")
        self.config = ConfigurationManager.get_instance()
        self.processor_factory = ProcessorFactory()

        self.adapters = {
            "AlphaVantage": AlphaVantageAdapter(), # Using the real data adapter
            "WebSocketMock": WebSocketAdapter(),
            "CSVMock": CsvAdapter()
        }
        print(f"Initialized adapters: {list(self.adapters.keys())}")

        self.pipeline_steps = ["cleaner", "moving_average"]
        print(f"Processing pipeline steps: {self.pipeline_steps}")
        self.pipeline_processors = [self.processor_factory.create_processor(step) for step in self.pipeline_steps]
        self._setup_observers()
        self._running = False
        self._tasks = []

    def _setup_observers(self):
        print("Setting up observers...")
        console_notifier = ConsoleNotifier(); logger = LoggingModule(); alert_generator = AlertGenerator()
        ma_processor = next((p for p in self.pipeline_processors if isinstance(p, MovingAverageCalculator)), None)
        if ma_processor:
            print("Attaching observers to MovingAverageCalculator...")
            ma_processor.attach(console_notifier); ma_processor.attach(logger); ma_processor.attach(alert_generator)
        else: print("Warning: MovingAverageCalculator not found; observers not attached.")

    async def _run_pipeline(self, data_item):
        current_data = data_item
        for processor in self.pipeline_processors:
            if current_data is None: break
            try: current_data = await processor.process(current_data)
            except Exception as e:
                print(f"Error processing data ID {data_item.get('id', 'N/A')} in {processor.__class__.__name__}: {e}")
                current_data = None; break

    async def _data_fetch_loop(self, adapter_name, adapter: DataSourceAdapter, interval_s: float):
        print(f"Starting fetch loop for {adapter_name} with interval {interval_s}s")
        while self._running:
            try:
                data = await adapter.fetch_data()
                if data: asyncio.create_task(self._run_pipeline(data))
                await asyncio.sleep(interval_s)
            except asyncio.CancelledError: print(f"Fetch loop for {adapter_name} cancelled."); break
            except Exception as e:
                print(f"Error in fetch loop for {adapter_name}: {e}")
                await asyncio.sleep(interval_s * 2 if interval_s > 0 else 2)

    async def start(self):
        if self._running: print("Engine already running."); return
        print("\nStarting RealTimeProcessingEngine...")
        self._running = True; self._tasks = []
        fetch_intervals = {
            "AlphaVantage": self.config.get("fetch_interval_alpha_vantage_s", 15.0),
            "WebSocketMock": self.config.get("fetch_interval_ws_s", 0.5),
            "CSVMock": self.config.get("fetch_interval_csv_s", 2.0)
        }
        for name, adapter in self.adapters.items():
            interval = fetch_intervals.get(name, 1.0)
            task = asyncio.create_task(self._data_fetch_loop(name, adapter, interval))
            self._tasks.append(task)
        print(f"Engine started with {len(self._tasks)} data source fetch loops.")
        try: await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError: print("Engine task gathering was cancelled.")
        finally: print("Engine fetch loops have completed or been cancelled.")

    async def stop(self):
        if not self._running: print("Engine not running."); return
        print("\nStopping RealTimeProcessingEngine...")
        self._running = False
        cancelled_tasks = []
        for task in self._tasks:
            if not task.done(): task.cancel(); cancelled_tasks.append(task)
        # Close adapter resources (e.g., aiohttp sessions)
        for adapter_name, adapter in self.adapters.items():
            if hasattr(adapter, 'close') and asyncio.iscoroutinefunction(adapter.close):
                try:
                    print(f"Closing resources for adapter: {adapter_name}")
                    await adapter.close()
                except Exception as e:
                    print(f"Error closing adapter {adapter_name}: {e}")
        if cancelled_tasks:
            try: await asyncio.gather(*cancelled_tasks, return_exceptions=True); print("All fetch tasks successfully cancelled.")
            except asyncio.CancelledError: print("Task cancellation gather itself was cancelled.")
            except Exception as e: print(f"Error during task cancellation gathering: {e}")
        self._tasks = []; print("Engine stopped.")

async def main_simulation(run_duration_seconds=60): # Increased default duration for real API
    engine = RealTimeProcessingEngine()
    engine_task = None
    try:
        engine_task = asyncio.create_task(engine.start())
        await asyncio.sleep(run_duration_seconds)
        print(f"\n--- Simulation duration ({run_duration_seconds}s) reached ---")
    except asyncio.CancelledError: print("Simulation run cancelled.")
    except Exception as e: print(f"An error occurred during simulation: {e}")
    finally:
        if engine: await engine.stop()
        if engine_task and not engine_task.done():
             engine_task.cancel()
             try: await engine_task
             except asyncio.CancelledError: pass
        print("\nSimulation finished.")

if __name__ == "__main__":
    simulation_time = 65 # Run for a bit longer to see some Alpha Vantage fetches
    print(f"--- Starting Financial Data Processing Simulation for {simulation_time} seconds ---")
    print(">>> REMEMBER TO REPLACE 'YOUR_ALPHA_VANTAGE_API_KEY' IN THE CODE! <<<")
    try: asyncio.run(main_simulation(run_duration_seconds=simulation_time))
    except KeyboardInterrupt: print("\nSimulation interrupted by user (Ctrl+C).")
    print("--- Simulation Complete ---")