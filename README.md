# Real-time Financial Data Stream Processing Engine

This project is a Python-based simulation of a real-time financial data stream processing engine. It demonstrates the application of common software design patterns to build a modular, extensible, and maintainable system capable of ingesting, processing, and analyzing financial data streams. The engine can connect to mock data sources and has been extended to fetch real (though potentially delayed) financial data from Alpha Vantage.

## Features

* **Multiple Data Sources:** Ingests data from various sources:
    * **Alpha Vantage:** Fetches real stock quotes using the Alpha Vantage API.
    * **Mock WebSocket Stream:** Simulates a high-frequency WebSocket feed.
    * **Mock CSV File Reader:** Simulates reading historical or batch data from a CSV file.
* **Modular Data Ingestion:** Uses the **Adapter Pattern** to normalize data from different sources into a unified format.
* **Flexible Data Processing Pipeline:**
    * Implements a configurable pipeline for processing data items.
    * Uses the **Factory Pattern** (Simple Factory) to create different data processors (e.g., data cleaner, moving average calculator).
* **Real-time Analysis:** Performs calculations like Simple Moving Average (SMA) on the incoming data.
* **Event-Driven Notifications:**
    * Employs the **Observer Pattern** for event notification.
    * Components (e.g., `MovingAverageCalculator`) act as subjects, notifying observers (e.g., console logger, alert generator) of significant events (like price threshold breaches).
* **Centralized Configuration:** Utilizes the **Singleton Pattern** for managing application-wide configurations (API keys, endpoints, processing parameters).
* **Asynchronous Operations:** Leverages Python's `asyncio` library for concurrent data fetching and processing, essential for real-time responsiveness.
* **Extensible Design:** The use of design patterns makes it easier to add new data sources, processing steps, or notification mechanisms.

## Design Patterns Used

* **Singleton Pattern:** `ConfigurationManager` ensures a single instance for global access to configuration settings.
* **Adapter Pattern:** `DataSourceAdapter` interface and its concrete implementations (`AlphaVantageAdapter`, `WebSocketAdapter`, `CsvAdapter`) allow the system to work with various data source APIs seamlessly.
* **Factory Pattern (Simple Factory):** `ProcessorFactory` is used to create instances of different `DataProcessor` objects, decoupling the engine core from concrete processor classes.
* **Observer Pattern:** `Subject` and `Observer` classes enable a decoupled way for different parts of the system to react to events (e.g., new data processed, alerts triggered).

## Project Structure
.
├── financial_engine.py         # Main application code
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── docs/                       # (Optional) For additional documentation
└── images/
└── uml_class_diagram.png # &lt;&lt; 

## System Architecture (UML Class Diagram)

Below is the UML class diagram illustrating the main components and their relationships within the system.

![UML Class Diagram](./docs/images/uml_class_diagram.png)
*(If the image is in the root directory, use: ![UML Class Diagram](./uml_class_diagram.png))*

## Prerequisites

* Python 3.7+
* An Alpha Vantage API Key (for fetching real financial data)

## Setup

1.  **Clone the Repository (if applicable):**
    ```bash
    git clone <your-repository-url>
    cd <repository-directory>
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    ```
    Activate the environment:
    * Windows: `.\venv\Scripts\activate`
    * macOS/Linux: `source venv/bin/activate`

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Alpha Vantage API Key:**
    * Open the `financial_engine.py` file.
    * Locate the `ConfigurationManager` class.
    * Find the `_config` dictionary within this class.
    * Replace `"YOUR_ALPHA_VANTAGE_API_KEY"` with your actual Alpha Vantage API key:
        ```python
        _config = {
            "alpha_vantage_api_key": "YOUR_ACTUAL_API_KEY_HERE",
            # ... other configurations
        }
        ```
    * **Important:** Be mindful of the Alpha Vantage free tier API limits (e.g., 25 requests per day or 5 requests per minute). The default fetch interval for Alpha Vantage in the script is set conservatively.

## Running the Engine

To start the financial data processing engine simulation, run the following command from the project's root directory:

```bash
python financial_engine.py
