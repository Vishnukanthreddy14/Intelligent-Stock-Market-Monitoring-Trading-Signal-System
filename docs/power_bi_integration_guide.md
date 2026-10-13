# Power BI Real-Time Dashboard Integration Guide

This guide details how to connect **Power BI Desktop** directly to the PostgreSQL database for real-time stock market streaming visualization as depicted in the project architecture.

---

## 1. Connection Mode: DirectQuery vs. Import

To achieve true real-time streaming updates in Power BI without having to manually trigger data refreshes:
* **DirectQuery Mode (Recommended)**: Power BI queries PostgreSQL on-demand every time the dashboard renders or on automatic page refresh intervals. No data is stored inside the `.pbix` file.
* **Automatic Page Refresh (APR)**: DirectQuery supports page auto-refresh intervals as low as **1 to 5 seconds** (when published to Power BI Premium/Fabric capacity, or within Power BI Desktop).

---

## 2. Step-by-Step Connection Instructions

### Step 1: Install PostgreSQL ODBC / Npgsql Driver
1. Ensure the PostgreSQL driver is installed:
   * Power BI uses the **Npgsql .NET Data Provider** or the standard **PostgreSQL ODBC Driver (x64)**.
2. In Power BI Desktop, click **Get Data** > **More...** > **Database** > **PostgreSQL database** > **Connect**.

### Step 2: Configure Server & Database Parameters
* **Server**: `localhost:5432` (or your remote host IP if running across VMs)
* **Database**: `stock_analytics`
* **Data Connectivity mode**: Select **DirectQuery**
* **Advanced options** (Optional SQL statement):
  You can connect directly to the pre-built view:
  ```sql
  SELECT * FROM v_live_stock_dashboard;
  ```
  Or import all three tables:
  * `stock_ticks`
  * `stock_features`
  * `stock_signals`

### Step 3: Enter Database Credentials
* **User name**: `stock_user`
* **Password**: `stock_password`
* **Encryption**: Select *Optional* or *Disable* (for local Docker container).

---

## 3. Recommended Data Model & Relationships

When importing multiple tables in DirectQuery mode, configure 1-to-Many relationships:
* `stock_ticks (id)` <--> `stock_features (id)` on `symbol` and `timestamp`
* `stock_ticks (id)` <--> `stock_signals (id)` on `symbol` and `timestamp`

---

## 4. Key DAX Measures

Add these calculated measures to your Power BI model for dynamic KPI rendering:

### 1. Latest Stock Price
```dax
Latest Price = 
VAR LastTick = MAX(stock_ticks[timestamp])
RETURN
    CALCULATE(
        MAX(stock_ticks[price]),
        stock_ticks[timestamp] = LastTick
    )
```

### 2. Price Percentage Change (vs Open)
```dax
Price Change Pct = 
VAR CurrPrice = [Latest Price]
VAR OpenPrice = CALCULATE(MAX(stock_ticks[open_price]), LASTDATE(stock_ticks[timestamp]))
RETURN
    DIVIDE(CurrPrice - OpenPrice, OpenPrice, 0)
```

### 3. Current 1-Min Simple Moving Average (SMA)
```dax
SMA 1Min = 
CALCULATE(
    MAX(stock_features[sma_1m]),
    LASTDATE(stock_features[timestamp])
)
```

### 4. Current 5-Min Simple Moving Average (SMA)
```dax
SMA 5Min = 
CALCULATE(
    MAX(stock_features[sma_5m]),
    LASTDATE(stock_features[timestamp])
)
```

### 5. Signal Color Conditional Formatting
```dax
Signal Color = 
SWITCH(
    SELECTEDVALUE(stock_signals[signal]),
    "BUY", "#10B981",    // Green
    "SELL", "#EF4444",   // Red
    "HOLD", "#F59E0B",   // Yellow / Amber
    "#6B7280"            // Neutral Gray
)
```

---

## 5. Visual Dashboard Configuration (Matching the Architecture Diagram)

| Visual Element | Visual Type | Data Fields & Configurations |
|---|---|---|
| **Top KPI Cards** | Multi-row Card / 5 Card Visuals | `symbol`, `Latest Price`, `Price Change Pct` formatted as `+0.85%` / `-0.30%` |
| **Price vs Time** | Line Chart | **X-Axis**: `timestamp` (Time), **Y-Axis**: `price` and `sma_1m`, **Legend**: Symbol |
| **Day High / Low** | Card / KPI | `MAX(stock_ticks[high_price])` and `MIN(stock_ticks[low_price])` |
| **Moving Averages** | Card Visuals | `[SMA 1Min]` and `[SMA 5Min]` |
| **Current Signal** | Card / Button | `SELECTEDVALUE(stock_signals[signal])` formatted with `[Signal Color]` |
| **Trend & Signals Table** | Table Visual | Columns: `timestamp`, `symbol`, `price`, `signal`, `confidence`. Background color of `signal` linked to `[Signal Color]` measure. |

---

## 6. Enabling Automatic Page Refresh (APR)

1. In Power BI Desktop, click on empty canvas space (no visual selected).
2. Go to the **Format** pane (paint roller icon).
3. Toggle **Page refresh** to **ON**.
4. Set **Refresh type**: *Auto page refresh*.
5. Set **Refresh every**: `5 Seconds`.

Your Power BI dashboard will now poll PostgreSQL every 5 seconds and display real-time live trading signals, rolling averages, and incoming price ticks!
