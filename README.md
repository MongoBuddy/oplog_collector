# MongoDB Oplog Collector & Management System

## 1. Overview

This project provides a web-based management tool and a background collector script designed for continuously capturing MongoDB oplogs (operation logs) from your source clusters. The primary purpose of collecting these oplogs is to enable Point-in-Time Recovery (PITR) capabilities when used in conjunction with a base backup (like a volume snapshot or a `mongodump`).

While this system manages the collection and storage of oplogs, the actual process of applying these oplogs to a restored backup for PITR would typically be handled by a separate utility (e.g., the [pitr_cli tool](https://github.com/MongoBuddy/pitr_cli), or custom scripts).

The core components included in this repository are:

* **Web UI (`index.html`)**: A browser-based interface to register new MongoDB source clusters that need oplog collection, monitor the status of their Oplog Collectors, and manage these collector processes.

* **Backend Server (`app.py`)**: A Flask-based server that:
  * Manages the lifecycle of Oplog Collector subprocesses.
  * Provides an API for the Web UI to interact with.
  * Stores cluster registration details and collector status in a Central Metadata MongoDB.

* **Oplog Collector (`scripts/oplog_collector.py`)**: A Python script that runs as a daemon for each registered source cluster. It:
  * Connects to the source MongoDB cluster and watches its change stream (oplog).
  * Captures various database operations (inserts, updates, deletes, DDL changes, etc.).
  * Stores these captured oplog entries, along with resume tokens, into a dedicated Oplog Data Store (another MongoDB instance).
  * Periodically reports its operational status to the Central Metadata DB.

* **`requirements.txt`**: Lists the necessary Python dependencies for the backend server.

## 2. Purpose of Oplog Collection for PITR

Point-in-Time Recovery (PITR) allows you to restore a database to a very specific moment in time, typically between your regular full backups. This is crucial for recovering from accidental data deletion or corruption that occurred after your last full backup.

The process generally involves:

1. **Restoring a Base Backup**: First, you restore your database from a known good full backup (e.g., an LVM snapshot, AWS EBS snapshot, or a `mongodump` taken at time `T_snapshot`). This brings your database to the state it was in at `T_snapshot`.

2. **Applying Oplogs (Rolling Forward)**: The `oplog_collector.py` script in this project continuously captures all changes (oplogs) happening on your source database *after* `T_snapshot` and stores them. To reach a specific recovery point `T_recovery_point` (where `T_snapshot < T_recovery_point`), you would use a separate utility to apply these stored oplogs from `T_snapshot` up to `T_recovery_point` onto the restored base backup.

This system provides the essential first part: **reliable and continuous collection of oplogs**.

## **3\. Key Features**

### **3.1. Web UI (`index.html` \+ `app.py`)**

* **Source Cluster Registration**:  
![image](https://github.com/user-attachments/assets/ee865744-7d09-4bd8-b90e-7182359d1387)

  * Register new MongoDB source clusters via the Web UI, specifying a display name, connection URI, the URI for the Oplog Data Store, and an optional specific database to monitor.  
  * Automatically starts an Oplog Collector process for each newly registered cluster.  
* **Oplog Collector Monitoring & Management**:  
![image](https://github.com/user-attachments/assets/ef8a32f9-4378-456e-b104-7ce5dd55ff6c)

  * View a list of registered clusters and the real-time status of their Oplog Collectors (e.g., starting, running, stopped, retrying connection, error).  
  * Display key metrics for each collector: First Oplog Time (earliest oplog in its store, reported by collector), Last Collected Oplog Time, and the current time GAP (lag).  
  * Visual warnings if the GAP exceeds a defined threshold (e.g., 10 minutes).  
  * Manually start or stop Oplog Collector processes via the UI.  
  * **Reset Oplog Store**: If a collector encounters a non-resumable change stream error (e.g., resume token lost), the UI provides an option to reset. This action stops the collector, **wipes all collected oplog data and the resume token for that specific cluster from its Oplog Data Store**, and then restarts the collector to begin fresh from the current time.  
* **Cluster De-registration**:  
  * Remove a registered cluster from the management system. This action will:  
    1. Stop the associated Oplog Collector process.  
    2. **Wipe all collected oplog data and the resume token for that cluster from its Oplog Data Store.**  
    3. Delete the cluster's metadata from the Central Metadata DB.  
* **Automatic Collector Restart**:  
  * The `app.py` backend attempts to restart Oplog Collectors that were in a running or recoverable state when the backend application itself starts up. (Note: Collectors in an `error_resume_token_lost` state are not automatically restarted and require manual "Reset Oplog Store" action).  
* **Log Rotation**:  
  * Both `app.py` and `scripts/oplog_collector.py` implement log rotation. Log files are rotated when they reach a certain size (default 100MB), and a configurable number of backup log files are kept (default 10).  
  * Log file names follow the pattern: `app.log`, `app.log.1`, ... and `oplog_collector_<cluster_name>.log`, `oplog_collector_<cluster_name>.log.1`, ...

### **3.2. Oplog Collector (`scripts/oplog_collector.py`)**

* Watches the change stream of a specified source MongoDB cluster (or a specific database within it), using `show_expanded_events=True` for MongoDB 6.0+.  
* Collects oplog entries for various operation types (`insert`, `update`, `delete`, `replace`, `drop`, `rename`, `dropDatabase`, `invalidate`, `createIndexes`, `dropIndexes`, `create`, `modify`).  
* Stores collected oplog entries (including `operationDescription` for DDLs like `createIndexes`, `dropIndexes`, `create`, `modify`) and resume tokens in a separate MongoDB instance (the Oplog Data Store).  
* Periodically reports its status (PID, running state, last processed oplog time, first oplog time in its store, error messages) to the Central Metadata DB.  
* Automatically retries connection to the source DB on failure, up to a configurable maximum.  
* Handles `NonResumableChangeStreamError` by reporting a specific error state (`error_resume_token_lost`) to the Central Metadata DB and stopping, awaiting manual intervention ("Reset Oplog Store" via UI).  
* Handles `SIGINT` and `SIGTERM` signals for graceful shutdown, updating its status.  
* Log level can be controlled via the `OPLOG_LOG_LEVEL` environment variable (default: `INFO`; available: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). Log rotation is implemented.

## **4\. System Architecture**

1. **Source MongoDB Cluster(s)**: The original databases generating oplogs.  
2. **Oplog Collector (`scripts/oplog_collector.py`)**: One daemon process per source cluster, collecting oplogs.  
3. **Oplog Data Store (MongoDB)**: A separate MongoDB instance/database where collected oplog entries and resume tokens are stored. Data is organized per cluster (e.g., `oplogs_myCluster` collection, `resume_tokens_for_myCluster` collection).  
4. **Central Metadata DB (MongoDB)**: Used by `app.py` for management.  
   * `source_clusters_info` collection: Stores details of registered clusters and the current status of their collectors.  
5. **Backend Server (`app.py`)**: Handles UI API requests, manages collector subprocesses, and interacts with the Central Metadata DB.  
6. **Web UI (`index.html`)**: The front-end interface for users.

## **5\. Directory Structure**

It is assumed your project has the following structure:

```
/your_project_root/  
├── app.py  
├── index.html  
├── requirements.txt  
└── scripts/  
    └── oplog_collector.py  
└── logs/            (Created automatically for log files)  
    ├── app.log  
    └── oplog_collector_<cluster_name>.log
```

## **6\. Setup and Installation**

### **6.1. Prerequisites**

* Python 3.7+  
* MongoDB (for Source Clusters, Oplog Data Store, and Central Metadata DB)  
* `pip` (Python package installer)

### **6.2. Install Python Libraries**

In your project root directory, ensure you have a `requirements.txt` file with the following content:
```
Flask  
pymongo  
Flask-CORS
```
Then, install the dependencies:
```
pip install -r requirements.txt
```
### **6.3. Environment Variables**

The application and scripts use environment variables for configuration.

* **For `app.py` (Backend Server)**:  
  * `CENTRAL_METADATA_DB_URI`: MongoDB connection URI for the Central Metadata DB.  
    (Default: `mongodb://localhost:27017/oplog_manager_orchestrator_db`)  
  * `APP_LOG_LEVEL`: Log level for `app.py` and also passed to spawned Collector processes as `OPLOG_LOG_LEVEL`.  
    (Default: `INFO`. Options: `DEBUG`, `INFO`, `WARNING`, `ERROR`,`CRITICAL`)  


### **6.4. Running the Application**

1. Start the Backend Server (`app.py`):  
   Navigate to your project root directory and run:
   ```
   python app.py
   ```
   The Flask server will start. This server provides the API endpoints for the UI. Ensure your MongoDB instances (Central Metadata DB, and any Oplog Data Stores you plan to use) are running and accessible.  
3. Access the Web UI (`index.html`):  
   Since `app.py` does not serve `index.html` directly, you need to serve it using a simple HTTP server. Navigate to your project root directory (where `index.html` is located) in a separate terminal and run:  
```
   python -m http.server 8000
```
   Then, open your web browser and go to `http://localhost:8000/index.html`. The JavaScript in `index.html` is configured to make API calls to the Flask backend running on port 4999\.

## **7\. Log Management**

* **`app.py`**: Logs to `logs/app.log` with rotation (default 100MB per file, 10 backups). Log level is controlled by `APP_LOG_LEVEL` (default `INFO`).  
* **`scripts/oplog_collector.py`**: Logs to `logs/oplog_collector_<cluster_name>.log` with rotation (parameters set by `app.py` via environment variables, defaulting to 100MB and 10 backups). Log level is controlled by `OPLOG_LOG_LEVEL` (default `INFO`, can be overridden by `APP_LOG_LEVEL` when launched by `app.py`).

To enable `DEBUG` level logging for troubleshooting, set the respective environment variable before starting `app.py` (e.g., `export APP_LOG_LEVEL=DEBUG` on Linux/macOS or `set APP_LOG_LEVEL=DEBUG` on Windows).

## **8\. Oplog Data Store Retention (TTL Index)**

To prevent the Oplog Data Store from growing indefinitely, it's crucial to implement a data retention policy. A common way to achieve this in MongoDB is by using a Time-To-Live (TTL) index on a date field in your oplog documents.

The `oplog_collector.py` script saves a `wall` field (UTC datetime) with each oplog entry. You can create a TTL index on this field in each `oplogs_<cluster_name>` collection.

Example: Setting a 30-day TTL on Oplogs

Connect to your Oplog Data Store MongoDB using the `mongosh` and execute the following command for each cluster's oplog collection you want to manage. Replace `oplogs_myClusterName` with the actual collection name.
```
// Connect to the database where your oplog collections are stored  
// use oplog_prod_db; 

// For each cluster's oplog collection:  
db.oplogs_myClusterName.createIndex(  
  { "wall": 1 },  
  { expireAfterSeconds: 2592000 } // 30 days in seconds (30 * 24 * 60 * 60)  
);
```
Explanation:

* `{ "wall": 1 }`: Creates an ascending index on the `wall` field.  
* `{ expireAfterSeconds: 2592000 }`: Configures the index as a TTL index. Documents will be automatically deleted 2,592,000 seconds (30 days) after the time specified in their `wall` field.

Important Notes for TTL Indexes:

* The field used for TTL must be a BSON Date type or an array of BSON Date types. The wall field is stored as a UTC datetime object by the collector, which is compatible.  
* TTL indexes are efficient for removing large amounts of data but are not instant. MongoDB's background TTL thread runs periodically (default every 60 seconds) to remove expired documents.  
* Choose an `expireAfterSeconds` value that aligns with your organization's data retention policies and PITR requirements. You need to retain oplogs long enough to cover the period between your oldest usable base backup and your desired recovery points.  

## **9\. Troubleshooting Notes**

* If a collector is in an error state, check its specific log file in the `logs/` directory for details.  
* The `error_resume_token_lost` status indicates that the collector's last known position in the source oplog is no longer available. This requires a manual "Reset Oplog Store" action via the UI.  
* Ensure all MongoDB URIs are correctly formatted and accessible from where the `app.py` and `scripts/oplog_collector.py` scripts are running.
