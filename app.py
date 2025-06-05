from flask import Flask, request, jsonify
from flask_cors import CORS
import uuid
import datetime
import time
import sys
import os
import subprocess 
import signal 
from pymongo import MongoClient, UpdateOne, ASCENDING 
from pymongo.errors import PyMongoError
import threading 
import logging 
from logging.handlers import RotatingFileHandler 

# --- Flask App Setup ---
app = Flask(__name__)
CORS(app) 

# --- Application Configuration ---
APP_CONFIG = {
    "CENTRAL_METADATA_DB_URI": os.environ.get("CENTRAL_METADATA_DB_URI", "mongodb://localhost:27017/oplog_manager_orchestrator_db"),
    "MANAGED_CLUSTERS_COLLECTION": "source_clusters_info", 
    "OPLOG_COLLECTOR_SCRIPT_PATH": os.path.join(os.path.dirname(__file__), "scripts", "oplog_collector.py"),
    "LOG_DIR": os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs"), 
    "APP_LOG_FILE": "app.log", 
    "LOG_MAX_BYTES": 100 * 1024 * 1024, # 100MB
    "LOG_BACKUP_COUNT": 10, 
}

# --- Logger Setup ---
if not os.path.exists(APP_CONFIG["LOG_DIR"]):
    os.makedirs(APP_CONFIG["LOG_DIR"])

app_log_file = os.path.join(APP_CONFIG["LOG_DIR"], APP_CONFIG["APP_LOG_FILE"])
file_handler = RotatingFileHandler(
    app_log_file,
    maxBytes=APP_CONFIG["LOG_MAX_BYTES"],
    backupCount=APP_CONFIG["LOG_BACKUP_COUNT"],
    encoding='utf-8' 
)
log_level_str = os.environ.get("APP_LOG_LEVEL", "INFO").upper()
numeric_log_level = getattr(logging, log_level_str, logging.INFO)

file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(numeric_log_level)

app.logger.addHandler(file_handler) 
app.logger.setLevel(numeric_log_level)
app.logger.info("Flask application logger initialized.")


# --- Global Variables for Subprocess Management ---
active_collector_processes = {} 

# --- Central Metadata Database Client ---
try:
    central_meta_client = MongoClient(APP_CONFIG["CENTRAL_METADATA_DB_URI"], serverSelectionTimeoutMS=5000)
    central_meta_client.admin.command('ping') 
    central_meta_db = central_meta_client.get_database() 
    
    clusters_collection = central_meta_db[APP_CONFIG["MANAGED_CLUSTERS_COLLECTION"]]
    
    app.logger.info(f"Successfully connected to Central Metadata DB: {APP_CONFIG['CENTRAL_METADATA_DB_URI']}")
    clusters_collection.create_index("name", unique=True) 
except PyMongoError as e:
    app.logger.fatal(f"FATAL: Could not connect to Central Metadata DB at {APP_CONFIG['CENTRAL_METADATA_DB_URI']}: {e}")
    sys.exit(1) 

# --- Helper Functions ---
def get_utc_timestamp_iso():
    """ Returns the current UTC time as an ISO format string. """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def is_process_running(pid):
    """ Checks if a process with the given PID is running. """
    if pid is None: 
        return False
    try:
        os.kill(pid, 0) 
    except OSError: 
        return False
    else: 
        return True

def update_cluster_status_in_db(cluster_doc_id, status_data, collection_ref=None):
    """ Updates the cluster status in the central DB. """
    target_collection = collection_ref if collection_ref is not None else clusters_collection
    try:
        update_fields = {f"collector_status.{k}": v for k, v in status_data.items()}
        update_fields["collector_status.last_app_checked_utc"] = get_utc_timestamp_iso() 
        target_collection.update_one(
            {"_id": cluster_doc_id},
            {"$set": update_fields}
        )
        app.logger.debug(f"Updated status for cluster {cluster_doc_id} in DB: {status_data}")
    except PyMongoError as e:
        app.logger.error(f"Error updating status for cluster {cluster_doc_id} in DB: {e}")

# --- Oplog Collector Management ---
def start_oplog_collector_process(cluster_doc, collection_ref=None):
    """ Launches the oplog_collector.py script as a subprocess. """
    target_clusters_collection = collection_ref if collection_ref is not None else clusters_collection 
    cluster_id = cluster_doc["_id"]
    cluster_name = cluster_doc["name"] 
    
    if cluster_id in active_collector_processes:
        process = active_collector_processes[cluster_id]
        if process.poll() is None: 
            app.logger.warning(f"Oplog collector for cluster {cluster_name} (ID: {cluster_id}, PID: {process.pid}) is already managed and running by this app instance.")
            return
        else: 
            app.logger.info(f"Oplog collector for cluster {cluster_name} (ID: {cluster_id}, PID: {process.pid}) was found in active list but terminated. Removing and attempting restart.")
            del active_collector_processes[cluster_id]

    existing_pid = cluster_doc.get("collector_status", {}).get("pid")
    if is_process_running(existing_pid):
        app.logger.warning(f"An orphaned oplog collector process (PID: {existing_pid}) might be running for cluster {cluster_name}. "
                           f"This app instance will start a new managed process. Consider stopping the orphaned process manually if needed.")

    env_vars = os.environ.copy()
    env_vars["SOURCE_MONGO_URI"] = cluster_doc["source_connection_string"]
    env_vars["OPLOG_STORE_URI"] = cluster_doc["oplog_data_store_uri"] 
    env_vars["CLUSTER_NAME"] = cluster_name 
    if cluster_doc.get("change_stream_target_db"):
        env_vars["CHANGE_STREAM_TARGET_DB"] = cluster_doc["change_stream_target_db"]
    
    env_vars["OPLOG_MANAGER_CENTRAL_DB_URI"] = APP_CONFIG["CENTRAL_METADATA_DB_URI"]
    env_vars["OPLOG_MANAGER_CLUSTER_ID"] = cluster_id 
    env_vars["OPLOG_MANAGER_COLLECTION_NAME"] = APP_CONFIG["MANAGED_CLUSTERS_COLLECTION"]
    env_vars["OPLOG_LOG_LEVEL"] = os.environ.get("APP_LOG_LEVEL", "INFO") 

    collector_log_file = os.path.join(APP_CONFIG["LOG_DIR"], f"oplog_collector_{cluster_name}.log")
    env_vars["COLLECTOR_LOG_FILE"] = collector_log_file
    env_vars["COLLECTOR_LOG_MAX_BYTES"] = str(APP_CONFIG["LOG_MAX_BYTES"])
    env_vars["COLLECTOR_LOG_BACKUP_COUNT"] = str(APP_CONFIG["LOG_BACKUP_COUNT"])

    try:
        script_path = APP_CONFIG["OPLOG_COLLECTOR_SCRIPT_PATH"]
        if not os.path.exists(script_path):
            app.logger.error(f"Oplog collector script not found at {script_path}")
            update_cluster_status_in_db(cluster_id, {"status": "error", "error_message": "Collector script not found"}, target_clusters_collection)
            return

        process = subprocess.Popen([sys.executable, script_path], env=env_vars)
        active_collector_processes[cluster_id] = process
        app.logger.info(f"Started oplog_collector.py for cluster {cluster_name} (ID: {cluster_id}, PID: {process.pid}). Log file: {collector_log_file}")
        update_cluster_status_in_db(cluster_id, {"status": "starting", "pid": process.pid, "error_message": None}, target_clusters_collection)
        
    except Exception as e:
        app.logger.error(f"Failed to start oplog_collector.py for cluster {cluster_name}: {e}")
        update_cluster_status_in_db(cluster_id, {"status": "error", "error_message": str(e)}, target_clusters_collection)

def stop_oplog_collector_process(cluster_id, collection_ref=None):
    """ Stops the oplog_collector.py subprocess for the given cluster_id. """
    target_clusters_collection = collection_ref if collection_ref is not None else clusters_collection 
    process_stopped_by_app = False
    if cluster_id in active_collector_processes:
        process = active_collector_processes[cluster_id]
        if process.poll() is None: 
            app.logger.info(f"Stopping oplog_collector.py for cluster ID {cluster_id} (PID: {process.pid})...")
            try:
                process.send_signal(signal.SIGTERM) 
                process.wait(timeout=10) 
                app.logger.info(f"Oplog collector for cluster ID {cluster_id} (PID: {process.pid}) terminated by app.")
            except subprocess.TimeoutExpired:
                app.logger.warning(f"Oplog collector for cluster ID {cluster_id} (PID: {process.pid}) did not terminate gracefully. Forcing kill.")
                process.kill()
            except Exception as e:
                app.logger.error(f"Error stopping collector for {cluster_id}: {e}. Forcing kill.")
                process.kill() 
            process_stopped_by_app = True
        else:
            app.logger.info(f"Oplog collector for cluster ID {cluster_id} (PID: {process.pid}) was already stopped (managed by app).")
        del active_collector_processes[cluster_id]
    
    cluster_doc_for_stop = target_clusters_collection.find_one({"_id": cluster_id})
    if cluster_doc_for_stop: 
        existing_pid = cluster_doc_for_stop.get("collector_status", {}).get("pid")
        if existing_pid and (not process_stopped_by_app or (process_stopped_by_app and existing_pid != (process.pid if 'process' in locals() else None) ) ): 
            if is_process_running(existing_pid):
                app.logger.warning(f"Attempting to stop an orphaned oplog collector process (PID: {existing_pid}) for cluster ID {cluster_id}.")
                try:
                    os.kill(existing_pid, signal.SIGTERM)
                    time.sleep(2) 
                    if not is_process_running(existing_pid):
                        app.logger.info(f"Orphaned process (PID: {existing_pid}) for cluster {cluster_id} likely stopped.")
                    else:
                        app.logger.warning(f"Orphaned process (PID: {existing_pid}) for cluster {cluster_id} might still be running after SIGTERM. Consider manual kill.")
                except Exception as e:
                    app.logger.error(f"Error sending SIGTERM to orphaned process {existing_pid} for cluster {cluster_id}: {e}")
    
    if cluster_doc_for_stop:
        update_cluster_status_in_db(cluster_id, {"status": "stopped", "pid": None}, target_clusters_collection)
    return True

# --- Application startup: Restart previously running collectors ---
def restart_collectors_on_startup():
    app.logger.info("Checking for collectors to restart on application startup...")
    try:
        statuses_to_restart = [
            "running", "running_basic_check", "starting", 
            "resetting_oplog_store", "running_orphaned", 
            "error", 
            "retrying_connection",
            "error_max_retries_reached" 
        ]
        previously_active_clusters = clusters_collection.find({
            "collector_status.status": {"$in": statuses_to_restart}
        })
        
        restarted_count = 0
        for cluster_doc in previously_active_clusters:
            cluster_id = cluster_doc["_id"]
            cluster_name = cluster_doc["name"]
            last_known_status = cluster_doc.get("collector_status", {}).get("status")
            
            if last_known_status == "error_resume_token_lost":
                app.logger.warning(f"Cluster '{cluster_name}' (ID: {cluster_id}) is in 'error_resume_token_lost' state. Manual 'Oplog Reset' is required. Skipping automatic restart.")
                continue

            app.logger.info(f"Cluster '{cluster_name}' (ID: {cluster_id}) had status '{last_known_status}'. Attempting to restart collector.")
            start_oplog_collector_process(cluster_doc, collection_ref=clusters_collection)
            restarted_count += 1
            time.sleep(0.5) 

        if restarted_count > 0:
            app.logger.info(f"Attempted to restart {restarted_count} collector(s).")
        else:
            app.logger.info("No collectors required restart based on previous status (or all were in 'error_resume_token_lost' state).")

    except PyMongoError as e:
        app.logger.error(f"Database error during collector restart check: {e}")
    except Exception as e:
        app.logger.error(f"Unexpected error during collector restart check: {e}")
    app.logger.info("Collector restart check finished.")


# --- API Endpoints ---
@app.route('/api/clusters', methods=['POST'])
def register_new_cluster_api():
    return register_new_cluster() 

def register_new_cluster(): 
    data = request.json
    required_fields = ["name", "connection_string", "oplog_target_store_uri"]
    if not all(field in data for field in required_fields):
        return jsonify({"message": "Missing required fields: name, connection_string, oplog_target_store_uri"}), 400

    cluster_name = data["name"]
    if clusters_collection.find_one({"name": cluster_name}):
        return jsonify({"message": f"Cluster with name '{cluster_name}' already exists."}), 409

    cluster_doc_id = str(uuid.uuid4())
    new_cluster_document = {
        "_id": cluster_doc_id,
        "name": cluster_name,
        "source_connection_string": data["connection_string"],
        "oplog_data_store_uri": data["oplog_target_store_uri"],
        "change_stream_target_db": data.get("change_stream_target_db"), 
        "registration_utc": get_utc_timestamp_iso(),
        "collector_status": { 
            "status": "pending_registration", 
            "last_app_checked_utc": get_utc_timestamp_iso(),
            "first_oplog_time_utc": None, 
            "last_collector_reported_utc": None,
            "last_oplog_time_utc": None,
            "pid": None,
            "error_message": None
        }
    }
    try:
        clusters_collection.insert_one(new_cluster_document)
        app.logger.info(f"Registered cluster '{cluster_name}' with ID {cluster_doc_id}.")
        start_oplog_collector_process(new_cluster_document) 

        response_doc = {**new_cluster_document, "id": new_cluster_document["_id"]}
        del response_doc["_id"] 
        return jsonify(response_doc), 201

    except PyMongoError as e:
        app.logger.error(f"Database error registering cluster '{cluster_name}': {e}")
        return jsonify({"message": f"Database error: {e}"}), 500
    except Exception as e:
        app.logger.error(f"Unexpected error registering cluster '{cluster_name}': {e}")
        return jsonify({"message": f"Server error: {e}"}), 500


@app.route('/api/clusters', methods=['GET'])
def list_all_clusters_api():
    return list_all_clusters() 

def list_all_clusters():
    try:
        all_clusters_docs = list(clusters_collection.find({}))
        for doc in all_clusters_docs:
            doc["id"] = doc["_id"]
            del doc["_id"]
            
            collector_status_doc = doc.get("collector_status", {})
            process_pid = collector_status_doc.get("pid")
            current_db_status = collector_status_doc.get("status")
            cluster_id_for_update = doc["id"] 

            if cluster_id_for_update in active_collector_processes:
                if active_collector_processes[cluster_id_for_update].poll() is None: 
                    if current_db_status not in ["running", "starting", "resetting_oplog_store", "retrying_connection", "error_resume_token_lost", "error_max_retries_reached"]:
                         doc["collector_status"]["status"] = "app_managed_running" 
                else: 
                    if current_db_status not in ["stopped", "error", "error_resume_token_lost", "error_max_retries_reached"]: 
                        doc["collector_status"]["status"] = "error_stopped_unexpectedly" 
                        doc["collector_status"]["pid"] = None 
                        update_cluster_status_in_db(cluster_id_for_update, {"status": "error_stopped_unexpectedly", "pid": None})
            elif process_pid: 
                if is_process_running(process_pid):
                     if current_db_status not in ["error_resume_token_lost", "error_max_retries_reached"]:
                        doc["collector_status"]["status"] = "running_orphaned" 
                else: 
                     if current_db_status not in ["stopped", "error_resume_token_lost", "error_max_retries_reached"]: 
                        doc["collector_status"]["status"] = "stopped_orphaned" 
                        doc["collector_status"]["pid"] = None
                        update_cluster_status_in_db(cluster_id_for_update, {"status": "stopped_orphaned", "pid": None})
        
        return jsonify(all_clusters_docs)
    except PyMongoError as e:
        app.logger.error(f"Database error listing clusters: {e}")
        return jsonify({"message": f"Database error: {e}"}), 500

@app.route('/api/clusters/<cluster_id>/start_collector', methods=['POST'])
def start_collector_for_cluster_api(cluster_id):
    return start_collector_for_cluster(cluster_id)

def start_collector_for_cluster(cluster_id):
    cluster_doc = clusters_collection.find_one({"_id": cluster_id})
    if not cluster_doc:
        return jsonify({"message": "Cluster not found"}), 404
    
    current_status = cluster_doc.get("collector_status", {}).get("status", "unknown")
    process_pid = cluster_doc.get("collector_status", {}).get("pid")

    if cluster_id in active_collector_processes and active_collector_processes[cluster_id].poll() is None:
         return jsonify({"message": f"Collector for {cluster_doc['name']} is already managed and running by this app instance."}), 409
    if current_status == "error_resume_token_lost":
        return jsonify({"message": f"Collector for {cluster_doc['name']} is in 'error_resume_token_lost' state. Please perform 'Oplog Reset' first."}), 409
    if process_pid and is_process_running(process_pid) and current_status not in ["stopped", "error", "stopped_orphaned", "error_max_retries_reached"]: 
         return jsonify({"message": f"Collector for {cluster_doc['name']} (PID: {process_pid}) seems to be running (possibly orphaned). Stop it first or check manually."}), 409

    start_oplog_collector_process(cluster_doc)
    return jsonify({"message": f"Attempting to start oplog collector for cluster '{cluster_doc['name']}'."})

@app.route('/api/clusters/<cluster_id>/stop_collector', methods=['POST'])
def stop_collector_for_cluster_api(cluster_id):
    return stop_collector_for_cluster(cluster_id)

def stop_collector_for_cluster(cluster_id):
    cluster_doc = clusters_collection.find_one({"_id": cluster_id})
    if not cluster_doc:
        return jsonify({"message": "Cluster not found"}), 404

    if stop_oplog_collector_process(cluster_id):
        return jsonify({"message": f"Oplog collector for cluster '{cluster_doc['name']}' stopping process initiated."})
    else:
        return jsonify({"message": f"Oplog collector for '{cluster_doc['name']}' was not actively managed or already stopped. Status updated."})

@app.route('/api/clusters/<cluster_id>/reset_oplog_store', methods=['POST'])
def reset_oplog_store_for_cluster_api(cluster_id):
    return reset_oplog_store_for_cluster(cluster_id)

def reset_oplog_store_for_cluster(cluster_id):
    cluster_doc = clusters_collection.find_one({"_id": cluster_id})
    if not cluster_doc:
        return jsonify({"message": "Cluster not found"}), 404

    cluster_name = cluster_doc["name"]
    oplog_data_store_uri = cluster_doc["oplog_data_store_uri"]

    app.logger.info(f"Initiating Oplog Store reset for cluster '{cluster_name}' (ID: {cluster_id}).")
    stop_oplog_collector_process(cluster_id) 
    update_cluster_status_in_db(cluster_id, {
        "status": "resetting_oplog_store", 
        "error_message": None, 
        "last_oplog_time_utc": None,
        "first_oplog_time_utc": None 
    })

    oplog_store_client = None
    try:
        app.logger.info(f"Connecting to Oplog Store URI '{oplog_data_store_uri}' for cluster '{cluster_name}' to clear data.")
        oplog_store_client = MongoClient(oplog_data_store_uri, serverSelectionTimeoutMS=5000)
        oplog_store_db = oplog_store_client.get_database() 
        if oplog_store_db.name == 'test' and 'oplog_backup_db' not in oplog_data_store_uri : 
             oplog_store_db = oplog_store_client.oplog_backup_db

        oplog_data_coll_name = f"oplogs_{cluster_name}"
        resume_token_coll_name = f"resume_tokens_for_{cluster_name}" 
        resume_token_doc_id = f"resume_token_{cluster_name}" 

        app.logger.info(f"Dropping oplog data collection: {oplog_store_db.name}.{oplog_data_coll_name}")
        oplog_store_db[oplog_data_coll_name].drop()
        
        app.logger.info(f"Deleting resume token document: {resume_token_doc_id} from {oplog_store_db.name}.{resume_token_coll_name}")
        oplog_store_db[resume_token_coll_name].delete_one({"_id": resume_token_doc_id}) 
        
        app.logger.info(f"Oplog data and resume token cleared for cluster '{cluster_name}' from its Oplog Store.")

    except PyMongoError as e:
        app.logger.error(f"Error clearing Oplog Store for cluster '{cluster_name}': {e}")
        update_cluster_status_in_db(cluster_id, {"status": "error", "error_message": f"Failed to clear Oplog Store: {e}"})
        if oplog_store_client: oplog_store_client.close()
        return jsonify({"message": f"Error clearing Oplog Store: {e}"}), 500
    finally:
        if oplog_store_client: oplog_store_client.close()

    app.logger.info(f"Restarting oplog collector for cluster '{cluster_name}' after reset.")
    start_oplog_collector_process(cluster_doc) 

    return jsonify({"message": f"Oplog Store for cluster '{cluster_name}' has been reset and collector is restarting."}), 200

@app.route('/api/clusters/<cluster_id>', methods=['DELETE'])
def delete_cluster_api(cluster_id):
    return delete_cluster(cluster_id)

def delete_cluster(cluster_id):
    app.logger.info(f"Attempting to delete cluster with ID: {cluster_id}")
    cluster_doc = clusters_collection.find_one({"_id": cluster_id})
    if not cluster_doc:
        app.logger.warning(f"Cluster with ID {cluster_id} not found for deletion.")
        return jsonify({"message": "Cluster not found"}), 404

    cluster_name = cluster_doc.get("name", "Unknown")
    oplog_data_store_uri = cluster_doc.get("oplog_data_store_uri")
    app.logger.info(f"Found cluster '{cluster_name}' for deletion. Oplog Store URI: {oplog_data_store_uri}")

    stop_oplog_collector_process(cluster_id) 

    oplog_store_cleaned_successfully = False
    if oplog_data_store_uri:
        oplog_store_client = None
        try:
            app.logger.info(f"Connecting to Oplog Store URI '{oplog_data_store_uri}' for cluster '{cluster_name}' to WIPE data.")
            oplog_store_client = MongoClient(oplog_data_store_uri, serverSelectionTimeoutMS=5000)
            oplog_store_db = oplog_store_client.get_database()
            if oplog_store_db.name == 'test' and 'oplog_backup_db' not in oplog_data_store_uri:
                oplog_store_db = oplog_store_client.oplog_backup_db

            oplog_data_coll_name = f"oplogs_{cluster_name}"
            resume_token_coll_name = f"resume_tokens_for_{cluster_name}"
            
            app.logger.info(f"Dropping oplog data collection: {oplog_store_db.name}.{oplog_data_coll_name}")
            oplog_store_db[oplog_data_coll_name].drop()
            
            app.logger.info(f"Dropping resume token collection: {oplog_store_db.name}.{resume_token_coll_name}")
            oplog_store_db[resume_token_coll_name].drop() 
            
            app.logger.info(f"Oplog data and resume token collections WIPED for cluster '{cluster_name}' from its Oplog Store.")
            oplog_store_cleaned_successfully = True
        except PyMongoError as e:
            app.logger.error(f"Error WIPING Oplog Store for cluster '{cluster_name}': {e}. Metadata will still be deleted.")
        finally:
            if oplog_store_client: oplog_store_client.close()
    else:
        app.logger.warning(f"No oplog_data_store_uri found for cluster '{cluster_name}'. Skipping Oplog Store data wipe.")
        oplog_store_cleaned_successfully = True 


    try:
        delete_result = clusters_collection.delete_one({"_id": cluster_id})
        if delete_result.deleted_count > 0:
            app.logger.info(f"Successfully deleted cluster '{cluster_name}' (ID: {cluster_id}) from metadata store.")
            message = f"Cluster '{cluster_name}' metadata deleted successfully."
            if oplog_data_store_uri:
                message += " Oplog data store content for this cluster has also been wiped." if oplog_store_cleaned_successfully else " Failed to wipe Oplog data store content."
            return jsonify({"message": message}), 200
        else:
            app.logger.warning(f"Cluster '{cluster_name}' (ID: {cluster_id}) was found but not deleted from metadata store (delete_count=0).")
            return jsonify({"message": "Cluster found but could not be deleted from store."}), 500
    except PyMongoError as e:
        app.logger.error(f"Database error deleting cluster '{cluster_name}' (ID: {cluster_id}): {e}")
        return jsonify({"message": f"Database error during cluster deletion: {e}"}), 500
    except Exception as e:
        app.logger.error(f"Unexpected error deleting cluster '{cluster_name}' (ID: {cluster_id}): {e}")
        return jsonify({"message": f"Server error during cluster deletion: {e}"}), 500

# --- Graceful shutdown for app.py ---
def on_app_shutdown():
    app.logger.info("Application is shutting down. Stopping active collector processes...")
    for cluster_id in list(active_collector_processes.keys()):
        stop_oplog_collector_process(cluster_id) 
    app.logger.info("Cleanup finished.")

import atexit
atexit.register(on_app_shutdown)

# --- Main Execution ---
if __name__ == '__main__':
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug: 
        startup_thread = threading.Thread(target=restart_collectors_on_startup, daemon=True)
        startup_thread.start()

    app.run(debug=True, host='0.0.0.0', port=4999, use_reloader=False) 

