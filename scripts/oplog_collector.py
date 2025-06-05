from pymongo import MongoClient, UpdateOne, ASCENDING 
from pymongo.errors import PyMongoError, BulkWriteError, OperationFailure
from bson.timestamp import Timestamp 
import time
import datetime
import os
import signal 
import logging 
import json 
from logging.handlers import RotatingFileHandler 

# --- Logger Setup ---
LOG_LEVEL_ENV = os.environ.get("OPLOG_LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, LOG_LEVEL_ENV, logging.INFO)

logger = logging.getLogger(os.path.basename(__file__).replace(".py", "")) 
logger.setLevel(numeric_level)
# Basic stream handler (console output)
stream_handler = logging.StreamHandler() 
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
if not logger.handlers: 
    logger.addHandler(stream_handler)

logger.debug("Starting oplog_collector.py script...")
logger.debug(f"Initial log level set to: {LOG_LEVEL_ENV} ({numeric_level})")

# --- Environment Variable Configuration ---
SOURCE_MONGO_URI = os.environ.get("SOURCE_MONGO_URI")
OPLOG_STORE_URI = os.environ.get("OPLOG_STORE_URI")
CLUSTER_NAME = os.environ.get("CLUSTER_NAME") 
CHANGE_STREAM_TARGET_DB = os.environ.get("CHANGE_STREAM_TARGET_DB")
OPLOG_MANAGER_CENTRAL_DB_URI = os.environ.get("OPLOG_MANAGER_CENTRAL_DB_URI")
OPLOG_MANAGER_CLUSTER_ID = os.environ.get("OPLOG_MANAGER_CLUSTER_ID") 
OPLOG_MANAGER_COLLECTION_NAME = os.environ.get("OPLOG_MANAGER_COLLECTION_NAME")
COLLECTOR_LOG_FILE_PATH = os.environ.get("COLLECTOR_LOG_FILE") # Passed by app.py

# Log rotation settings from environment variables or defaults
LOG_MAX_BYTES_DEFAULT = 100 * 1024 * 1024  # 100MB
LOG_BACKUP_COUNT_DEFAULT = 10
COLLECTOR_LOG_MAX_BYTES = int(os.environ.get("COLLECTOR_LOG_MAX_BYTES", LOG_MAX_BYTES_DEFAULT))
COLLECTOR_LOG_BACKUP_COUNT = int(os.environ.get("COLLECTOR_LOG_BACKUP_COUNT", LOG_BACKUP_COUNT_DEFAULT))

# --- File Logger Setup (if path is provided) ---
if COLLECTOR_LOG_FILE_PATH:
    log_dir = os.path.dirname(COLLECTOR_LOG_FILE_PATH)
    if log_dir and not os.path.exists(log_dir): 
        try:
            os.makedirs(log_dir)
            logger.info(f"Log directory created: {log_dir}")
        except OSError as e:
            logger.error(f"Could not create log directory {log_dir}: {e}")
            COLLECTOR_LOG_FILE_PATH = None # Disable file logging if dir creation fails
    
    if COLLECTOR_LOG_FILE_PATH:
        try:
            file_handler_collector = RotatingFileHandler(
                COLLECTOR_LOG_FILE_PATH, # Base name like oplog_collector_rs0.log
                maxBytes=COLLECTOR_LOG_MAX_BYTES, 
                backupCount=COLLECTOR_LOG_BACKUP_COUNT, 
                encoding='utf-8'
            )
            file_handler_collector.setFormatter(formatter) 
            logger.addHandler(file_handler_collector)
            logger.info(f"Collector file logging initialized at: {COLLECTOR_LOG_FILE_PATH}")
        except Exception as e:
            logger.error(f"Failed to initialize file logger at {COLLECTOR_LOG_FILE_PATH}: {e}")


logger.debug(f"SOURCE_MONGO_URI: {SOURCE_MONGO_URI}")
logger.debug(f"OPLOG_STORE_URI: {OPLOG_STORE_URI}")
logger.debug(f"CLUSTER_NAME: {CLUSTER_NAME}")
logger.debug(f"CHANGE_STREAM_TARGET_DB: {CHANGE_STREAM_TARGET_DB}")
logger.debug(f"OPLOG_MANAGER_CENTRAL_DB_URI: {OPLOG_MANAGER_CENTRAL_DB_URI}")
logger.debug(f"OPLOG_MANAGER_CLUSTER_ID: {OPLOG_MANAGER_CLUSTER_ID}")
logger.debug(f"OPLOG_MANAGER_COLLECTION_NAME: {OPLOG_MANAGER_COLLECTION_NAME}")


if not all([SOURCE_MONGO_URI, OPLOG_STORE_URI, CLUSTER_NAME]):
    logger.critical(f"FATAL: SOURCE_MONGO_URI, OPLOG_STORE_URI, and CLUSTER_NAME environment variables must be set.")
    if OPLOG_MANAGER_CENTRAL_DB_URI and OPLOG_MANAGER_CLUSTER_ID and OPLOG_MANAGER_COLLECTION_NAME:
        try:
            status_client_temp = MongoClient(OPLOG_MANAGER_CENTRAL_DB_URI, serverSelectionTimeoutMS=3000)
            status_db_temp = status_client_temp.get_database()
            status_coll_temp = status_db_temp[OPLOG_MANAGER_COLLECTION_NAME]
            status_coll_temp.update_one(
                {"_id": OPLOG_MANAGER_CLUSTER_ID},
                {"$set": {
                    "collector_status.status": "error",
                    "collector_status.error_message": "Missing critical environment variables.",
                    "collector_status.last_collector_reported_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
                }}
            )
            status_client_temp.close()
        except Exception as e_status:
            logger.error(f"FATAL_STATUS_UPDATE: Failed to report critical env var error to central DB: {e_status}")
    exit(1)

OPLOG_BATCH_SIZE = int(os.environ.get("OPLOG_BATCH_SIZE", 100))
OPLOG_BATCH_TIMEOUT_SECONDS = float(os.environ.get("OPLOG_BATCH_TIMEOUT_SECONDS", 1.0))
STATUS_UPDATE_INTERVAL_SECONDS = int(os.environ.get("STATUS_UPDATE_INTERVAL_SECONDS", 30)) 

RETRY_DELAY_SECONDS = 10
MAX_CONNECTION_RETRIES = 10000

shutdown_flag = False
last_processed_oplog_time_utc = None 

def signal_handler(signum, frame):
    global shutdown_flag
    logger.info(f"Received termination signal ({signum}). Shutting down gracefully...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

status_update_client = None
status_update_db = None
status_update_collection = None

if OPLOG_MANAGER_CENTRAL_DB_URI and OPLOG_MANAGER_CLUSTER_ID and OPLOG_MANAGER_COLLECTION_NAME:
    try:
        logger.debug(f"Attempting to connect to Central Status Reporting DB: {OPLOG_MANAGER_CENTRAL_DB_URI}")
        status_update_client = MongoClient(OPLOG_MANAGER_CENTRAL_DB_URI, serverSelectionTimeoutMS=5000)
        status_update_client.admin.command('ping')
        status_update_db = status_update_client.get_database() 
        status_update_collection = status_update_db[OPLOG_MANAGER_COLLECTION_NAME]
        logger.info(f"Successfully connected to Central Status Reporting DB: {OPLOG_MANAGER_CENTRAL_DB_URI}")
    except PyMongoError as e:
        logger.warning(f"Failed to connect to Central Status Reporting DB: {e}. Proceeding without status reporting.")
        status_update_client = None 


def update_status_to_central_db(status, error_message=None, pid=None):
    global last_processed_oplog_time_utc
    logger.debug(f"Attempting to update central DB status. Current status_update_collection: {'Set' if status_update_collection is not None else 'None'}, OPLOG_MANAGER_CLUSTER_ID: {OPLOG_MANAGER_CLUSTER_ID}")
    if status_update_collection is None or not OPLOG_MANAGER_CLUSTER_ID:
        logger.debug("Central status DB information is not available. Skipping status update.")
        return

    payload = {
        "collector_status.status": status,
        "collector_status.last_collector_reported_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if last_processed_oplog_time_utc: 
        payload["collector_status.last_oplog_time_utc"] = last_processed_oplog_time_utc
    if error_message:
        payload["collector_status.error_message"] = str(error_message)
    else: 
        payload["collector_status.error_message"] = None
    if pid is not None: 
        payload["collector_status.pid"] = pid
    
    current_first_oplog_ts_iso = None
    if oplog_data_collection is not None: 
        try:
            first_entry = oplog_data_collection.find_one(
                {"cluster_name": CLUSTER_NAME}, 
                sort=[("ts", ASCENDING)],
                projection={"ts": 1}
            )
            if first_entry and first_entry.get("ts"):
                ts_val = first_entry.get("ts")
                if isinstance(ts_val, datetime.datetime):
                    current_first_oplog_ts_iso = ts_val.replace(tzinfo=datetime.timezone.utc).isoformat()
                elif isinstance(ts_val, Timestamp): 
                    current_first_oplog_ts_iso = ts_val.as_datetime().replace(tzinfo=datetime.timezone.utc).isoformat()
                logger.debug(f"Dynamically fetched first_oplog_time_utc for status update: {current_first_oplog_ts_iso}")
        except Exception as e_fetch_first:
            logger.warning(f"Could not fetch first_oplog_time_utc dynamically during status update: {e_fetch_first}")
    
    if current_first_oplog_ts_iso:
        payload["collector_status.first_oplog_time_utc"] = current_first_oplog_ts_iso
    elif status in ["starting", "resetting_oplog_store", "running"] and not last_processed_oplog_time_utc : 
        payload["collector_status.first_oplog_time_utc"] = None


    logger.debug(f"Payload for status update (Cluster ID: {OPLOG_MANAGER_CLUSTER_ID}): {json.dumps(payload, default=str)}")

    try:
        if status_update_client:
            try:
                logger.debug("Pinging central DB before status update...")
                status_update_client.admin.command('ping') 
                logger.debug("Central DB ping successful.")
            except PyMongoError as ping_e:
                logger.warning(f"Connection to central DB seems lost (ping failed: {ping_e}). Skipping status update.")
                return 
        
        result = status_update_collection.update_one(
            {"_id": OPLOG_MANAGER_CLUSTER_ID},
            {"$set": payload}
        )
        logger.debug(f"Central DB status update result for {OPLOG_MANAGER_CLUSTER_ID}: matched_count={result.matched_count}, modified_count={result.modified_count}")
    except PyMongoError as e:
        logger.error(f"Error updating status to central DB: {e}")
    except Exception as e: 
        logger.error(f"Unexpected error during status update to central DB: {e}")


# --- Oplog Store Client (for Oplog data and Resume Tokens) ---
oplog_store_client = None 
oplog_db_for_data = None
oplog_data_collection = None
resume_token_collection = None

try:
    logger.debug(f"Attempting to connect to Oplog Data Store: {OPLOG_STORE_URI}")
    oplog_store_client = MongoClient(OPLOG_STORE_URI, serverSelectionTimeoutMS=5000)
    oplog_store_client.admin.command('ping')
    oplog_db_for_data = oplog_store_client.get_database()
    if oplog_db_for_data.name == 'test' and 'oplog_backup_db' not in OPLOG_STORE_URI: 
        oplog_db_for_data = oplog_store_client.oplog_backup_db 

    oplog_data_collection_name = f"oplogs_{CLUSTER_NAME}"
    oplog_data_collection = oplog_db_for_data[oplog_data_collection_name]
    resume_token_collection_name = f"resume_tokens_for_{CLUSTER_NAME}" 
    resume_token_collection = oplog_db_for_data[resume_token_collection_name]
    logger.info(f"Successfully connected to Oplog Data Store: {OPLOG_STORE_URI}, DB: {oplog_db_for_data.name}")
    logger.info(f"Oplog storage collection: {oplog_data_collection_name}")
    logger.info(f"Resume token storage collection: {resume_token_collection_name}")
except PyMongoError as e:
    logger.critical(f"Could not connect to Oplog Data Store: {e}")
    update_status_to_central_db("error", f"Failed to connect to Oplog Data Store: {e}")
    exit(1)


def get_last_resume_token_from_oplog_store():
    if resume_token_collection is None: 
        logger.warning("Resume token collection not initialized, cannot get token.")
        return None 
    try:
        logger.debug(f"Fetching last resume token for _id: resume_token_{CLUSTER_NAME}")
        token_doc = resume_token_collection.find_one({"_id": f"resume_token_{CLUSTER_NAME}"})
        if token_doc:
            logger.debug(f"Loaded resume token from Oplog Store: {str(token_doc['token']['_data'])[:20]}...")
        else:
            logger.debug("No resume token found in Oplog Store.")
        return token_doc['token'] if token_doc else None
    except PyMongoError as e:
        logger.error(f"Error fetching resume token from Oplog Store: {e}")
        return None

def save_resume_token_to_oplog_store(token):
    if resume_token_collection is None: 
        logger.warning("Resume token collection not initialized, cannot save token.")
        return 
    try:
        if token is None: 
            logger.info(f"Deleting resume token for _id: resume_token_{CLUSTER_NAME}")
            result = resume_token_collection.delete_one({"_id": f"resume_token_{CLUSTER_NAME}"})
            logger.debug(f"Resume token delete result: deleted_count={result.deleted_count}")
        else:
            logger.debug(f"Saving resume token for _id: resume_token_{CLUSTER_NAME}, Token: {str(token['_data'])[:20]}...")
            result = resume_token_collection.update_one(
                {"_id": f"resume_token_{CLUSTER_NAME}"},
                {"$set": {"token": token, "updated_at": datetime.datetime.utcnow()}},
                upsert=True
            )
            logger.debug(f"Resume token save result: matched_count={result.matched_count}, modified_count={result.modified_count}, upserted_id={result.upserted_id}")
    except PyMongoError as e:
        logger.error(f"Error saving/deleting resume token to Oplog Store: {e}")


def process_oplog_batch(oplog_batch):
    global last_processed_oplog_time_utc
    if not oplog_batch:
        logger.debug("process_oplog_batch called with empty batch.")
        return True, None
    if oplog_data_collection is None: 
        logger.error("Oplog data collection not initialized. Cannot process batch.")
        return False, None
    
    logger.debug(f"Processing batch of {len(oplog_batch)} oplog entries for Oplog Store.")
    try:
        insert_result = oplog_data_collection.insert_many(oplog_batch, ordered=False)
        logger.debug(f"insert_many result: acknowledged={insert_result.acknowledged}, inserted_ids_count={len(insert_result.inserted_ids)}")
        last_entry_in_batch = oplog_batch[-1]
        last_token_in_batch = last_entry_in_batch['_resume_token']
        
        if 'ts' in last_entry_in_batch and isinstance(last_entry_in_batch['ts'], datetime.datetime):
             last_processed_oplog_time_utc = last_entry_in_batch['ts'].replace(tzinfo=datetime.timezone.utc).isoformat()
        elif 'ts' in last_entry_in_batch : 
             last_processed_oplog_time_utc = last_entry_in_batch['ts'].as_datetime().replace(tzinfo=datetime.timezone.utc).isoformat()
        logger.debug(f"Last processed oplog time UTC set to: {last_processed_oplog_time_utc}")

        logger.info(f"Saved {len(oplog_batch)} oplog entries for cluster '{CLUSTER_NAME}' to Oplog Store. Last token: {str(last_token_in_batch['_data'])[:20]}...")
        return True, last_token_in_batch
    except BulkWriteError as bwe:
        logger.error(f"BulkWriteError saving batch for '{CLUSTER_NAME}' to Oplog Store: {bwe.details}")
        update_status_to_central_db("error", f"Oplog batch save failed (BulkWriteError): {bwe.details}")
        return False, None
    except PyMongoError as e:
        logger.error(f"PyMongoError saving batch for '{CLUSTER_NAME}' to Oplog Store: {e}")
        update_status_to_central_db("error", f"Oplog batch save failed (PyMongoError): {e}")
        return False, None

def watch_changes():
    global shutdown_flag, last_processed_oplog_time_utc
    process_pid = os.getpid()
    logger.info(f"Oplog Collector Service starting (PID: {process_pid}, Cluster: {CLUSTER_NAME})")
    update_status_to_central_db("starting", pid=process_pid) 

    logger.info(f"Source MongoDB: {SOURCE_MONGO_URI}")
    logger.info(f"Oplog Data Store (for data & resume tokens): {OPLOG_STORE_URI}, DB: {oplog_db_for_data.name if oplog_db_for_data is not None else 'N/A'}, Collection: {oplog_data_collection.name if oplog_data_collection is not None else 'N/A'}")
    if CHANGE_STREAM_TARGET_DB:
        logger.info(f"Change stream target DB: {CHANGE_STREAM_TARGET_DB}")
    else:
        logger.info("Change stream target: Entire cluster (or DB specified in Source URI)")

    oplog_batch = []
    last_batch_save_time = time.time()
    last_status_update_time = time.time()
    source_client = None
    connection_retries = 0
    
    while not shutdown_flag:
        try:
            if source_client is None:
                logger.info(f"Attempting to connect to Source MongoDB... (Attempt {connection_retries + 1}/{MAX_CONNECTION_RETRIES})")
                if connection_retries > 0: 
                     update_status_to_central_db("retrying_connection", error_message=f"Source DB Connection attempt {connection_retries + 1}", pid=process_pid)

                source_client = MongoClient(SOURCE_MONGO_URI, serverSelectionTimeoutMS=5000)
                source_client.admin.command('ping')
                logger.info("Successfully connected to Source MongoDB.")
                connection_retries = 0 
                update_status_to_central_db("running", pid=process_pid) 

            resume_token = get_last_resume_token_from_oplog_store()
            pipeline = [{'$match': {'operationType': {'$in': [
                'insert', 'update', 'delete', 'replace', 
                'drop', 'rename', 'dropDatabase', 'invalidate',
                'createIndexes', 'dropIndexes', 'create', 'modify' 
            ]}}}]
            change_stream_options = {'full_document': 'updateLookup', 'show_expanded_events': True}


            if resume_token:
                change_stream_options['resume_after'] = resume_token
                logger.info(f"Resuming change stream for '{CLUSTER_NAME}' (Token: {str(resume_token['_data'])[:20]}...)")
            else:
                logger.info(f"Starting new change stream for '{CLUSTER_NAME}' (No resume token).")

            if CHANGE_STREAM_TARGET_DB:
                db_to_watch = source_client[CHANGE_STREAM_TARGET_DB]
                stream_context = db_to_watch.watch(pipeline, **change_stream_options)
                logger.info(f"Watching changes on database: {CHANGE_STREAM_TARGET_DB}...")
            else:
                stream_context = source_client.watch(pipeline, **change_stream_options)
                logger.info("Watching changes on the entire cluster (or Source URI DB)...")

            with stream_context as stream:
                logger.debug(f"Change stream opened for {CLUSTER_NAME}. Stream alive: {stream.alive}")
                while stream.alive and not shutdown_flag:
                    change = stream.try_next()
                    if change is None:
                        if oplog_batch and (time.time() - last_batch_save_time) >= OPLOG_BATCH_TIMEOUT_SECONDS:
                            logger.debug(f"Batch timeout reached. Processing {len(oplog_batch)} items.")
                            pass 
                        else:
                            time.sleep(0.1) 
                    else:
                        op_type = change['operationType']
                        logger.debug(f"Received change event: Type={op_type}, NS={change.get('ns')}, ID={change['_id']}")
                        if logger.level == logging.DEBUG: 
                             logger.debug(f"Full change document: {json.dumps(change, default=str)}")
                        
                        to_field_value = None
                        if op_type == 'rename':
                            to_field_value = change.get('to') 

                        oplog_entry = {
                            "_resume_token": change['_id'], 
                            "cluster_name": CLUSTER_NAME, 
                            "ts": change['clusterTime'], 
                            "wall": datetime.datetime.utcnow(), 
                            "op_type": op_type,
                            "ns": change.get('ns', {}).get('db', '') + '.' + change.get('ns', {}).get('coll', ''),
                            "doc_key": change.get('documentKey'),
                            "full_document": change.get('fullDocument'),
                            "update_description": change.get('updateDescription'),
                            "to": to_field_value, 
                            "operation_description": change.get('operationDescription') if op_type in ['createIndexes', 'dropIndexes', 'create', 'modify'] else None
                        }
                        oplog_batch.append(oplog_entry)
                        logger.debug(f"Added oplog entry to batch. Batch size: {len(oplog_batch)}")

                    current_time = time.time()
                    if oplog_batch and \
                       (len(oplog_batch) >= OPLOG_BATCH_SIZE or \
                        (current_time - last_batch_save_time) >= OPLOG_BATCH_TIMEOUT_SECONDS):
                        
                        logger.debug(f"Processing batch due to size ({len(oplog_batch)}) or timeout.")
                        success, last_saved_token_from_batch = process_oplog_batch(list(oplog_batch))
                        if success and last_saved_token_from_batch:
                            save_resume_token_to_oplog_store(last_saved_token_from_batch) 
                            oplog_batch.clear()
                        elif not success:
                            logger.warning("Batch save failed. Token not updated. Check for potential data loss.")
                            oplog_batch.clear() 
                        last_batch_save_time = current_time
                    
                    if (current_time - last_status_update_time) >= STATUS_UPDATE_INTERVAL_SECONDS:
                        logger.debug("Periodic status update to central DB.")
                        update_status_to_central_db("running", pid=process_pid) 
                        last_status_update_time = current_time

                    if shutdown_flag: 
                        logger.info("Shutdown flag detected in main loop.")
                        break
            
            if not shutdown_flag and not stream.alive: 
                logger.warning("Change stream closed unexpectedly. Attempting to reconnect.")
                update_status_to_central_db("error", "Change stream closed unexpectedly. Retrying.", pid=process_pid)
                if source_client: source_client.close(); source_client = None
                time.sleep(RETRY_DELAY_SECONDS)

        except OperationFailure as e:
            logger.error(f"MongoDB OperationFailure: {e.details}")
            code_name_from_details = e.details.get('codeName', 'UnknownCodeName')
            error_msg_for_status = f"MongoDB OperationFailure: {code_name_from_details} (Code: {e.code})"
            
            is_non_resumable = False
            if e.has_error_label("ChangeStreamHistoryLost"): 
                logger.warning("Resume token lost (ChangeStreamHistoryLost). This stream is non-resumable with the current token.")
                error_msg_for_status += " (ChangeStreamHistoryLost - NonResumable)"
                is_non_resumable = True
            elif "NonResumableChangeStreamError" in e.error_labels if hasattr(e, 'error_labels') else False \
                 or (e.code == 280 and "the resume token was not found" in e.details.get("errmsg","").lower()): 
                logger.warning("NonResumableChangeStreamError (Resume token lost/invalid). This stream is non-resumable.")
                error_msg_for_status += " (NonResumableChangeStreamError - Token Lost)"
                is_non_resumable = True

            if is_non_resumable:
                logger.critical(f"CRITICAL: Non-resumable change stream error. Collector for '{CLUSTER_NAME}' will stop. Manual reset required via UI.")
                update_status_to_central_db("error_resume_token_lost", error_msg_for_status, pid=process_pid)
                shutdown_flag = True 
            else: 
                update_status_to_central_db("error", error_msg_for_status, pid=process_pid)
            
            if source_client: source_client.close(); source_client = None
            if not shutdown_flag: 
                time.sleep(RETRY_DELAY_SECONDS)


        except PyMongoError as e: 
            logger.error(f"Source MongoDB connection or change stream error: {e}")
            connection_retries += 1
            error_message_for_status = f"Source DB/Stream Error: {e}"
            current_status_for_retry = "retrying_connection"

            if connection_retries >= MAX_CONNECTION_RETRIES:
                logger.critical(f"Max connection retries ({MAX_CONNECTION_RETRIES}) reached. Exiting.")
                current_status_for_retry = "error_max_retries_reached"
                error_message_for_status = "Max connection retries reached. Collector stopped."
                shutdown_flag = True 
            
            update_status_to_central_db(current_status_for_retry, error_message_for_status, pid=process_pid)
            
            if source_client: source_client.close(); source_client = None
            
            if not shutdown_flag:
                logger.info(f"Retrying in {RETRY_DELAY_SECONDS} seconds... (Attempt {connection_retries}/{MAX_CONNECTION_RETRIES})")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                break 

        except Exception as e:
            logger.exception(f"Unexpected error: {e}") 
            update_status_to_central_db("error", f"Unexpected error: {e}", pid=process_pid)
            if source_client: source_client.close(); source_client = None
            time.sleep(RETRY_DELAY_SECONDS) 

    # --- Shutdown Procedure ---
    logger.info(f"Starting shutdown procedure (PID: {process_pid})...")
    
    final_db_status = "stopped" 
    if shutdown_flag and connection_retries >= MAX_CONNECTION_RETRIES: 
        final_db_status = "error_max_retries_reached"
    elif shutdown_flag and status_update_collection is not None: 
        current_doc = status_update_collection.find_one({"_id": OPLOG_MANAGER_CLUSTER_ID}, {"collector_status.status": 1})
        if current_doc and current_doc.get("collector_status",{}).get("status") == "error_resume_token_lost":
            final_db_status = "error_resume_token_lost" 
    
    if final_db_status != "error_resume_token_lost" and final_db_status != "error_max_retries_reached":
         update_status_to_central_db("stopping", pid=process_pid) 
    
    if oplog_batch:
        logger.info(f"Processing {len(oplog_batch)} remaining oplog entries before shutdown...")
        success, last_saved_token_final = process_oplog_batch(list(oplog_batch))
        if success and last_saved_token_final:
            save_resume_token_to_oplog_store(last_saved_token_final)
        else:
            logger.warning("Failed to save remaining batch during shutdown.")

    update_status_to_central_db(final_db_status, pid=None) 

    if source_client: 
        try: source_client.close() 
        except Exception as e_sc: logger.error(f"Error closing source_client: {e_sc}")
    if oplog_store_client: 
        try: oplog_store_client.close()
        except Exception as e_osc: logger.error(f"Error closing oplog_store_client: {e_osc}")
    if status_update_client: 
        try: status_update_client.close()
        except Exception as e_suc: logger.error(f"Error closing status_update_client: {e_suc}")
    
    logger.info(f"Oplog Collector Service (PID: {process_pid}, Cluster: {CLUSTER_NAME}) has been shut down with status: {final_db_status}.")

if __name__ == "__main__":
    watch_changes()

