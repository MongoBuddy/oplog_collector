export CENTRAL_METADATA_DB_URI="mongodb://user:password@localhost:27017?replicaSet=oplog&authSource=admin"
nohup python app.py &
python -m http.server 8000
