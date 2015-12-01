# RethinkDB binary
echo "[Installing RethinkDB server binary] ..."
source /etc/lsb-release && echo "deb http://download.rethinkdb.com/apt $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list
wget -qO- http://download.rethinkdb.com/apt/pubkey.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install rethinkdb

# RethinkDB client
echo "[Installing RethinkDB (Python) client library] ..."
sudo pip install rethinkdb
