# A5 Specs

## Frontend-Storage RPC APIs

### Frontend to Storage

* Check if storage node is online

<code>
function isAlive(type request, bool &reply);
</code>

* Put a key-value pair to disk

<code>
function Put(type request, bool &reply);
</code>

* Retrieve a key-value pair to client
<code> 
function Get(type request, type &reply);
</code>

### Storage to Frontend

* resReturn result to frontend
<code>
function Result(type request, type &reply);
</code>
