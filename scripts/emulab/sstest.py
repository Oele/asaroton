"""Sliding sync probe: initial response, then a state change by the other user and an incremental
response, printing what required_state and timeline carry. Reads lab.json from setup-room.py.
"""
import json, urllib.request, urllib.parse, sys, time
lab=json.load(open("lab.json")); HS=lab.get("hs", "http://localhost:8008"); rid=lab["room_id"]
tok_ex=lab["ex"][2]; tok_web=lab["web"][2]
def ss(pos=None, conn="sstest"):
    q={"timeout":"1000"}
    if pos: q["pos"]=pos
    body={"conn_id":conn,"lists":{},"room_subscriptions":{rid:{"required_state":[["*","*"]],"timeline_limit":10}}}
    r=urllib.request.Request(HS+"/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?"+urllib.parse.urlencode(q), data=json.dumps(body).encode(), method="POST", headers={"Content-Type":"application/json","Authorization":"Bearer "+tok_ex})
    return json.load(urllib.request.urlopen(r))
def send_state(t, key, content):
    r=urllib.request.Request(f"{HS}/_matrix/client/v3/rooms/{urllib.parse.quote(rid,safe='')}/state/{t}/{key}", data=json.dumps(content).encode(), method="PUT", headers={"Content-Type":"application/json","Authorization":"Bearer "+tok_web})
    return json.load(urllib.request.urlopen(r))["event_id"]
conn="sstest"+str(int(time.time()))
a=ss(conn=conn); room=a["rooms"][rid]
print("initial: required_state types:", sorted({(e["type"],e.get("state_key","")) for e in room["required_state"]}))
print("initial: timeline:", [(e["type"]) for e in room["timeline"]])
eid=send_state("m.room.topic","",{"topic":"t"+str(int(time.time()))})
b=ss(a["pos"], conn)
room=b.get("rooms",{}).get(rid,{})
print("incremental: required_state:", [(e["type"],e["event_id"]==eid) for e in room.get("required_state",[])])
print("incremental: timeline:", [(e["type"],e["event_id"]==eid) for e in room.get("timeline",[])])
