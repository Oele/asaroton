-- A local topological order (tracker #99): when an event is stored it gets the highest order of
-- its known prev events plus one, or its wire depth when none of them is known (backfill). The
-- wire depth is whatever the sending server wrote, and a server that resets it (Synapse #20524)
-- or inflates it would otherwise decide which events a first view of a room shows. Events stored
-- before this have NULL and count with their depth; adding the column rewrites nothing.
ALTER TABLE events ADD COLUMN topo BIGINT;
