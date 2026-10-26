# Persistent Query definitions

Exports of every `oms-*` PQ (`dhconfig pq export`) and the ACL definitions live here once the
PQs exist on a cluster, so the query definitions are infrastructure-as-code (docs/design/07).
The inventory, heaps, schedules and dependencies are in docs/design/07 section 2; the scripts
they run are `queries/oms_trace/*.py`.
