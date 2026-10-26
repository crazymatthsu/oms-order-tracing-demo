# Deephaven Application Mode descriptor. docker/docker-compose.yml mounts this folder at /app.d
# and sets -Ddeephaven.application.dir=/app.d, so the server runs main.py at start-up and every
# table it leaves in globals() shows up in the web IDE (Panels) and via pydeephaven.
type=script
scriptType=python
enabled=true
id=oms.order.tracing
name=OMS Order Tracing
file_0=main.py
