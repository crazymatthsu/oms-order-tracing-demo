# Deephaven server (Community Core 42.5) + the AMPS python client: the image the AMPS demo
# overlay runs (docker/docker-compose.amps.yml, docs/design/11). No Deephaven image ships an AMPS
# client and `podman exec ... pip install` does not survive `down`, so the demo builds a derived
# image once. On Enterprise the same package goes into the worker's Python environment
# (doc 11 section 6); this Dockerfile is the Community analogue.
#
#   podman compose -f docker/docker-compose.yml -f docker/docker-compose.amps.yml build
#   podman build -t localhost/oms-trace-deephaven-amps:42.5 -f docker/deephaven-amps.Dockerfile docker
#
# Every external source comes from repos.env (the base image and the pip index); compose passes
# them as build args, and the defaults are the public ones so a plain `podman build` works too.
ARG BASE_IMAGE=ghcr.io/deephaven/server:42.5
FROM ${BASE_IMAGE}
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=

# amps-python-client: a binary wheel on PyPI (manylinux x86_64/aarch64, macOS, Windows). The image
# runs as root with its venv on PATH, so a plain `pip install` lands in the server's interpreter.
# An empty trusted host is unset rather than handed to pip as an empty value.
RUN if [ -z "$PIP_TRUSTED_HOST" ]; then unset PIP_TRUSTED_HOST; fi; \
    pip install --no-cache-dir --disable-pip-version-check "amps-python-client>=5.3,<6"
