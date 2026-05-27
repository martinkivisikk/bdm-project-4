FROM apache/airflow:2.10.4-python3.11

# Install project dependencies declared in pyproject.toml.
# We copy only the manifest and create a stub package so pip can resolve deps
# without the full source tree (src/ is bind-mounted at runtime via PYTHONPATH).
#
# To add ML deps (torch, CLIP, SBERT) when embed_image, embed_text, extract, eval tasks are ready,
# change the pip install line to:  pip install --no-cache-dir "/tmp/rico[ml]"
COPY --chown=airflow:root pyproject.toml /tmp/pyproject.toml
RUN mkdir -p /tmp/rico/src/rico && \
    cp /tmp/pyproject.toml /tmp/rico/pyproject.toml && \
    touch /tmp/rico/src/rico/__init__.py && \
    pip install --no-cache-dir /tmp/rico[ml] && \
    rm -rf /tmp/rico /tmp/pyproject.toml
