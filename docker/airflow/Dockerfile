FROM apache/airflow:2.9.2-python3.11

USER airflow

# Install our project's Python dependencies on top of the Airflow base image.
# Pinned versions keep the build reproducible across team members' machines.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
