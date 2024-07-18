# metaflow_config.py

from metaflow import configure

configure(
    datastore='local',  # Configure Metaflow datastore (e.g., local, s3)
    namespace='airbnb_etl',  # Namespace for Metaflow project
    backend='local',       # Backend for Metaflow project (e.g., local, aws)
    plugins={'kubernetes': False}
)
