import os

class CoreEnv:
  bucket_instagram: str
  apify_token: str
  actor_instagram: str
  account_service_instagram_gcp: str
  kafka_cluster_host: str
  gcp_project_id: str
  ollama_base_url: str
  ollama_model: str

  def __init__(self):
    self.bucket_instagram = os.getenv("BUCKET_INSTAGRAM")
    self.apify_token = os.getenv("APIFY_TOKEN")
    self.actor_instagram = os.getenv("ACTOR_INSTAGRAM")
    self.account_service_instagram_gcp = os.getenv("ACCOUNT_SERVICE_INSTAGRAM_GCP")
    self.kafka_cluster_host = os.getenv("KAFKA_CLUSTER_HOST")
    self.gcp_project_id = os.getenv("GCP_PROJECT_ID")
    self.ollama_base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    self.ollama_model = os.getenv("OLLAMA_MODEL", "llama3.1")