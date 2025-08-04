# download_model.py
from transformers import AutoModel, AutoTokenizer
import os

os.environ["HF_HOME"] = "/hf_cache"
model_name = "BAAI/bge-m3"

AutoTokenizer.from_pretrained(model_name)
AutoModel.from_pretrained(model_name)
