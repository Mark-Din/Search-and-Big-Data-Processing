import time
import torch
import global_vars as global_vars

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware
from routers import seg_recommend_ai
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from contextlib import asynccontextmanager
from transformers import AutoModel, AutoTokenizer
from ollama import Client
import os
os.environ["HF_HOME"] = "/app/hf_cache"

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting application...")

    # Load Hugging Face model for embeddings
    model_name = "BAAI/bge-m3"
    tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=True)
    model = AutoModel.from_pretrained(model_name, local_files_only=True)

    client = Client(host='http://10.187.1.3:11434')
    # connect to the Ollama server locally
    # client = Client(host="http://host.docker.internal:11434")

    # Warm up the model by requesting it once
    model_name = "wangrongsheng/mistral-7b-v0.3-chinese:latest"
    response = client.chat(
            model=model_name,
            messages=[{"role": "user", "content": "你在嗎？"}]
        )

    print("Model warmed up:", response["message"]["content"])

    # Ensure GPU usage
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    # Store them in the global module
    global_vars.set_client(client)
    global_vars.set_model(model)
    global_vars.set_tokenizer(tokenizer)
    global_vars.set_dspy_globally()
    
    print(f"Model loaded and using device: {device}")

    yield

    print("Application shutting down. Database connection closed.")

# ✅ Attach lifespan to FastAPI in `main.py`
app = FastAPI(lifespan=lifespan)

# Add session middleware with a secret key, and HTTPS redirect middleware
app.add_middleware(SessionMiddleware, secret_key="840ecc2dfc070af39d1b")
# app.add_middleware(HTTPSRedirectMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"]   # Allows all headers
)

@app.middleware("http")
async def add_process_time_header(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    process_time = (time.perf_counter() - start) * 1000  # Convert to milliseconds
    response.headers["X-Process-Time-ms"] = str(process_time)
    return response

# Health check endpoint
@app.get("/healthy")
def health_check():
    return {"status": "healthy"}

@app.post("/alert")
def alert_endpoint(metric_value: float):
    """
    Endpoint to receive alert metrics.
    """
    print(f"Alert received with metric value: {metric_value}")
    return {"status": "alert received", "metric_value": metric_value}


# ✅ Include router WITHOUT lifespan
app.include_router(seg_recommend_ai.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0")
