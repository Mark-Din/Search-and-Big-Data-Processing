# Ren streamlit and fastapi

#!/bin/bash

# Start FastAPI service in the background
uvicorn /app/main:app --host 0.0.0.0 --port 3002 --reload &
# Start Streamlit service
streamlit run /app/streamlit/streamlit_app.py --server.port 8501 --server.address 0.