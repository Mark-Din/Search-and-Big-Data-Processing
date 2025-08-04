FROM python:3.9.12-slim

# Set the working directory
WORKDIR /app

RUN pip install --no-cache-dir torch==2.2.2+cpu -f https://download.pytorch.org/whl/torch_stable.html

# Copy the requirements file into the container at /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY ./app /app

# Ensure models go here
ENV HF_HOME=/hf_cache

# Preload the model at build time to cache it under /root/.cache/huggingface.
COPY download_model.py /app/download_model.py
RUN python /app/download_model.py

#  Update the PATH environment variable to include the local bin directory
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Expose the port the app runs on
EXPOSE 8000

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]