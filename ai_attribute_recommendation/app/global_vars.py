# global_vars.py - Shared storage for global variables


import dspy
model = None
tokenizer = None
client = None
lm = None

def set_model(m):
    global model
    model = m

def set_tokenizer(t):
    global tokenizer
    tokenizer = t

def set_client(c):
    global client
    client = c

def set_dspy_globally():
    global lm
    lm = dspy.LM(
        'gemma:2b-instruct-v1.1-q2_K',  # Use 'ollama/mistral' instead of 'ollama_chat/mistral'
        api_base="http://172.21.96.1:11434",  # Ensure it connects to local Ollama
        api_key=None  # No API key needed for local Ollama
    )
    