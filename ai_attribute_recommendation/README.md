# SegDB_attribute_recommendation
Auto create attribute recommendataion for the predefined segmentation name

# generative_ia.py is a prototype for creating an AI
model wangrongsheng/mistral-7b-v0.3-chinese:latest is better than llama3:8b in chinese kind of questions

# python backend in docker
docker build -t seg-ai-image --rm .
docker run -d --name seg_ai_container -v "${PWD}\app:/app" -p 8000:8000 seg-ai-image

# postgresql in docker 
docker run --name pgvector-db -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=infopower -p 9100:5432 -d ankane/pgvector
