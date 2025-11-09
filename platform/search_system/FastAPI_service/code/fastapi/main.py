from fastapi import FastAPI, Response
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware
from routers import fastAPI_UI_elasticSearch
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
app = FastAPI()

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
 

# Health check endpoint
@app.get("/healthy")
def health_check():
    return {"status": "healthy"}


# Router for search
app.include_router(fastAPI_UI_elasticSearch.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app)
