from fastapi import FastAPI
from .routers import segmentation

app = FastAPI(title="DuckMart User Segmentation API", version="1.0.0")

app.include_router(segmentation.router)

@app.get("/")
def root():
    return {"message": "DuckMart User Segmentation API"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}
