# backend/app/main.py
from fastapi import FastAPI
from .routers import promotions

app = FastAPI(title="Fly Wise - Backend (MVP)")

app.include_router(promotions.router)

@app.get("/")
def root():
    return {"message": "Fly Wise API rodando 🚀"}
