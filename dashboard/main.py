from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dashboard.api.alerts import router as alerts_router
from dashboard.api.health import router as health_router
from dashboard.api.symbols import router as symbols_router

app = FastAPI(
    title="Market Data Governance Dashboard",
    version="1.0"
)

# ✅ CORS CONFIG (REQUIRED FOR FRONTEND)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(alerts_router)
app.include_router(health_router)
app.include_router(symbols_router)
