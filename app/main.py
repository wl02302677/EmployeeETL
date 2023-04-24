from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel, EmailStr
from router.api_router import api_router
from fastapi.middleware.cors import CORSMiddleware


def include_router(app):
    app.include_router(api_router)


def start_application():
    app = FastAPI()

    origins = ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    include_router(app)
    return app


app = start_application()
