"""Runtime config profiles for McCain Capital."""

import os


class BaseConfig:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key")
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "0") == "1"
    PERMANENT_SESSION_LIFETIME_MINUTES = int(os.environ.get("SESSION_LIFETIME_MIN", "720"))


class DevConfig(BaseConfig):
    DEBUG = os.environ.get("DEBUG", "0") == "1"


class ProdConfig(BaseConfig):
    DEBUG = False


def select_config():
    env = os.environ.get("APP_ENV", "dev").lower().strip()
    if env in {"prod", "production"}:
        return ProdConfig
    return DevConfig
