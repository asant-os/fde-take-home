from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi import Depends

from app.config import load_config_and_env, Config
from app.repository import Base, Repository
from app.notifier import EmailNotifier, SlackNotifier

@lru_cache()
def get_config() -> Config:
    return load_config_and_env()

@lru_cache()
def get_engine():
    config = get_config()
    engine = create_engine(f"sqlite:///{config.sqlite_path}")
    Base.metadata.create_all(engine)
    return engine

def get_db():
    SessionLocal = sessionmaker(bind=get_engine(), expire_on_commit=False)
    db = SessionLocal()
    try:
        yield db
        db.commit() 
    except:
        db.rollback()
        raise
    finally:
        db.close()

def get_repo(db: Session = Depends(get_db)) -> Repository:
    return Repository(db)

@lru_cache()
def get_slack() -> SlackNotifier:
    config = get_config()
    return SlackNotifier(
        base_url=config.slack_webhook_base_url,
        single_webhook=config.slack_webhook_url,
        details_url=config.details_base_url,
        max_retries=config.slack_max_retries,
        base_backoff=config.slack_backoff_base,
        max_backoff=config.slack_backoff_max,
    )

@lru_cache
def get_email() -> EmailNotifier:
    config = get_config()
    return EmailNotifier(
        escalation_email=config.escalation_email,
    )