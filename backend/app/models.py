from datetime import datetime
from sqlmodel import SQLModel, Field
from typing import Optional


class User(SQLModel, table=True):
    __tablename__ = "users"

    osu_id: int = Field(primary_key=True)
    username: str = Field(index=True)
    avatar_url: Optional[str] = None
    score: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Team(SQLModel, table=True):
    __tablename__ = "teams"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_osu_id: int = Field(index=True)  # The user who owns this team
    tournament: str = Field(default="owc2025")  # Tournament identifier
    player_ids: str = Field(default="")  # Comma-separated player IDs from the tournament
    budget_used: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
