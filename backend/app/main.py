from fastapi import FastAPI, Request, HTTPException, Response, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from sqlmodel import Session, select
from datetime import datetime
import httpx
import os
import sqlite3
from pathlib import Path
from urllib.parse import urlencode
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional
from app.database import create_db_and_tables, get_session, engine
from app.models import User, Team

# Path to players database
PLAYERS_DB_PATH = Path(__file__).parent / "players.db"

# Budget configuration
TOTAL_BUDGET = 100
MAX_TEAM_SIZE = 8
DEFAULT_PLAYER_COST = 10  # Base cost per player

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / "env")

app = FastAPI()

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

# 配置 CORS，允许 React 前端访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],  # Vite 默认端口
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLIENT_ID = os.getenv("OSU_CLIENT_ID")
CLIENT_SECRET = os.getenv("OSU_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
AUTH_URL = "https://osu.ppy.sh/oauth/authorize"
TOKEN_URL = "https://osu.ppy.sh/oauth/token"
SCOPES = "public identify"

@app.get("/auth/login")
async def login():
    """生成 osu! 授权链接并重定向用户"""
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
    }
    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(auth_url)

@app.get("/auth/callback")
async def auth_callback(code: str, state: str = None):
    """处理 osu! 的回调，用授权码换取访问令牌"""
    print(f"Received code: {code}, state: {state}")
    if not code:
        raise HTTPException(status_code=400, detail="缺少授权码")
    
    # 准备请求数据
    token_data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
    }
    
    # 向 osu! 请求访问令牌
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                TOKEN_URL,
                data=token_data,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                timeout=30.0
            )
            
            if response.status_code == 200:
                token_info = response.json()
                access_token = token_info.get("access_token")
                refresh_token = token_info.get("refresh_token")
                expires_in = token_info.get("expires_in", 86400)  # Default 24 hours
                
                # Fetch user data from osu! API
                user_data = await fetch_osu_user_data(access_token)
                
                # Store user in database
                if user_data:
                    await store_user_in_db(user_data)
                
                # Create response and set httpOnly cookies
                json_response = JSONResponse({
                    "success": True,
                    "message": "Authentication successful"
                })
                
                # Set access token cookie (httpOnly, secure in production)
                json_response.set_cookie(
                    key="osu_access_token",
                    value=access_token,
                    httponly=True,
                    secure=False,  # Set to True in production with HTTPS
                    samesite="lax",
                    max_age=expires_in
                )
                
                # Set refresh token cookie (httpOnly, secure in production, longer expiry)
                json_response.set_cookie(
                    key="osu_refresh_token",
                    value=refresh_token,
                    httponly=True,
                    secure=False,  # Set to True in production with HTTPS
                    samesite="lax",
                    max_age=expires_in * 30  # Refresh token lasts longer
                )
                
                return json_response
            else:
                error_detail = response.json().get("error_description", "未知错误")
                raise HTTPException(status_code=400, detail=f"令牌请求失败: {error_detail}")
                
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"网络请求错误: {str(e)}")
    

@app.post("/auth/refresh")
async def refresh_token(request: Request):
    """使用刷新令牌获取新的访问令牌"""
    refresh_token = request.cookies.get("osu_refresh_token")
    
    if not refresh_token:
        raise HTTPException(status_code=401, detail="未找到刷新令牌")
    
    refresh_data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "scope": SCOPES
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            TOKEN_URL,
            data=refresh_data,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        
        if response.status_code == 200:
            token_info = response.json()
            access_token = token_info.get("access_token")
            new_refresh_token = token_info.get("refresh_token")
            expires_in = token_info.get("expires_in", 86400)
            
            json_response = JSONResponse({
                "success": True,
                "message": "Token refreshed successfully"
            })
            
            json_response.set_cookie(
                key="osu_access_token",
                value=access_token,
                httponly=True,
                secure=False,
                samesite="lax",
                max_age=expires_in
            )
            
            json_response.set_cookie(
                key="osu_refresh_token",
                value=new_refresh_token,
                httponly=True,
                secure=False,
                samesite="lax",
                max_age=expires_in * 30
            )
            
            return json_response
        else:
            raise HTTPException(status_code=400, detail="令牌刷新失败")

@app.get("/api/user")
async def get_current_user(request: Request):
    """使用访问令牌获取当前用户信息（示例API调用）"""
    access_token = request.cookies.get("osu_access_token")
    
    if not access_token:
        raise HTTPException(status_code=401, detail="未找到访问令牌")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://osu.ppy.sh/api/v2/me",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch user info")

@app.post("/auth/logout")
async def logout():
    """清除认证 cookies"""
    response = JSONResponse({"success": True, "message": "Logged out successfully"})
    response.delete_cookie(key="osu_access_token", samesite="lax")
    response.delete_cookie(key="osu_refresh_token", samesite="lax")
    return response

@app.get("/auth/status")
async def auth_status(request: Request):
    """检查用户是否已认证"""
    access_token = request.cookies.get("osu_access_token")
    return {"authenticated": bool(access_token)}

@app.get("/api/leaderboard")
async def get_leaderboard():
    """Get top players leaderboard from database"""
    with Session(engine) as session:
        try:
            statement = select(User).order_by(User.score.desc()).limit(10)
            users = session.exec(statement).all()
            
            leaderboard = [
                {
                    "position": idx + 1,
                    "osu_id": user.osu_id,
                    "username": user.username,
                    "avatar_url": user.avatar_url,
                    "score": user.score
                }
                for idx, user in enumerate(users)
            ]
            
            return {"leaderboard": leaderboard}
        except Exception as e:
            print(f"Error fetching leaderboard: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch leaderboard")

async def fetch_osu_user_data(access_token: str):
    """Fetch user data from osu! API"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                "https://osu.ppy.sh/api/v2/me",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Error fetching user data: {e}")
            return None

async def store_user_in_db(user_data: dict):
    """Store or update user in database"""
    with Session(engine) as session:
        try:
            osu_id = user_data.get("id")
            username = user_data.get("username")
            avatar_url = user_data.get("avatar_url")

            # Get statistics (score might be in different fields depending on mode)
            statistics = user_data.get("statistics", {})
            score = statistics.get("ranked_score", 0)  # or total_score, pp, etc.

            # Check if user exists
            statement = select(User).where(User.osu_id == osu_id)
            existing_user = session.exec(statement).first()

            if existing_user:
                # Update existing user
                existing_user.username = username
                existing_user.avatar_url = avatar_url
                existing_user.score = score
                existing_user.updated_at = datetime.utcnow()
            else:
                # Create new user
                new_user = User(
                    osu_id=osu_id,
                    username=username,
                    avatar_url=avatar_url,
                    score=score
                )
                session.add(new_user)

            session.commit()
            print(f"User {username} (ID: {osu_id}) stored successfully")
        except Exception as e:
            print(f"Error storing user in database: {e}")
            session.rollback()


# ============ Player and Team Management API ============

def get_players_db():
    """Get connection to players database"""
    conn = sqlite3.connect(PLAYERS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/api/players")
async def get_players(tournament: str = "owc2025"):
    """Get all players from the tournament database"""
    table_name = "2025owc" if tournament == "owc2025" else tournament

    try:
        conn = get_players_db()
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, username, profile_url, avatar_url, country FROM "{table_name}"')
        rows = cursor.fetchall()
        conn.close()

        players = []
        for row in rows:
            # Calculate player cost based on their ID (can be adjusted later based on ranking/stats)
            player_id = row["id"]
            # Cost varies from 5 to 20 based on position in list (simulate ranking-based cost)
            cost = max(5, min(20, DEFAULT_PLAYER_COST + (player_id % 11) - 5))

            players.append({
                "id": player_id,
                "username": row["username"],
                "profile_url": row["profile_url"],
                "avatar_url": row["avatar_url"],
                "country": row["country"] or "Unknown",
                "cost": cost
            })

        return {
            "players": players,
            "total_budget": TOTAL_BUDGET,
            "max_team_size": MAX_TEAM_SIZE
        }
    except Exception as e:
        print(f"Error fetching players: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch players: {str(e)}")


class TeamSaveRequest(BaseModel):
    player_ids: List[int]
    tournament: str = "owc2025"


@app.get("/api/team")
async def get_team(request: Request, tournament: str = "owc2025"):
    """Get current user's team"""
    access_token = request.cookies.get("osu_access_token")

    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # Get user info to get osu_id
    user_data = await fetch_osu_user_data(access_token)
    if not user_data:
        raise HTTPException(status_code=401, detail="Failed to get user data")

    user_osu_id = user_data.get("id")

    with Session(engine) as session:
        statement = select(Team).where(
            Team.user_osu_id == user_osu_id,
            Team.tournament == tournament
        )
        team = session.exec(statement).first()

        if not team:
            return {
                "team": None,
                "player_ids": [],
                "budget_used": 0,
                "budget_remaining": TOTAL_BUDGET
            }

        player_ids = [int(pid) for pid in team.player_ids.split(",") if pid]

        return {
            "team": {
                "id": team.id,
                "tournament": team.tournament,
                "created_at": team.created_at.isoformat(),
                "updated_at": team.updated_at.isoformat()
            },
            "player_ids": player_ids,
            "budget_used": team.budget_used,
            "budget_remaining": TOTAL_BUDGET - team.budget_used
        }


@app.post("/api/team")
async def save_team(request: Request, team_data: TeamSaveRequest):
    """Save user's team"""
    access_token = request.cookies.get("osu_access_token")

    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # Get user info
    user_data = await fetch_osu_user_data(access_token)
    if not user_data:
        raise HTTPException(status_code=401, detail="Failed to get user data")

    user_osu_id = user_data.get("id")
    player_ids = team_data.player_ids
    tournament = team_data.tournament

    # Validate team size
    if len(player_ids) > MAX_TEAM_SIZE:
        raise HTTPException(status_code=400, detail=f"Team cannot have more than {MAX_TEAM_SIZE} players")

    # Calculate total cost
    table_name = "2025owc" if tournament == "owc2025" else tournament
    total_cost = 0

    try:
        conn = get_players_db()
        cursor = conn.cursor()

        for pid in player_ids:
            cursor.execute(f'SELECT id FROM "{table_name}" WHERE id = ?', (pid,))
            row = cursor.fetchone()
            if not row:
                conn.close()
                raise HTTPException(status_code=400, detail=f"Player with ID {pid} not found")
            # Calculate cost same as in get_players
            cost = max(5, min(20, DEFAULT_PLAYER_COST + (pid % 11) - 5))
            total_cost += cost

        conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate players: {str(e)}")

    # Check budget
    if total_cost > TOTAL_BUDGET:
        raise HTTPException(
            status_code=400,
            detail=f"Team cost ({total_cost}) exceeds budget ({TOTAL_BUDGET})"
        )

    # Save team
    with Session(engine) as session:
        statement = select(Team).where(
            Team.user_osu_id == user_osu_id,
            Team.tournament == tournament
        )
        existing_team = session.exec(statement).first()

        if existing_team:
            existing_team.player_ids = ",".join(str(pid) for pid in player_ids)
            existing_team.budget_used = total_cost
            existing_team.updated_at = datetime.utcnow()
        else:
            new_team = Team(
                user_osu_id=user_osu_id,
                tournament=tournament,
                player_ids=",".join(str(pid) for pid in player_ids),
                budget_used=total_cost
            )
            session.add(new_team)

        session.commit()

        return {
            "success": True,
            "message": "Team saved successfully",
            "budget_used": total_cost,
            "budget_remaining": TOTAL_BUDGET - total_cost
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
