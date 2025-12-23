import React, { useState, useEffect } from 'react';
import axios from 'axios';
import Button from './ui/Button';

// Configure axios to send cookies with requests
axios.defaults.withCredentials = true;

// 配置后端 API 基础 URL
const API_BASE_URL = 'http://localhost:8000';

const AuthButton = () => {
  const [userData, setUserData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  // Check authentication status on mount
  useEffect(() => {
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/auth/status`);
      setIsAuthenticated(response.data.authenticated);
      if (response.data.authenticated) {
        fetchUserData();
      }
    } catch (err) {
      console.error('Auth status check failed:', err);
      setIsAuthenticated(false);
    }
  };

  // 开始授权流程
  const handleLogin = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // 第一步：重定向到 FastAPI 的 /auth/login 端点
      // 该端点会进一步重定向到 osu! 授权页面
      window.location.href = `${API_BASE_URL}/auth/login`;
    } catch (err) {
      setError('重定向失败: ' + err.message);
      setLoading(false);
    }
  };

  // 处理授权回调（应在回调页面调用）
  const handleCallback = async () => {
    // 从 URL 参数中获取授权码
    const urlParams = new URLSearchParams(window.location.search);
    const code = urlParams.get('code');
    
    if (!code) {
      setError('未收到授权码');
      return;
    }

    try {
      setLoading(true);
      // 发送授权码到后端换取访问令牌 (backend sets httpOnly cookies)
      const response = await axios.get(`${API_BASE_URL}/auth/callback`, {
        params: { code },
        withCredentials: true
      });

      if (response.data.success) {
        setIsAuthenticated(true);
        setError(null);
        
        // Fetch user data immediately after successful auth
        await fetchUserData();
        
        // 清除 URL 中的授权码参数
        window.history.replaceState({}, document.title, window.location.pathname);
      }
    } catch (err) {
      setError('授权失败: ' + (err.response?.data?.detail || err.message));
      setIsAuthenticated(false);
    } finally {
      setLoading(false);
    }
  };

  // 获取用户信息
  const fetchUserData = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE_URL}/api/user`, {
        withCredentials: true
      });
      
      setUserData(response.data);
      setError(null);
    } catch (err) {
      if (err.response?.status === 401) {
        // Token expired or invalid, try refresh
        await refreshAccessToken();
      } else {
        setError('获取用户信息失败: ' + (err.response?.data?.detail || err.message));
      }
    } finally {
      setLoading(false);
    }
  };

  // 检查 URL 是否有授权码（用于回调页面）
  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('code')) {
      handleCallback();
    }
  }, []);

  // 刷新访问令牌
  const refreshAccessToken = async () => {
    try {
      const response = await axios.post(`${API_BASE_URL}/auth/refresh`, {}, {
        withCredentials: true
      });

      if (response.data.success) {
        setIsAuthenticated(true);
        // Retry fetching user data
        await fetchUserData();
      }
    } catch (err) {
      console.error('令牌刷新失败:', err);
      setIsAuthenticated(false);
      setUserData(null);
    }
  };

  // 登出
  const handleLogout = async () => {
    try {
      await axios.post(`${API_BASE_URL}/auth/logout`, {}, {
        withCredentials: true
      });
      setIsAuthenticated(false);
      setUserData(null);
      setError(null);
    } catch (err) {
      console.error('Logout failed:', err);
      // Force logout on frontend even if backend call fails
      setIsAuthenticated(false);
      setUserData(null);
    }
  };

  return (
    <div className="auth-container">
      {isAuthenticated && userData && (
        <div style={{ position: 'fixed', top: 12, right: 12, zIndex: 1000, display: 'flex', alignItems: 'center', gap: 8 }}>
          {(() => {
            const avatar = userData.avatar_url;
            const username = userData.username;
            if (avatar && username) return (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ color: '#fff', fontWeight: 500 }}>{username}</div>
                <img 
                  src={avatar} 
                  alt={`${username} avatar`} 
                  style={{ 
                    width: 40, 
                    height: 40, 
                    borderRadius: '50%', 
                    objectFit: 'cover', 
                    border: '2px solid rgba(255,255,255,0.9)' 
                  }} 
                />
              </div>
            );
            return null;
          })()}

          <Button 
            onClick={handleLogout} 
            style={{ 
              backgroundColor: '#24222A', 
              color: '#fff', 
              width: '40px', 
              height: '40px', 
              padding: 0, 
              borderRadius: '6px' 
            }} 
            className="flex items-center justify-center" 
            aria-label="Logout" 
            title="Logout"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M16 17l5-5-5-5" />
              <path d="M21 12H9" />
              <path d="M9 19H5a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h4" />
            </svg>
          </Button>
        </div>
      )}
      
      {loading && <p>加载中...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}
      
      {!isAuthenticated ? (
        <button onClick={handleLogin} disabled={loading} className="button-texture">
          {loading ? '正在连接到 osu!...' : 'Login with osu!'}
        </button>
      ) : (
        <div>
          <div style={{ marginTop: '20px' }}>
            {/* Authenticated content */}
          </div>
        </div>
      )}
    </div>
  );
};

export default AuthButton;
