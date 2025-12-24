import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Landing from './pages/Landing'
import AuthButton from './components/AuthButton'
import CallbackPage from './components/CallbackPage'
import OWC2025Page from './pages/OWC2025Page'
import MyTeamPage from './pages/MyTeamPage'

function App(){
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Landing/>} />
        <Route path="/league/owc2025" element={<OWC2025Page />} />
        <Route path="/league/owc2025/my-team" element={<MyTeamPage />} />
        <Route path="/auth" element={<AuthButton />} />
        <Route path="/auth/callback" element={<CallbackPage />} />
      </Routes>
    </Router>
  )
}

export default App;