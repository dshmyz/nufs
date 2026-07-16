import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useState, useEffect } from 'react'
import Login from './pages/Login'
import Layout from './components/Layout'
import GlobalOverview from './pages/global/GlobalOverview'
import ClusterOverview from './pages/overview/ClusterOverview'
import Nodes from './pages/nodes/Nodes'
import Buckets from './pages/buckets/Buckets'
import Integrity from './pages/integrity/Integrity'
import Governance from './pages/governance/Governance'
import Clusters from './pages/clusters/Clusters'
import { api } from './api/client'

function App() {
  const [token, setToken] = useState<string | null>(localStorage.getItem('token'))

  useEffect(() => {
    const storedToken = localStorage.getItem('token')
    if (storedToken) {
      setToken(storedToken)
    }
  }, [])

  const handleLogin = (newToken: string) => {
    localStorage.setItem('token', newToken)
    setToken(newToken)
  }

  const handleLogout = () => {
    localStorage.removeItem('token')
    setToken(null)
  }

  if (!token) {
    return <Login onLogin={handleLogin} />
  }

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout onLogout={handleLogout} />}>
          <Route index element={<Navigate to="/global" replace />} />
          <Route path="global" element={<GlobalOverview />} />
          <Route path="clusters/manage" element={<Clusters />} />
          <Route path="clusters/:clusterId/overview" element={<ClusterOverview />} />
          <Route path="clusters/:clusterId/nodes" element={<Nodes />} />
          <Route path="clusters/:clusterId/buckets" element={<Buckets />} />
          <Route path="clusters/:clusterId/integrity" element={<Integrity />} />
          <Route path="clusters/:clusterId/governance" element={<Governance />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App