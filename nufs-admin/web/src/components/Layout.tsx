import { Outlet, Link, useParams, useNavigate } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { listClusters, ClusterView } from '../api/client'
import { toggleTheme, initTheme } from '../theme'

interface LayoutProps {
  onLogout: () => void
}

export default function Layout({ onLogout }: LayoutProps) {
  const [clusters, setClusters] = useState<ClusterView[]>([])
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null)
  const [theme, setTheme] = useState<'light' | 'dark'>(initTheme())
  const params = useParams()
  const navigate = useNavigate()

  const handleTheme = () => setTheme(toggleTheme())

  useEffect(() => {
    listClusters().then(setClusters).catch(console.error)
  }, [])

  useEffect(() => {
    if (params.clusterId) setSelectedCluster(params.clusterId)
  }, [params.clusterId])

  const handleClusterSelect = (name: string) => {
    setSelectedCluster(name)
    navigate(`/clusters/${name}/overview`)
  }

  const isManage = window.location.pathname.includes('/clusters/manage')
  const isGlobal = !selectedCluster && !isManage

  return (
    <div className="app-shell">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="brand">
          <span className="mark">▧</span>
          <h2>NUFS</h2>
          <span className="sub">Console</span>
        </div>

        <nav className="side-nav">
          <div className={`nav-item ${isGlobal ? 'active' : ''}`} onClick={() => { setSelectedCluster(null); navigate('/global') }}>
            <span className="led led-idle" />
            <span className="nav-label">多集群总览</span>
          </div>
          <div className={`nav-item ${isManage ? 'active' : ''}`} onClick={() => { setSelectedCluster(null); navigate('/clusters/manage') }}>
            <span className="led led-idle" />
            <span className="nav-label">集群管理</span>
          </div>
        </nav>

        <div className="side-group">集群列表</div>
        <nav className="side-nav" style={{ overflowY: 'auto' }}>
          {clusters.map(cluster => (
            <div
              key={cluster.name}
              className={`nav-item ${selectedCluster === cluster.name ? 'active' : ''}`}
              onClick={() => handleClusterSelect(cluster.name)}
            >
              <span className={`led led-${cluster.health === 'healthy' ? 'ok' : cluster.health === 'unhealthy' ? 'danger' : 'idle'}`} />
              <span className="nav-label">{cluster.name}</span>
              <span className="faint" style={{ fontSize: 11 }}>{cluster.region}</span>
            </div>
          ))}
        </nav>
      </aside>

      {/* Main */}
      <main className="main-area">
        <header className="topbar">
          <div className="ctx">
            {selectedCluster ? <><span className="crumb">集群</span> / {selectedCluster}</> : '多集群总览'}
          </div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <button className="btn btn-ghost" onClick={handleTheme} title="切换浅色/深色主题">
              {theme === 'dark' ? '☀ 浅色' : '☾ 深色'}
            </button>
            <button className="btn btn-ghost" onClick={onLogout}>退出登录</button>
          </div>
        </header>

        {selectedCluster && !isManage && (
          <div className="tabs">
            {[
              { path: 'overview', label: '概览' },
              { path: 'nodes', label: '节点' },
              { path: 'buckets', label: 'Bucket' },
              { path: 'integrity', label: '数据完整性' },
              { path: 'governance', label: '集群治理' },
            ].map(tab => (
              <Link
                key={tab.path}
                to={`/clusters/${selectedCluster}/${tab.path}`}
                className={`tab ${params['*']?.includes(tab.path) ? 'active' : ''}`}
              >
                {tab.label}
              </Link>
            ))}
          </div>
        )}

        <div className="content">
          <Outlet />
        </div>
      </main>
    </div>
  )
}
