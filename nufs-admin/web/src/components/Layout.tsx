import { Outlet, Link, useParams, useNavigate } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { listClusters, ClusterView } from '../api/client'

interface LayoutProps {
  onLogout: () => void
}

export default function Layout({ onLogout }: LayoutProps) {
  const [clusters, setClusters] = useState<ClusterView[]>([])
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null)
  const params = useParams()
  const navigate = useNavigate()

  useEffect(() => {
    listClusters().then(setClusters).catch(console.error)
  }, [])

  useEffect(() => {
    if (params.clusterId) {
      setSelectedCluster(params.clusterId)
    }
  }, [params.clusterId])

  const handleClusterSelect = (clusterName: string) => {
    setSelectedCluster(clusterName)
    navigate(`/clusters/${clusterName}/overview`)
  }

  return (
    <div style={{ display: 'flex', minHeight: '100vh', background: '#f5f6f8' }}>
      {/* Sidebar */}
      <aside style={{
        width: '220px',
        background: '#fff',
        borderRight: '1px solid #e2e6ec',
        padding: '16px 0',
      }}>
        <div style={{
          padding: '12px 20px',
          borderBottom: '1px solid #e2e6ec',
          marginBottom: '12px',
        }}>
          <h2 style={{ fontSize: '18px', fontWeight: 700, color: '#2563eb' }}>NUFS Admin</h2>
        </div>

        <div style={{ padding: '8px 20px' }}>
          <div
            onClick={() => { setSelectedCluster(null); navigate('/global') }}
            style={{
              padding: '10px 12px',
              borderRadius: '6px',
              cursor: 'pointer',
              background: !selectedCluster && !window.location.pathname.includes('/clusters/manage') ? '#eff6ff' : 'transparent',
              color: !selectedCluster && !window.location.pathname.includes('/clusters/manage') ? '#2563eb' : '#1a2233',
              fontWeight: !selectedCluster && !window.location.pathname.includes('/clusters/manage') ? 600 : 400,
            }}
          >
            多集群总览
          </div>

          <div
            onClick={() => { setSelectedCluster(null); navigate('/clusters/manage') }}
            style={{
              padding: '10px 12px',
              borderRadius: '6px',
              cursor: 'pointer',
              background: window.location.pathname.includes('/clusters/manage') ? '#eff6ff' : 'transparent',
              color: window.location.pathname.includes('/clusters/manage') ? '#2563eb' : '#1a2233',
              fontWeight: window.location.pathname.includes('/clusters/manage') ? 600 : 400,
            }}
          >
            集群管理
          </div>
        </div>

        <div style={{ padding: '12px 20px 8px', fontSize: '11px', color: '#9ca3af', fontWeight: 600 }}>
          集群列表
        </div>

        {clusters.map(cluster => (
          <div
            key={cluster.name}
            onClick={() => handleClusterSelect(cluster.name)}
            style={{
              padding: '10px 20px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              background: selectedCluster === cluster.name ? '#eff6ff' : 'transparent',
            }}
          >
            <span style={{
              width: '8px',
              height: '8px',
              borderRadius: '50%',
              background: cluster.health === 'healthy' ? '#16a34a' :
                          cluster.health === 'unhealthy' ? '#dc2626' : '#9ca3af',
            }} />
            <span style={{
              color: selectedCluster === cluster.name ? '#2563eb' : '#1a2233',
              fontWeight: selectedCluster === cluster.name ? 600 : 400,
            }}>
              {cluster.name}
            </span>
            <span style={{ fontSize: '11px', color: '#9ca3af' }}>
              {cluster.region}
            </span>
          </div>
        ))}
      </aside>

      {/* Main content */}
      <main style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        {/* Top bar */}
        <header style={{
          height: '56px',
          background: '#fff',
          borderBottom: '1px solid #e2e6ec',
          padding: '0 24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}>
          <div style={{ fontSize: '16px', fontWeight: 600 }}>
            {selectedCluster ? `集群: ${selectedCluster}` : '多集群总览'}
          </div>
          <button
            onClick={onLogout}
            style={{
              padding: '6px 16px',
              background: '#f5f6f8',
              border: '1px solid #e2e6ec',
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '13px',
            }}
          >
            退出登录
          </button>
        </header>

        {/* Tab bar (only when cluster selected) */}
        {selectedCluster && !window.location.pathname.includes('/clusters/manage') && (
          <div style={{
            background: '#fff',
            borderBottom: '1px solid #e2e6ec',
            padding: '0 24px',
            display: 'flex',
            gap: '24px',
          }}>
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
                style={{
                  padding: '14px 0',
                  color: params['*']?.includes(tab.path) ? '#2563eb' : '#5a6478',
                  fontWeight: params['*']?.includes(tab.path) ? 600 : 400,
                  borderBottom: params['*']?.includes(tab.path) ? '2px solid #2563eb' : 'none',
                  textDecoration: 'none',
                }}
              >
                {tab.label}
              </Link>
            ))}
          </div>
        )}

        {/* Content */}
        <div style={{ flex: 1, padding: '24px', overflow: 'auto' }}>
          <Outlet />
        </div>
      </main>
    </div>
  )
}