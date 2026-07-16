import { useState, useEffect } from 'react'
import {
  listClusters,
  addCluster,
  removeCluster,
  getClusterAuditLogs,
  ClusterInfo,
  ClusterAuditLog,
} from '../../api/client'

export default function Clusters() {
  const [clusters, setClusters] = useState<ClusterInfo[]>([])
  const [logs, setLogs] = useState<ClusterAuditLog[]>([])
  const [loading, setLoading] = useState(true)
  const [showAdd, setShowAdd] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [form, setForm] = useState({
    id: '',
    region: '',
    metad_ops_url: '',
    description: '',
  })

  const loadData = async () => {
    try {
      const [clusterData, logData] = await Promise.all([
        listClusters(),
        getClusterAuditLogs().catch(() => []),
      ])
      setClusters(clusterData)
      setLogs(logData)
    } catch (err: any) {
      setError(err.response?.data?.error || err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
  }, [])

  const handleAdd = async () => {
    setError(null)
    if (!form.id || !form.metad_ops_url) {
      setError('集群名称和 metad_ops_url 必填')
      return
    }

    try {
      await addCluster(form)
      setForm({ id: '', region: '', metad_ops_url: '', description: '' })
      setShowAdd(false)
      await loadData()
    } catch (err: any) {
      setError(err.response?.data?.error || err.message)
    }
  }

  const handleRemove = async (id: string) => {
    if (!confirm(`确认删除集群 ${id}？`)) return
    try {
      await removeCluster(id)
      await loadData()
    } catch (err: any) {
      setError(err.response?.data?.error || err.message)
    }
  }

  if (loading) return <div style={{ textAlign: 'center', padding: '40px' }}>加载中...</div>

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '24px' }}>
        <h1 style={{ fontSize: '24px', fontWeight: 700 }}>集群管理</h1>
        <button
          onClick={() => setShowAdd(!showAdd)}
          style={{
            padding: '8px 16px',
            background: '#2563eb',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer',
          }}
        >
          {showAdd ? '取消' : '添加集群'}
        </button>
      </div>

      {error && (
        <div style={{
          padding: '12px',
          background: '#fee2e2',
          color: '#dc2626',
          borderRadius: '8px',
          marginBottom: '16px',
          fontSize: '13px',
        }}>
          {error}
        </div>
      )}

      {showAdd && (
        <div style={{
          background: '#fff',
          border: '1px solid #e2e6ec',
          borderRadius: '10px',
          padding: '20px',
          marginBottom: '24px',
        }}>
          <h3 style={{ fontSize: '14px', fontWeight: 700, marginBottom: '16px' }}>添加新集群</h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginBottom: '12px' }}>
            <div>
              <label style={{ fontSize: '12px', color: '#5a6478', display: 'block', marginBottom: '4px' }}>集群名称 *</label>
              <input
                value={form.id}
                onChange={(e) => setForm({ ...form, id: e.target.value })}
                placeholder="例如: gz-prod"
                style={{ width: '100%', padding: '8px', border: '1px solid #e2e6ec', borderRadius: '6px' }}
              />
            </div>
            <div>
              <label style={{ fontSize: '12px', color: '#5a6478', display: 'block', marginBottom: '4px' }}>区域</label>
              <input
                value={form.region}
                onChange={(e) => setForm({ ...form, region: e.target.value })}
                placeholder="例如: guangzhou"
                style={{ width: '100%', padding: '8px', border: '1px solid #e2e6ec', borderRadius: '6px' }}
              />
            </div>
            <div style={{ gridColumn: 'span 2' }}>
              <label style={{ fontSize: '12px', color: '#5a6478', display: 'block', marginBottom: '4px' }}>metad Ops URL *</label>
              <input
                value={form.metad_ops_url}
                onChange={(e) => setForm({ ...form, metad_ops_url: e.target.value })}
                placeholder="例如: http://10.0.4.3:8091"
                style={{ width: '100%', padding: '8px', border: '1px solid #e2e6ec', borderRadius: '6px' }}
              />
            </div>
            <div style={{ gridColumn: 'span 2' }}>
              <label style={{ fontSize: '12px', color: '#5a6478', display: 'block', marginBottom: '4px' }}>描述</label>
              <input
                value={form.description}
                onChange={(e) => setForm({ ...form, description: e.target.value })}
                placeholder="例如: 广州生产集群"
                style={{ width: '100%', padding: '8px', border: '1px solid #e2e6ec', borderRadius: '6px' }}
              />
            </div>
          </div>
          <button
            onClick={handleAdd}
            style={{
              padding: '10px 20px',
              background: '#16a34a',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
              marginTop: '8px',
            }}
          >
            确认添加
          </button>
        </div>
      )}

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', marginBottom: '24px', overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ background: '#f5f6f8' }}>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>名称</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>区域</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>Ops URL</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>健康状态</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>来源</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作</th>
            </tr>
          </thead>
          <tbody>
            {clusters.map(cluster => (
              <tr key={cluster.name}>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec', fontWeight: 600 }}>{cluster.name}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{cluster.region || '-'}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec', fontFamily: 'monospace', fontSize: '12px' }}>
                  {cluster.description || '-'}
                </td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                  <span style={{
                    padding: '2px 8px',
                    borderRadius: '4px',
                    background: cluster.health === 'healthy' ? '#d1fae5' :
                                cluster.health === 'unhealthy' ? '#fee2e2' : '#f3f4f6',
                    color: cluster.health === 'healthy' ? '#047857' :
                           cluster.health === 'unhealthy' ? '#b91c1c' : '#6b7280',
                  }}>
                    {cluster.health}
                  </span>
                </td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                  <span style={{
                    padding: '2px 8px',
                    borderRadius: '4px',
                    background: cluster.source === 'static' ? '#dbeafe' : '#fef3c7',
                    color: cluster.source === 'static' ? '#1d4ed8' : '#b45309',
                  }}>
                    {cluster.source === 'static' ? 'YAML' : '动态'}
                  </span>
                </td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                  {cluster.source === 'dynamic' ? (
                    <button
                      onClick={() => handleRemove(cluster.name)}
                      style={{
                        padding: '4px 12px',
                        background: '#fee2e2',
                        color: '#dc2626',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: 'pointer',
                      }}
                    >
                      删除
                    </button>
                  ) : (
                    <span style={{ color: '#9ca3af', fontSize: '12px' }}>基线不可删</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>变更审计日志</h3>
        {logs.length === 0 ? (
          <p style={{ color: '#9ca3af', fontSize: '13px' }}>暂无变更记录</p>
        ) : (
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ background: '#f5f6f8' }}>
                <th style={{ padding: '10px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>时间</th>
                <th style={{ padding: '10px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>集群</th>
                <th style={{ padding: '10px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作</th>
                <th style={{ padding: '10px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作人</th>
                <th style={{ padding: '10px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>详情</th>
              </tr>
            </thead>
            <tbody>
              {logs.map(log => (
                <tr key={log.id}>
                  <td style={{ padding: '10px', borderBottom: '1px solid #e2e6ec', fontSize: '12px' }}>{log.created_at}</td>
                  <td style={{ padding: '10px', borderBottom: '1px solid #e2e6ec' }}>{log.cluster_id}</td>
                  <td style={{ padding: '10px', borderBottom: '1px solid #e2e6ec' }}>
                    <span style={{
                      padding: '2px 6px',
                      borderRadius: '4px',
                      background: log.action === 'add' ? '#d1fae5' :
                                  log.action === 'remove' ? '#fee2e2' : '#dbeafe',
                      color: log.action === 'add' ? '#047857' :
                             log.action === 'remove' ? '#b91c1c' : '#1d4ed8',
                    }}>
                      {log.action}
                    </span>
                  </td>
                  <td style={{ padding: '10px', borderBottom: '1px solid #e2e6ec' }}>{log.operator}</td>
                  <td style={{ padding: '10px', borderBottom: '1px solid #e2e6ec', fontSize: '12px', color: '#5a6478' }}>{log.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}