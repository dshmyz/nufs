import { useState, useEffect } from 'react'
import {
  listClusters,
  addCluster,
  removeCluster,
  getClusterAuditLogs,
  getWriteOpsStatus,
  ClusterInfo,
  ClusterAuditLog,
  WriteOpsStatus,
} from '../../api/client'

export default function Clusters() {
  const [clusters, setClusters] = useState<ClusterInfo[]>([])
  const [logs, setLogs] = useState<ClusterAuditLog[]>([])
  const [writeOps, setWriteOps] = useState<Record<string, WriteOpsStatus | { error: string }>>({})
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
      const statuses = await Promise.all(
        clusterData.map(async cluster => {
          try {
            return [cluster.name, await getWriteOpsStatus(cluster.name)] as const
          } catch (err: any) {
            const responseError = err.response?.data
            const message = typeof responseError === 'string'
              ? responseError
              : responseError?.error || err.message || '状态不可用'
            return [cluster.name, { error: message }] as const
          }
        })
      )
      setWriteOps(Object.fromEntries(statuses))
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

      {clusters.length > 0 && (
        <div style={{
          background: '#fff',
          border: '1px solid #e2e6ec',
          borderRadius: '10px',
          padding: '20px',
          marginBottom: '24px',
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '12px', marginBottom: '16px', flexWrap: 'wrap' }}>
            <div>
              <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '4px' }}>对象写入闭环</h3>
              <div style={{ fontSize: '12px', color: '#6b7280' }}>recovery / GC worker</div>
            </div>
            <button
              onClick={loadData}
              style={{
                padding: '7px 12px',
                background: '#f8fafc',
                color: '#334155',
                border: '1px solid #cbd5e1',
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '12px',
              }}
            >
              刷新
            </button>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '12px' }}>
            {clusters.map(cluster => {
              const status = writeOps[cluster.name]
              const isError = !status || 'error' in status
              const attempts = isError ? {} : status.attempts
              const recoveryNeeded = attempts.recovery_needed || 0
              const failed = attempts.failed || 0
              const halfWritten = (attempts.pending || 0) + (attempts.chunks_allocated || 0)
              const hasBacklog = recoveryNeeded + failed + halfWritten > 0

              return (
                <div key={cluster.name} style={{
                  border: '1px solid #e2e6ec',
                  borderRadius: '8px',
                  padding: '14px',
                  minWidth: 0,
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', alignItems: 'center', marginBottom: '12px' }}>
                    <div style={{ fontWeight: 700, fontSize: '14px', overflow: 'hidden', textOverflow: 'ellipsis' }}>{cluster.name}</div>
                    <span style={{
                      flex: '0 0 auto',
                      padding: '2px 8px',
                      borderRadius: '4px',
                      background: isError ? '#fee2e2' : hasBacklog ? '#fef3c7' : '#d1fae5',
                      color: isError ? '#b91c1c' : hasBacklog ? '#92400e' : '#047857',
                      fontSize: '12px',
                      fontWeight: 600,
                    }}>
                      {isError ? '不可用' : hasBacklog ? '有积压' : '正常'}
                    </span>
                  </div>

                  {isError ? (
                    <div style={{ color: '#b91c1c', background: '#fef2f2', borderRadius: '6px', padding: '10px', fontSize: '12px', wordBreak: 'break-word' }}>
                      {status?.error || '状态不可用'}
                    </div>
                  ) : (
                    <>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '8px', marginBottom: '12px' }}>
                        <WriteOpsMetric label="待恢复" value={recoveryNeeded} tone={recoveryNeeded > 0 ? 'warn' : 'ok'} />
                        <WriteOpsMetric label="失败" value={failed} tone={failed > 0 ? 'bad' : 'ok'} />
                        <WriteOpsMetric label="半写入" value={halfWritten} tone={halfWritten > 0 ? 'warn' : 'ok'} />
                      </div>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '8px' }}>
                        <WorkerState label="Recovery" state={status.recovery_task?.state || 'unknown'} error={status.recovery_task?.last_error} />
                        <WorkerState label="GC" state={status.gc_task?.state || 'unknown'} error={status.gc_task?.last_error} />
                      </div>
                    </>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', marginBottom: '24px', overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ background: '#f5f6f8' }}>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>名称</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>区域</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>描述</th>
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

function WriteOpsMetric({ label, value, tone }: { label: string; value: number; tone: 'ok' | 'warn' | 'bad' }) {
  const colors = {
    ok: { background: '#f0fdf4', color: '#047857' },
    warn: { background: '#fffbeb', color: '#92400e' },
    bad: { background: '#fef2f2', color: '#b91c1c' },
  }[tone]

  return (
    <div style={{ background: colors.background, borderRadius: '6px', padding: '10px', minWidth: 0 }}>
      <div style={{ color: colors.color, fontSize: '18px', lineHeight: 1.2, fontWeight: 700 }}>{value}</div>
      <div style={{ color: '#64748b', fontSize: '12px', marginTop: '4px' }}>{label}</div>
    </div>
  )
}

function WorkerState({ label, state, error }: { label: string; state: string; error?: string }) {
  const isHealthy = state === 'succeeded' || state === 'leased' || state === 'queued'
  return (
    <div style={{ border: '1px solid #e2e6ec', borderRadius: '6px', padding: '10px', minWidth: 0 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '6px', alignItems: 'center', marginBottom: error ? '6px' : 0 }}>
        <span style={{ fontSize: '12px', color: '#64748b' }}>{label}</span>
        <span style={{
          padding: '1px 6px',
          borderRadius: '4px',
          background: isHealthy ? '#eef2ff' : '#fef2f2',
          color: isHealthy ? '#3730a3' : '#b91c1c',
          fontSize: '11px',
          fontWeight: 600,
          maxWidth: '110px',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}>
          {state}
        </span>
      </div>
      {error && (
        <div style={{ color: '#b91c1c', fontSize: '11px', wordBreak: 'break-word' }}>{error}</div>
      )}
    </div>
  )
}
