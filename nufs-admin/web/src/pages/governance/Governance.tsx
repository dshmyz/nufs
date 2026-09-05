import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import {
  getRaftStatus,
  getAuditLogs,
  getBackupStatus,
  triggerBackup,
  getClusterBalance,
  getBackgroundTasks,
  getECQueue,
  RaftStatus,
  AuditLog,
} from '../../api/client'

const card: React.CSSProperties = { background: 'var(--bg-elev)', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px', marginBottom: '24px' }

export default function Governance() {
  const { clusterId } = useParams()
  const [raft, setRaft] = useState<RaftStatus | null>(null)
  const [logs, setLogs] = useState<AuditLog[]>([])
  const [backup, setBackup] = useState<any>(null)
  const [backupErr, setBackupErr] = useState('')
  const [tasks, setTasks] = useState<any[]>([])
  const [ecQueue, setEcQueue] = useState<any[]>([])
  const [balance, setBalance] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  const loadOps = () => {
    if (!clusterId) return
    getBackupStatus(clusterId)
      .then(d => { setBackup(d); setBackupErr('') })
      .catch(() => setBackupErr('未启用元数据备份（metad 未配置 --backup-enabled）'))
    getBackgroundTasks(clusterId).then(setTasks).catch(() => setTasks([]))
    getECQueue(clusterId).then(setEcQueue).catch(() => setEcQueue([]))
    getClusterBalance(clusterId).then(setBalance).catch(() => setBalance(null))
  }

  useEffect(() => {
    if (!clusterId) return
    Promise.all([
      getRaftStatus(clusterId),
      getAuditLogs(clusterId, { limit: 20 })
    ])
      .then(([raftData, logsData]) => {
        setRaft(raftData)
        setLogs(logsData)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
    loadOps()
  }, [clusterId])

  const handleTriggerBackup = async () => {
    if (!clusterId) return
    if (!window.confirm('立即触发一次元数据备份？')) return
    try {
      await triggerBackup(clusterId)
      window.alert('备份已触发')
      setTimeout(loadOps, 3000)
    } catch (err) {
      window.alert('触发失败：' + (err as any)?.response?.data?.error || String(err))
    }
  }

  if (loading) return <div>加载中...</div>

  const backupStatus: string = backup?.Status?.status || backup?.status || ''
  const lastTask = backup?.Catalog?.tasks?.[0] || backup?.Catalog?.last_task
  const nodes = balance?.nodes || []
  const imbalance = balance?.imbalance

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>集群治理</h1>

      <div style={card}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>Raft 状态</h3>
        {raft && (
          <div style={{ fontSize: '13px', color: 'var(--text-dim)', lineHeight: 1.8 }}>
            <div>Leader: {raft.leader}</div>
            <div>Term: {raft.term}</div>
            <div>Commit: {raft.commit}</div>
            <div>Applied: {raft.applied}</div>
          </div>
        )}
      </div>

      {/* 元数据运维：备份 / 后台任务 / EC 队列 / 容量均衡 */}
      <div style={card}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: 700, margin: 0 }}>元数据运维</h3>
          <button className="btn btn-sm" onClick={loadOps}>刷新</button>
        </div>

        {/* 元数据健康 KPI */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 10, marginBottom: 12 }}>
          <div className="stat">
            <div className="label">备份状态</div>
            <div className="value" style={{ fontSize: 18, color: backupErr ? 'var(--danger)' : backupStatus === 'ready' || backupStatus === 'healthy' ? 'var(--ok)' : 'var(--warn)' }}>
              {backupErr ? '未启用' : (backupStatus || '—')}
            </div>
          </div>
          <div className="stat">
            <div className="label">后台任务</div>
            <div className="value" style={{ fontSize: 18 }}>{tasks.length}</div>
          </div>
          <div className="stat">
            <div className="label">EC 队列</div>
            <div className="value" style={{ fontSize: 18, color: ecQueue.length > 0 ? 'var(--warn)' : 'var(--ok)' }}>{ecQueue.length}</div>
          </div>
          <div className="stat">
            <div className="label">容量不均衡度</div>
            <div className="value" style={{ fontSize: 18, color: typeof imbalance === 'number' && imbalance > 0.15 ? 'var(--warn)' : 'var(--ok)' }}>
              {typeof imbalance === 'number' ? imbalance.toFixed(2) : '—'}
            </div>
          </div>
        </div>

        {/* 备份 */}
        <div style={{ padding: '10px 12px', background: 'var(--bg-hover)', borderRadius: '8px', marginBottom: '12px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
            <div>
              <div style={{ color: 'var(--text-dim)', fontSize: '12px' }}>元数据备份</div>
              {backupErr ? (
                <span style={{ color: 'var(--danger)', fontSize: '13px' }}>{backupErr}</span>
              ) : (
                <span style={{ fontSize: '13px' }}>
                  <b>{backupStatus || '—'}</b>
                  {lastTask && <span style={{ color: 'var(--text-dim)', marginLeft: '8px' }}>{new Date(lastTask.ts ?? lastTask.completed_at ?? lastTask.created_at).toLocaleString()}</span>}
                </span>
              )}
            </div>
            <button onClick={handleTriggerBackup} disabled={!!backupErr} style={{ padding: '4px 12px', background: 'var(--accent)', color: 'var(--bg-elev)', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
              立即备份
            </button>
          </div>
        </div>

        {/* 容量均衡（图表） */}
        {nodes.length > 0 && (
          <div style={{ marginBottom: '12px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
              <span style={{ color: 'var(--text-dim)', fontSize: '12px' }}>容量均衡</span>
              {typeof imbalance === 'number' && (
                <span className={`badge ${imbalance > 0.2 ? 'badge-warn' : 'badge-ok'}`}>不均衡度 {imbalance.toFixed(2)}</span>
              )}
            </div>
            {(() => {
              const pcts: number[] = nodes.map((n: any) => n.used_pct ?? 0)
              const avg = pcts.reduce((a, b) => a + b, 0) / pcts.length
              const max = Math.max(...pcts)
              const min = Math.min(...pcts)
              const sorted = [...nodes].sort((a: any, b: any) => (b.used_pct ?? 0) - (a.used_pct ?? 0))
              const maxBar = Math.max(max, 1)
              return (
                <>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(90px, 1fr))', gap: 8, marginBottom: 10 }}>
                    <MiniStat label="最大" value={`${max.toFixed(1)}%`} tone={max > 85 ? 'danger' : max > 70 ? 'warn' : 'ok'} />
                    <MiniStat label="平均" value={`${avg.toFixed(1)}%`} />
                    <MiniStat label="最小" value={`${min.toFixed(1)}%`} tone="accent" />
                  </div>
                  {sorted.map((n: any, i: number) => {
                    const p = n.used_pct ?? 0
                    const color = p > 85 ? 'var(--danger)' : p > 70 ? 'var(--warn)' : 'var(--accent)'
                    return (
                      <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12, padding: '2px 0' }}>
                        <span style={{ width: 40, color: 'var(--text-dim)' }}>node-{n.id}</span>
                        <div style={{ flex: 1, height: 10, background: 'var(--line)', borderRadius: 4, overflow: 'hidden', position: 'relative' }}>
                          <div style={{ width: `${(p / maxBar) * 100}%`, height: '100%', background: `linear-gradient(90deg, ${color}66, ${color})`, borderRadius: 4, transition: 'width .4s ease' }} />
                        </div>
                        <span className="mono" style={{ width: 52, textAlign: 'right', color: 'var(--text)' }}>{p.toFixed(1)}%</span>
                      </div>
                    )
                  })}
                </>
              )
            })()}
          </div>
        )}

        {/* 后台任务 */}
        {tasks.length > 0 && (
          <div style={{ marginBottom: '12px' }}>
            <div style={{ color: 'var(--text-dim)', fontSize: '12px', marginBottom: '6px' }}>后台任务</div>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <tbody>
                {tasks.map((t: any, i: number) => (
                  <tr key={i}>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid #eef1f5', fontSize: '12px' }}>{t.type ?? t.id}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid #eef1f5', fontSize: '12px' }}>{t.state ?? t.status}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid #eef1f5', fontSize: '12px', color: 'var(--text-dim)' }}>{t.owner ?? ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* EC 转换队列 */}
        {ecQueue.length > 0 && (
          <div>
            <div style={{ color: 'var(--text-dim)', fontSize: '12px', marginBottom: '6px' }}>EC 转换队列（{ecQueue.length}）</div>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <tbody>
                {ecQueue.map((t: any, i: number) => (
                  <tr key={i}>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid #eef1f5', fontSize: '12px' }}>{t.bucket ?? t.name}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid #eef1f5', fontSize: '12px' }}>{t.state ?? t.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {backupErr && tasks.length === 0 && ecQueue.length === 0 && nodes.length === 0 && (
          <div style={{ color: 'var(--text-dim)', fontSize: '13px' }}>（无元数据运维数据）</div>
        )}
      </div>

      <div style={card}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>审计日志</h3>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ background: 'var(--bg-hover)' }}>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>时间</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>用户</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>资源</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>结果</th>
            </tr>
          </thead>
          <tbody>
            {logs.map((log, i) => (
              <tr key={i}>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.timestamp}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.user}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.action}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.resource}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.result}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function MiniStat({ label, value, tone }: { label: string; value: string; tone?: 'ok' | 'warn' | 'danger' | 'accent' }) {
  const color = tone === 'ok' ? 'var(--ok)' : tone === 'warn' ? 'var(--warn)' : tone === 'danger' ? 'var(--danger)' : tone === 'accent' ? 'var(--accent)' : 'var(--text)'
  return (
    <div style={{ background: 'var(--bg-hover)', border: '1px solid var(--line)', borderRadius: 'var(--radius-sm)', padding: '8px 10px' }}>
      <div className="faint" style={{ fontSize: 11 }}>{label}</div>
      <div className="mono" style={{ fontSize: 15, fontWeight: 700, color }}>{value}</div>
    </div>
  )
}
