import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import {
  getClusterOverview,
  getClusterReadiness,
  getNodes,
  ClusterReadiness,
  NodeInfo,
} from '../../api/client'

const STATUS_COLOR: Record<string, string> = { ready: '#34d399', degraded: '#fbbf24', not_ready: '#f87171' }
const STATUS_LABEL: Record<string, string> = { ready: '就绪', degraded: '降级', not_ready: '未就绪' }

export default function ClusterOverview() {
  const { clusterId } = useParams()
  const [data, setData] = useState<any>(null)
  const [readiness, setReadiness] = useState<ClusterReadiness | null>(null)
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!clusterId) return
    Promise.all([
      getClusterOverview(clusterId).catch(() => null),
      getClusterReadiness(clusterId).catch(() => null),
      getNodes(clusterId).catch(() => []),
    ]).then(([overview, ready, ns]) => {
      setData(overview)
      setReadiness(ready)
      setNodes(ns)
    }).finally(() => setLoading(false))
  }, [clusterId])

  if (loading) return <div style={{ padding: '24px', color: 'var(--text-dim)' }}>加载中...</div>
  if (!data) return <div style={{ padding: '24px', color: 'var(--danger)' }}>加载失败</div>

  const fillClass = (pct: number) => (pct > 85 ? 'crit' : pct > 70 ? 'hot' : '')

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Cluster</div>
          <h1>{clusterId} 概览</h1>
        </div>
      </div>

      {/* Readiness banner */}
      {readiness && (
        <div className="panel panel-pad" style={{
          display: 'flex', alignItems: 'center', gap: 14, marginBottom: 18,
          borderColor: readiness.status === 'ready' ? 'rgba(52,211,153,.35)' : readiness.status === 'degraded' ? 'rgba(251,191,36,.35)' : 'rgba(248,113,113,.35)',
        }}>
          <span className={`led led-${readiness.status === 'ready' ? 'ok' : readiness.status === 'degraded' ? 'warn' : 'danger'}`} />
          <div>
            <div style={{ fontWeight: 700, color: STATUS_COLOR[readiness.status] || 'var(--text)' }}>
              集群状态：{STATUS_LABEL[readiness.status] || readiness.status}
            </div>
            <div className="muted" style={{ fontSize: 13, marginTop: 4 }}>
              可支撑 RF={readiness.can_write_rf} · 在线 {readiness.nodes_online}/{readiness.nodes_total} 节点
              {readiness.chunks_under_replicated > 0 && ` · ${readiness.chunks_under_replicated} 个 chunk 副本不足`}
              {!readiness.leader_stable && ' · ⚠️ Leader 不稳定'}
            </div>
          </div>
        </div>
      )}

      {/* 容量光谱（签名）：每个节点一个色块，fill=使用率 */}
      <div className="panel panel-pad" style={{ marginBottom: 18 }}>
        <div className="page-head" style={{ marginBottom: 8 }}>
          <h3>容量光谱</h3>
          <span className="muted" style={{ fontSize: 12 }}>{data.capacityUsed} / {data.capacityTotal} GB</span>
        </div>
        {nodes.length > 0 ? (
          <div className="spectrum">
            {nodes.map(n => {
              const pct = n.capacity > 0 ? (n.used / n.capacity) * 100 : 0
              return (
                <div className="spectrum-cell" key={n.id}>
                  <div className={`spectrum-fill ${fillClass(pct)}`} style={{ transform: `scaleX(${Math.min(100, pct) / 100})` }} />
                  <div className="spectrum-meta">
                    <span className="id">{n.id}</span>
                    <span className="pct">{pct.toFixed(1)}%</span>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          <div className="muted" style={{ fontSize: 13 }}>无节点数据</div>
        )}
      </div>

      {/* Metric cards */}
      <div className="stat-grid">
        <div className="stat">
          <div className="label">在线节点</div>
          <div className="value" style={{ color: readiness && readiness.nodes_online < readiness.nodes_total ? 'var(--warn)' : 'var(--ok)' }}>
            {readiness ? `${readiness.nodes_online}/${readiness.nodes_total}` : data.nodes}
          </div>
        </div>
        <div className="stat">
          <div className="label">容量使用</div>
          <div className="value">{data.capacityUsed} <span style={{ fontSize: 13, color: 'var(--text-faint)' }}>/ {data.capacityTotal} GB</span></div>
        </div>
        <div className="stat">
          <div className="label">Bucket 数</div>
          <div className="value" style={{ color: 'var(--accent)' }}>{data.buckets}</div>
        </div>
        <div className="stat">
          <div className="label">修复队列</div>
          <div className="value" style={{ color: (readiness?.repair_queue_depth ?? 0) > 0 ? 'var(--warn)' : 'var(--ok)' }}>
            {readiness?.repair_queue_depth ?? data.repairQueue ?? 0}
          </div>
        </div>
      </div>

      {/* Readiness checks detail */}
      {readiness && readiness.checks && Object.keys(readiness.checks).length > 0 && (
        <div className="panel panel-pad" style={{ marginBottom: 18 }}>
          <h3 style={{ marginBottom: 12 }}>健康检查明细</h3>
          <table className="dt">
            <thead>
              <tr><th>检查项</th><th>状态</th></tr>
            </thead>
            <tbody>
              {Object.entries(readiness.checks).map(([key, value]) => (
                <tr key={key}>
                  <td className="strong">{key}</td>
                  <td style={{ color: value === 'ok' || value === 'normal' ? 'var(--ok)' : 'var(--warn)' }}>{value}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="faint" style={{ fontSize: 11, marginTop: 12 }}>
            更新时间：{readiness.timestamp ? new Date(readiness.timestamp).toLocaleString() : '—'}
          </div>
        </div>
      )}

      {/* Ops guide */}
      <div className="panel panel-pad">
        <h3 style={{ marginBottom: 12 }}>运维功能</h3>
        <div className="muted" style={{ fontSize: 13, lineHeight: 2 }}>
          <div>· 节点管理：节点列表、下线节点、磁盘生命周期与可视化</div>
          <div>· Bucket 管理：创建/删除 Bucket、设置配额</div>
          <div>· 数据完整性：触发修复、修复队列、GC 扫描、再平衡</div>
          <div>· 集群治理：Raft 状态、元数据备份、后台任务、审计日志</div>
        </div>
      </div>
    </div>
  )
}
