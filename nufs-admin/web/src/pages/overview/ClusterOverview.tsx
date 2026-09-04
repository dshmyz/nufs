import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import {
  getClusterOverview,
  getClusterReadiness,
  getNodes,
  getNodeMetrics,
  getNodeDisks,
  ClusterReadiness,
  NodeInfo,
  NodeDisk,
  NodeMetrics,
} from '../../api/client'

const STATUS_LABEL: Record<string, string> = { ready: '就绪', degraded: '降级', not_ready: '未就绪' }

export default function ClusterOverview() {
  const { clusterId } = useParams()
  const [data, setData] = useState<any>(null)
  const [readiness, setReadiness] = useState<ClusterReadiness | null>(null)
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [metrics, setMetrics] = useState<Record<string, NodeMetrics>>({})
  const [disks, setDisks] = useState<Record<string, NodeDisk[]>>({})
  const [rateHist, setRateHist] = useState<Record<string, { r: number[]; w: number[] }>>({})
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

  // 每节点指标轮询（IOPS/容量），2s 一次
  useEffect(() => {
    if (!clusterId || nodes.length === 0) return
    const poll = async () => {
      for (const n of nodes) {
        try {
          const m = await getNodeMetrics(clusterId, n.id)
          setMetrics(prev => ({ ...prev, [n.id]: m }))
          setRateHist(prev => {
            const cur = prev[n.id] || { r: [], w: [] }
            return { ...prev, [n.id]: { r: [...cur.r, m?.disk?.read_iops ?? 0].slice(-30), w: [...cur.w, m?.disk?.write_iops ?? 0].slice(-30) } }
          })
          const d = await getNodeDisks(clusterId, n.id)
          setDisks(prev => ({ ...prev, [n.id]: d }))
        } catch { /* ignore */ }
      }
    }
    poll()
    const t = setInterval(poll, 2000)
    return () => clearInterval(t)
  }, [clusterId, nodes.length])

  if (loading) return <div style={{ padding: '24px', color: 'var(--text-dim)' }}>加载中...</div>
  if (!data) return <div style={{ padding: '24px', color: 'var(--danger)' }}>加载失败</div>

  // 聚合 IOPS（所有节点之和）
  const sumIOPS = (k: 'read_iops' | 'write_iops') =>
    Object.values(metrics).reduce((acc, m) => acc + (m?.disk?.[k] ?? 0), 0)
  const totalChunks = Object.values(metrics).reduce((acc, m) => acc + (m?.disk?.chunk_count ?? 0), 0)
  const totalDisks = Object.values(disks).reduce((acc, d) => acc + d.length, 0)
  const ioErrors = Object.values(metrics).reduce((acc, m) => acc + (m?.disk?.io_errors ?? 0), 0)

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Cluster · {clusterId}</div>
          <h1>{clusterId} 概览</h1>
        </div>
        {readiness && (
          <span className={`badge ${readiness.status === 'ready' ? 'badge-ok' : readiness.status === 'degraded' ? 'badge-warn' : 'badge-danger'}`}>
            <span className={`led led-${readiness.status === 'ready' ? 'ok' : readiness.status === 'degraded' ? 'warn' : 'danger'}`} />
            {STATUS_LABEL[readiness.status] || readiness.status}
          </span>
        )}
      </div>

      {/* KPI 指标条（横向密集） */}
      <div className="stat-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', marginBottom: 18 }}>
        <KPI label="在线节点" value={readiness ? `${readiness.nodes_online}/${readiness.nodes_total}` : String(data.nodes ?? 0)} tone={readiness && readiness.nodes_online < readiness.nodes_total ? 'warn' : 'ok'} />
        <KPI label="总容量" value={`${data.capacityTotal ?? 0} GB`} tone="accent" />
        <KPI label="已用容量" value={`${data.capacityUsed ?? 0} GB`} />
        <KPI label="读 IOPS" value={String(sumIOPS('read_iops'))} unit="实时" tone="accent" />
        <KPI label="写 IOPS" value={String(sumIOPS('write_iops'))} unit="实时" />
        <KPI label="chunk" value={String(totalChunks)} />
        <KPI label="磁盘" value={String(totalDisks)} />
        <KPI label="Bucket" value={String(data.buckets ?? 0)} />
        <KPI label="修复队列" value={String(readiness?.repair_queue_depth ?? data.repairQueue ?? 0)} tone={ioErrors > 0 ? 'danger' : undefined} />
      </div>

      {/* 节点实时卡网格（视觉 + 信息密度） */}
      <div className="panel panel-pad" style={{ marginBottom: 18 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <h3>节点实时状态</h3>
          <span className="faint" style={{ fontSize: 12 }}>2s 刷新</span>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(250px, 1fr))', gap: 14 }}>
          {nodes.map(n => {
            const m = metrics[n.id]?.disk
            const pct = n.capacity > 0 ? (n.used / n.capacity) * 100 : 0
            const diskCount = disks[n.id]?.length ?? 0
            return (
              <div key={n.id} style={{ background: 'var(--bg-hover)', border: '1px solid var(--line)', borderRadius: 'var(--radius)', padding: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
                  <span className={`led led-${n.status === 'online' ? 'ok' : 'danger'}`} />
                  <b className="mono">node-{n.id}</b>
                  <span className="faint" style={{ fontSize: 11 }}>{n.address}</span>
                </div>
                <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
                  <CapacityRing pct={pct} used={n.used} cap={n.capacity} online={n.status === 'online'} small />
                  <div style={{ flex: 1, fontSize: 12, color: 'var(--text-dim)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>读</span><b className="mono" style={{ color: 'var(--accent)' }}>{m?.read_iops ?? 0} IOPS</b></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>写</span><b className="mono">{m?.write_iops ?? 0} IOPS</b></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>IO 错误</span><b className="mono" style={{ color: (m?.io_errors ?? 0) > 0 ? 'var(--danger)' : 'var(--text)' }}>{m?.io_errors ?? 0}</b></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>chunk</span><b className="mono">{m?.chunk_count ?? 0}</b></div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>磁盘</span><b className="mono">{diskCount}</b></div>
                  </div>
                </div>
                <div style={{ display: 'flex', gap: 12, marginTop: 10, borderTop: '1px solid var(--line)', paddingTop: 8 }}>
                  <Sparkline label="读" data={rateHist[n.id]?.r} />
                  <Sparkline label="写" data={rateHist[n.id]?.w} />
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* 系统健康 */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: 16 }}>
        {readiness && readiness.checks && Object.keys(readiness.checks).length > 0 && (
          <div className="panel panel-pad">
            <h3 style={{ marginBottom: 12 }}>健康检查</h3>
            <table className="dt">
              <tbody>
                {Object.entries(readiness.checks).map(([k, v]) => (
                  <tr key={k}>
                    <td className="strong">{k}</td>
                    <td style={{ color: v === 'ok' || v === 'normal' ? 'var(--ok)' : v === 'standalone' ? 'var(--warn)' : 'var(--warn)' }}>{v}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="faint" style={{ fontSize: 11, marginTop: 10 }}>
              更新时间 {readiness.timestamp ? new Date(readiness.timestamp).toLocaleTimeString() : '—'}
              · 可支撑 RF={readiness.can_write_rf}
              {readiness.chunks_under_replicated > 0 && ` · ⚠️ ${readiness.chunks_under_replicated} chunk 副本不足`}
            </div>
          </div>
        )}
        <div className="panel panel-pad">
          <h3 style={{ marginBottom: 12 }}>运维入口</h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            {[
              { label: '节点 / 磁盘', hint: '生命周期 · 下线 · 运维' },
              { label: 'Bucket 管理', hint: '创建 · 删除 · 配额' },
              { label: '数据完整性', hint: '修复 · GC · 再平衡' },
              { label: '集群治理', hint: 'Raft · 备份 · 审计' },
            ].map(x => (
              <div key={x.label} style={{ background: 'var(--bg-hover)', borderRadius: 'var(--radius-sm)', padding: '10px 12px' }}>
                <div style={{ fontWeight: 600, fontSize: 13 }}>{x.label}</div>
                <div className="faint" style={{ fontSize: 11, marginTop: 2 }}>{x.hint}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

function KPI({ label, value, unit, tone }: { label: string; value: string; unit?: string; tone?: 'ok' | 'warn' | 'danger' | 'accent' }) {
  const color = tone === 'ok' ? 'var(--ok)' : tone === 'warn' ? 'var(--warn)' : tone === 'danger' ? 'var(--danger)' : tone === 'accent' ? 'var(--accent)' : 'var(--text)'
  return (
    <div className="stat">
      <div className="label">{label}{unit && <span style={{ opacity: .7 }}> · {unit}</span>}</div>
      <div className="value" style={{ fontSize: 20, color }}>{value}</div>
    </div>
  )
}

// 节点容量环（SVG donut）：stroke=使用率，中心显示百分比
function CapacityRing({ pct, used, cap, online, small }: { pct: number; used: number; cap: number; online: boolean; small?: boolean }) {
  const size = small ? 64 : 72
  const R = small ? 23 : 26
  const C = 2 * Math.PI * R
  const clamped = Math.min(100, Math.max(0, pct))
  const color = clamped > 85 ? 'var(--danger)' : clamped > 70 ? 'var(--warn)' : 'var(--accent)'
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
      <svg width={size} height={size} style={{ transform: 'rotate(-90deg)' }}>
        <circle cx={size / 2} cy={size / 2} r={R} fill="none" stroke="var(--line-strong)" strokeWidth={6} />
        <circle
          cx={size / 2} cy={size / 2} r={R} fill="none"
          stroke={color} strokeWidth={6} strokeLinecap="round"
          strokeDasharray={`${(clamped / 100) * C} ${C}`}
          style={{ filter: `drop-shadow(0 0 4px ${color})`, transition: 'stroke-dasharray .6s ease' }}
        />
      </svg>
      <span className="mono" style={{ fontSize: 12, fontWeight: 700, color }}>{clamped.toFixed(1)}%</span>
      <span className="mono faint" style={{ fontSize: 10 }}>{used}/{cap} GB</span>
      <span className={`badge ${online ? 'badge-ok' : 'badge-danger'}`}>{online ? '在线' : '离线'}</span>
    </div>
  )
}

// 迷你读写速率趋势线（2s 采样，缓冲 30 点 ≈ 1 分钟）
function Sparkline({ label, data }: { label: string; data?: number[] }) {
  const W = 64, H = 18
  if (!data || data.length < 2) {
    return <div style={{ width: W, height: H, display: 'flex', alignItems: 'center', gap: 4 }}><span className="faint" style={{ fontSize: 10 }}>{label}</span><span style={{ color: 'var(--text-faint)', fontSize: 10 }}>—</span></div>
  }
  const max = Math.max(...data, 1)
  const pts = data.map((v, i) => `${(i / (data.length - 1)) * W},${H - (v / max) * (H - 2) - 1}`).join(' ')
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
      <span className="faint" style={{ fontSize: 10 }}>{label}</span>
      <svg width={W} height={H}>
        <polyline points={pts} fill="none" stroke="var(--accent)" strokeWidth="1.5" />
      </svg>
    </div>
  )
}
