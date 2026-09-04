import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { getGlobalOverview, listClusters, AggregatedResult, ClusterView } from '../../api/client'

export default function GlobalOverview() {
  const [data, setData] = useState<AggregatedResult | null>(null)
  const [clusters, setClusters] = useState<ClusterView[]>([])
  const [loading, setLoading] = useState(true)

  const load = () => {
    Promise.all([getGlobalOverview().catch(() => null), listClusters().catch(() => [])])
      .then(([d, c]) => { setData(d); setClusters(c) })
      .catch(console.error)
      .finally(() => setLoading(false))
  }
  useEffect(() => { load() }, [])

  if (loading) return <div style={{ padding: '40px', color: 'var(--text-dim)', textAlign: 'center' }}>加载中...</div>
  if (!data) return <div style={{ padding: '40px', color: 'var(--danger)', textAlign: 'center' }}>加载失败</div>

  const results = data.results || {}
  const failures = data.failures || {}
  const names = Object.keys(results)

  // 聚合 KPI
  const agg = {
    nodes: names.reduce((s, n) => s + (results[n]?.nodes || 0), 0),
    capacity: names.reduce((s, n) => s + (results[n]?.capacity || 0), 0),
    used: names.reduce((s, n) => s + (results[n]?.capacityUsed || 0), 0),
    buckets: names.reduce((s, n) => s + (results[n]?.buckets || 0), 0),
    repair: names.reduce((s, n) => s + (results[n]?.repairQueue || 0), 0),
  }
  // 集群健康：failures 里的不可达，其余按 listClusters 的 health
  const healthOf = (name: string): 'ok' | 'danger' | 'idle' => {
    if (failures[name]) return 'danger'
    const c = clusters.find(x => x.name === name)
    return c?.health === 'healthy' ? 'ok' : c?.health === 'unhealthy' ? 'danger' : 'idle'
  }
  const healthLabel = (h: string) => (h === 'ok' ? '健康' : h === 'danger' ? '不可达' : '未知')

  const totalUsedPct = agg.capacity > 0 ? (agg.used / agg.capacity) * 100 : 0
  const usedPct = (n: string) => {
    const c = results[n]?.capacity || 0
    return c > 0 ? ((results[n]?.capacityUsed || 0) / c) * 100 : 0
  }

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Fleet</div>
          <h1>多集群总览</h1>
        </div>
        <button className="btn" onClick={load}>刷新</button>
      </div>

      {/* 聚合 KPI 条 */}
      <div className="stat-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', marginBottom: 18 }}>
        <div className="stat"><div className="label">集群</div><div className="value" style={{ fontSize: 20 }}>{names.length + Object.keys(failures).length}</div></div>
        <div className="stat"><div className="label">在线节点</div><div className="value" style={{ fontSize: 20, color: 'var(--ok)' }}>{agg.nodes}</div></div>
        <div className="stat"><div className="label">总容量</div><div className="value" style={{ fontSize: 20, color: 'var(--accent)' }}>{agg.capacity} <span style={{ fontSize: 12, color: 'var(--text-faint)' }}>GB</span></div></div>
        <div className="stat"><div className="label">已用容量</div><div className="value" style={{ fontSize: 20 }}>{agg.used} GB</div></div>
        <div className="stat"><div className="label">整体使用率</div><div className="value" style={{ fontSize: 20, color: totalUsedPct > 85 ? 'var(--danger)' : totalUsedPct > 70 ? 'var(--warn)' : 'var(--ok)' }}>{totalUsedPct.toFixed(1)}%</div></div>
        <div className="stat"><div className="label">Bucket 总数</div><div className="value" style={{ fontSize: 20 }}>{agg.buckets}</div></div>
        <div className="stat"><div className="label">修复队列</div><div className="value" style={{ fontSize: 20, color: agg.repair > 0 ? 'var(--warn)' : 'var(--ok)' }}>{agg.repair}</div></div>
      </div>

      {/* 集群级容量光谱 */}
      {names.length > 0 && (
        <div className="panel panel-pad" style={{ marginBottom: 18 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
            <h3>集群容量分布</h3>
            <span className="muted" style={{ fontSize: 12 }}>{agg.used} / {agg.capacity} GB</span>
          </div>
          <div className="spectrum">
            {names.map(n => {
              const pct = usedPct(n)
              const cls = pct > 85 ? 'crit' : pct > 70 ? 'hot' : ''
              const h = healthOf(n)
              return (
                <div className="spectrum-cell" key={n}>
                  <div className={`spectrum-fill ${cls}`} style={{ transform: `scaleX(${Math.min(100, pct) / 100})`, opacity: h === 'danger' ? 0.5 : 1 }} />
                  <div className="spectrum-meta">
                    <span className="id">{n}</span>
                    <span className="pct">{pct.toFixed(1)}%</span>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* 不可达告警 */}
      {Object.keys(failures).length > 0 && (
        <div className="panel panel-pad" style={{ borderColor: 'rgba(248,113,113,.4)', marginBottom: 18 }}>
          <span className="badge badge-danger">部分集群不可达</span>
          <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
            {Object.entries(failures).map(([name, err]) => (
              <div key={name} style={{ marginTop: 4 }}>{name}: <span className="mono">{err}</span></div>
            ))}
          </div>
        </div>
      )}

      {/* 集群卡网格 */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 16 }}>
        {names.map(name => {
          const o = results[name]
          const pct = usedPct(name)
          const h = healthOf(name)
          return (
            <div key={name} className="panel panel-pad">
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
                <h3 style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span className={`led led-${h}`} />
                  {name}
                </h3>
                <Link to={`/clusters/${name}/overview`} className="btn btn-sm">概览 ↗</Link>
              </div>
              <span className={`badge ${h === 'ok' ? 'badge-ok' : h === 'danger' ? 'badge-danger' : 'badge-warn'}`}>{healthLabel(h)}</span>

              {/* 容量条 */}
              <div style={{ margin: '12px 0 4px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--text-faint)', marginBottom: 4 }}>
                  <span>容量使用</span>
                  <span className="mono">{o?.capacityUsed || 0} / {o?.capacity || 0} GB · {pct.toFixed(1)}%</span>
                </div>
                <div style={{ height: 6, background: 'var(--line)', borderRadius: 3, overflow: 'hidden' }}>
                  <div style={{ width: `${Math.min(100, pct)}%`, height: '100%', background: pct > 85 ? 'var(--danger)' : pct > 70 ? 'var(--warn)' : 'var(--accent)', boxShadow: '0 0 8px rgba(34,211,238,.3)' }} />
                </div>
              </div>

              <div className="divider" />
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                <div><div className="faint" style={{ fontSize: 11 }}>节点</div><div className="mono" style={{ fontSize: 18, fontWeight: 700 }}>{o?.nodes || 0}</div></div>
                <div><div className="faint" style={{ fontSize: 11 }}>Bucket</div><div className="mono" style={{ fontSize: 18, fontWeight: 700, color: 'var(--accent)' }}>{o?.buckets || 0}</div></div>
                <div><div className="faint" style={{ fontSize: 11 }}>修复队列</div><div className="mono" style={{ fontSize: 18, fontWeight: 700, color: (o?.repairQueue || 0) > 0 ? 'var(--warn)' : 'var(--ok)' }}>{o?.repairQueue || 0}</div></div>
                <div><div className="faint" style={{ fontSize: 11 }}>使用率</div><div className="mono" style={{ fontSize: 18, fontWeight: 700, color: pct > 85 ? 'var(--danger)' : pct > 70 ? 'var(--warn)' : 'var(--ok)' }}>{pct.toFixed(1)}%</div></div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
