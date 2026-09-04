import { useState, useEffect } from 'react'
import { getGlobalOverview, AggregatedResult } from '../../api/client'

export default function GlobalOverview() {
  const [data, setData] = useState<AggregatedResult | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getGlobalOverview()
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div style={{ padding: '40px', color: 'var(--text-dim)', textAlign: 'center' }}>加载中...</div>
  if (!data) return <div style={{ padding: '40px', color: 'var(--danger)', textAlign: 'center' }}>加载失败</div>

  const names = Object.keys(data.results)

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Fleet</div>
          <h1>多集群总览</h1>
        </div>
      </div>

      {data.failures && Object.keys(data.failures).length > 0 && (
        <div className="panel panel-pad" style={{ borderColor: 'rgba(248,113,113,.4)', marginBottom: 18 }}>
          <span className="badge badge-danger">部分集群不可达</span>
          <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
            {Object.entries(data.failures).map(([name, err]) => (
              <div key={name} style={{ marginTop: 4 }}>{name}: <span className="mono">{err}</span></div>
            ))}
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 16 }}>
        {names.map(name => {
          const o = data.results[name]
          return (
            <div key={name} className="panel panel-pad">
              <h3 style={{ marginBottom: 14 }}>{name}</h3>
              <div className="stat-grid" style={{ gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 0 }}>
                <div>
                  <div className="faint" style={{ fontSize: 11 }}>节点</div>
                  <div className="mono" style={{ fontSize: 20, fontWeight: 700 }}>{o?.nodes || 0}</div>
                </div>
                <div>
                  <div className="faint" style={{ fontSize: 11 }}>容量</div>
                  <div className="mono" style={{ fontSize: 20, fontWeight: 700 }}>{o?.capacity || 0}<span style={{ fontSize: 12, color: 'var(--text-faint)' }}> GB</span></div>
                </div>
                <div>
                  <div className="faint" style={{ fontSize: 11 }}>Bucket</div>
                  <div className="mono" style={{ fontSize: 20, fontWeight: 700 }}>{o?.buckets || 0}</div>
                </div>
                <div>
                  <div className="faint" style={{ fontSize: 11 }}>修复队列</div>
                  <div className="mono" style={{ fontSize: 20, fontWeight: 700, color: (o?.repairQueue || 0) > 0 ? 'var(--warn)' : 'var(--ok)' }}>{o?.repairQueue || 0}</div>
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
