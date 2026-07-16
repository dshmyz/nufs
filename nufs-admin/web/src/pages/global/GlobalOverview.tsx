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

  if (loading) {
    return <div style={{ textAlign: 'center', padding: '40px' }}>加载中...</div>
  }

  if (!data) {
    return <div style={{ textAlign: 'center', padding: '40px', color: '#dc2626' }}>加载失败</div>
  }

  const clusterNames = Object.keys(data.results)

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>多集群总览</h1>

      {data.failures && Object.keys(data.failures).length > 0 && (
        <div style={{
          padding: '12px',
          background: '#fee2e2',
          color: '#dc2626',
          borderRadius: '8px',
          marginBottom: '24px',
        }}>
          <strong>部分集群不可达：</strong>
          {Object.entries(data.failures).map(([name, err]) => (
            <span key={name} style={{ marginLeft: '8px' }}>{name}: {err}</span>
          ))}
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '16px' }}>
        {clusterNames.map(name => {
          const overview = data.results[name]
          return (
            <div key={name} style={{
              background: '#fff',
              border: '1px solid #e2e6ec',
              borderRadius: '10px',
              padding: '20px',
            }}>
              <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '12px' }}>{name}</h3>
              <div style={{ fontSize: '13px', color: '#5a6478' }}>
                <div>节点: {overview?.nodes || 0}</div>
                <div>容量: {overview?.capacity || 0} GB</div>
                <div>Bucket: {overview?.buckets || 0}</div>
                <div>修复队列: {overview?.repairQueue || 0}</div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}