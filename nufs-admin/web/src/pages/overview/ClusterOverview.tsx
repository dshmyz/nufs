import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { getClusterOverview } from '../../api/client'

export default function ClusterOverview() {
  const { clusterId } = useParams()
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!clusterId) return
    getClusterOverview(clusterId)
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [clusterId])

  if (loading) return <div>加载中...</div>
  if (!data) return <div>加载失败</div>

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>
        {clusterId} 概览
      </h1>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '16px', marginBottom: '32px' }}>
        {[
          { label: '在线节点', value: data.nodes || 0, color: '#16a34a' },
          { label: '容量使用', value: `${data.capacityUsed || 0} / ${data.capacityTotal || 0} GB`, color: '#2563eb' },
          { label: 'Bucket 数', value: data.buckets || 0, color: '#7c3aed' },
          { label: '修复队列', value: data.repairQueue || 0, color: '#d97706' },
        ].map(card => (
          <div key={card.label} style={{
            background: '#fff',
            border: '1px solid #e2e6ec',
            borderRadius: '10px',
            padding: '20px',
          }}>
            <div style={{ fontSize: '12px', color: '#9ca3af', marginBottom: '8px' }}>{card.label}</div>
            <div style={{ fontSize: '24px', fontWeight: 700, color: card.color }}>{card.value}</div>
          </div>
        ))}
      </div>

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>运维功能</h3>
        <ul style={{ fontSize: '13px', color: '#5a6478', lineHeight: 1.8 }}>
          <li>节点管理：查看节点列表、下线节点</li>
          <li>Bucket 管理：创建/删除 Bucket、设置策略</li>
          <li>数据完整性：触发修复、查看修复队列、GC 扫描</li>
          <li>集群治理：Raft 状态、审计日志、配置管理</li>
        </ul>
      </div>
    </div>
  )
}