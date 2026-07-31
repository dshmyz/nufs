import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { getClusterOverview, getClusterReadiness, ClusterReadiness } from '../../api/client'

const STATUS_COLOR: Record<string, string> = {
  ready: '#16a34a',
  degraded: '#d97706',
  not_ready: '#dc2626',
}

const STATUS_LABEL: Record<string, string> = {
  ready: '就绪',
  degraded: '降级',
  not_ready: '未就绪',
}

export default function ClusterOverview() {
  const { clusterId } = useParams()
  const [data, setData] = useState<any>(null)
  const [readiness, setReadiness] = useState<ClusterReadiness | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!clusterId) return
    Promise.all([
      getClusterOverview(clusterId).catch(() => null),
      getClusterReadiness(clusterId).catch(() => null),
    ]).then(([overview, ready]) => {
      setData(overview)
      setReadiness(ready)
    }).finally(() => setLoading(false))
  }, [clusterId])

  if (loading) return <div style={{ padding: '24px', color: '#6b7280' }}>加载中...</div>
  if (!data) return <div style={{ padding: '24px', color: '#dc2626' }}>加载失败</div>

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>
        {clusterId} 概览
      </h1>

      {/* Readiness banner */}
      {readiness && (
        <div style={{
          background: readiness.status === 'ready' ? '#f0fdf4' : readiness.status === 'degraded' ? '#fffbeb' : '#fef2f2',
          border: `1px solid ${readiness.status === 'ready' ? '#bbf7d0' : readiness.status === 'degraded' ? '#fde68a' : '#fecaca'}`,
          borderRadius: '10px',
          padding: '16px 20px',
          marginBottom: '24px',
          display: 'flex',
          alignItems: 'center',
          gap: '12px',
        }}>
          <div style={{
            width: '12px',
            height: '12px',
            borderRadius: '50%',
            background: STATUS_COLOR[readiness.status] || '#6b7280',
            flexShrink: 0,
          }} />
          <div>
            <div style={{ fontWeight: 700, color: STATUS_COLOR[readiness.status] || '#374151' }}>
              集群状态：{STATUS_LABEL[readiness.status] || readiness.status}
            </div>
            <div style={{ fontSize: '13px', color: '#6b7280', marginTop: '4px' }}>
              可支撑 RF={readiness.can_write_rf} · 在线 {readiness.nodes_online}/{readiness.nodes_total} 节点
              {readiness.chunks_under_replicated > 0 && ` · ${readiness.chunks_under_replicated} 个 chunk 副本不足`}
              {!readiness.leader_stable && ' · ⚠️ Leader 不稳定'}
            </div>
          </div>
        </div>
      )}

      {/* Metric cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '16px', marginBottom: '32px' }}>
        {[
          { label: '在线节点', value: readiness ? `${readiness.nodes_online}/${readiness.nodes_total}` : (data.nodes || 0), color: readiness && readiness.nodes_online < readiness.nodes_total ? '#d97706' : '#16a34a' },
          { label: '容量使用', value: `${data.capacityUsed || 0} / ${data.capacityTotal || 0} GB`, color: '#2563eb' },
          { label: 'Bucket 数', value: data.buckets || 0, color: '#7c3aed' },
          { label: '修复队列', value: readiness?.repair_queue_depth ?? data.repairQueue ?? 0, color: (readiness?.repair_queue_depth ?? 0) > 0 ? '#d97706' : '#16a34a' },
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

      {/* Readiness checks detail */}
      {readiness && readiness.checks && Object.keys(readiness.checks).length > 0 && (
        <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px', marginBottom: '24px' }}>
          <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>健康检查明细</h3>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #e2e6ec' }}>
                <th style={{ textAlign: 'left', padding: '8px 12px', color: '#6b7280' }}>检查项</th>
                <th style={{ textAlign: 'left', padding: '8px 12px', color: '#6b7280' }}>状态</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(readiness.checks).map(([key, value]) => (
                <tr key={key} style={{ borderBottom: '1px solid #f3f4f6' }}>
                  <td style={{ padding: '8px 12px', fontWeight: 500 }}>{key}</td>
                  <td style={{
                    padding: '8px 12px',
                    color: value === 'ok' || value === 'normal' ? '#16a34a' : '#d97706',
                  }}>{value}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize: '11px', color: '#9ca3af', marginTop: '12px' }}>
            更新时间：{readiness.timestamp ? new Date(readiness.timestamp).toLocaleString() : '—'}
          </div>
        </div>
      )}

      {/* Ops guide */}
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
