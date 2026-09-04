import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { triggerRepair, getRepairQueue, triggerGC, triggerRebalance, RepairQueue } from '../../api/client'

export default function Integrity() {
  const { clusterId } = useParams()
  const [queue, setQueue] = useState<RepairQueue | null>(null)
  const [chunkId, setChunkId] = useState('')
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!clusterId) return
    getRepairQueue(clusterId).then(setQueue).catch(console.error)
  }, [clusterId])

  const handleRepair = async () => {
    if (!clusterId) return
    setLoading(true)
    try {
      await triggerRepair(clusterId)
      const updated = await getRepairQueue(clusterId)
      setQueue(updated)
    } catch (err) {
      console.error(err)
    } finally {
      setLoading(false)
    }
  }

  const handleGC = async () => {
    if (!clusterId) return
    setLoading(true)
    try {
      await triggerGC(clusterId)
    } catch (err) {
      console.error(err)
    } finally {
      setLoading(false)
    }
  }

  const handleRebalance = async () => {
    if (!clusterId) return
    setLoading(true)
    try {
      await triggerRebalance(clusterId)
    } catch (err) {
      console.error(err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Integrity</div>
          <h1>数据完整性</h1>
        </div>
        <button className="btn" onClick={() => getRepairQueue(clusterId!).then(setQueue)}>刷新</button>
      </div>

      {/* 修复队列 KPI */}
      {queue && (
        <div className="stat-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', marginBottom: 18 }}>
          <div className="stat"><div className="label">待修复</div><div className="value" style={{ fontSize: 20, color: queue.pending > 0 ? 'var(--warn)' : 'var(--ok)' }}>{queue.pending}</div></div>
          <div className="stat"><div className="label">进行中</div><div className="value" style={{ fontSize: 20, color: 'var(--accent)' }}>{queue.inProgress}</div></div>
          <div className="stat"><div className="label">已完成</div><div className="value" style={{ fontSize: 20 }}>{queue.completed}</div></div>
          <div className="stat"><div className="label">队列总量</div><div className="value" style={{ fontSize: 20 }}>{queue.pending + queue.inProgress + queue.completed}</div></div>
        </div>
      )}

      {/* 操作卡 */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 14, marginBottom: 18 }}>
        <div className="panel panel-pad">
          <h3 style={{ marginBottom: 8 }}>修复</h3>
          <p className="muted" style={{ fontSize: 13, marginBottom: 12 }}>重新补齐副本不足的 chunk（读修复）</p>
          <button className="btn btn-primary" onClick={handleRepair} disabled={loading}>{loading ? '触发中...' : '触发修复'}</button>
        </div>
        <div className="panel panel-pad">
          <h3 style={{ marginBottom: 8 }}>GC 扫描</h3>
          <p className="muted" style={{ fontSize: 13, marginBottom: 12 }}>扫描孤儿块并回收磁盘空间</p>
          <button className="btn btn-primary" onClick={handleGC} disabled={loading}>{loading ? '触发中...' : '触发 GC'}</button>
        </div>
        <div className="panel panel-pad">
          <h3 style={{ marginBottom: 8 }}>再平衡</h3>
          <p className="muted" style={{ fontSize: 13, marginBottom: 12 }}>重新分布数据以均衡节点负载</p>
          <button className="btn btn-primary" onClick={handleRebalance} disabled={loading}>{loading ? '触发中...' : '触发再平衡'}</button>
        </div>
      </div>

      {/* Chunk 查询 */}
      <div className="panel panel-pad">
        <h3 style={{ marginBottom: 12 }}>Chunk 查询</h3>
        <div style={{ display: 'flex', gap: 10 }}>
          <input value={chunkId} onChange={(e) => setChunkId(e.target.value)} placeholder="输入 Chunk ID" style={{ width: 300 }} />
          <button className="btn btn-primary" onClick={() => { if (chunkId.trim()) window.open(`/clusters/${clusterId}/chunks/${encodeURIComponent(chunkId.trim())}`, '_blank') }}>
            查询
          </button>
        </div>
      </div>
    </div>
  )
}