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
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>数据完整性</h1>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px', marginBottom: '32px' }}>
        <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
          <h3 style={{ fontSize: '14px', fontWeight: 700, marginBottom: '12px' }}>修复队列</h3>
          {queue && (
            <div style={{ fontSize: '13px', color: '#5a6478' }}>
              <div>待修复: {queue.pending}</div>
              <div>进行中: {queue.inProgress}</div>
              <div>已完成: {queue.completed}</div>
            </div>
          )}
          <button
            onClick={handleRepair}
            disabled={loading}
            style={{
              marginTop: '12px',
              padding: '8px 16px',
              background: '#2563eb',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              cursor: loading ? 'wait' : 'pointer',
            }}
          >
            触发修复
          </button>
        </div>

        <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
          <h3 style={{ fontSize: '14px', fontWeight: 700, marginBottom: '12px' }}>GC 扫描</h3>
          <p style={{ fontSize: '13px', color: '#5a6478', marginBottom: '12px' }}>
            扫描孤儿块并回收磁盘空间
          </p>
          <button
            onClick={handleGC}
            disabled={loading}
            style={{
              padding: '8px 16px',
              background: '#7c3aed',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              cursor: loading ? 'wait' : 'pointer',
            }}
          >
            触发 GC
          </button>
        </div>

        <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
          <h3 style={{ fontSize: '14px', fontWeight: 700, marginBottom: '12px' }}>再平衡</h3>
          <p style={{ fontSize: '13px', color: '#5a6478', marginBottom: '12px' }}>
            重新分布数据以均衡节点负载
          </p>
          <button
            onClick={handleRebalance}
            disabled={loading}
            style={{
              padding: '8px 16px',
              background: '#d97706',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              cursor: loading ? 'wait' : 'pointer',
            }}
          >
            触发再平衡
          </button>
        </div>
      </div>

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
        <h3 style={{ fontSize: '14px', fontWeight: 700, marginBottom: '12px' }}>Chunk 查询</h3>
        <input
          value={chunkId}
          onChange={(e) => setChunkId(e.target.value)}
          placeholder="输入 Chunk ID"
          style={{
            padding: '10px',
            border: '1px solid #e2e6ec',
            borderRadius: '6px',
            marginRight: '12px',
            width: '300px',
          }}
        />
        <button
          style={{
            padding: '10px 16px',
            background: '#2563eb',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer',
          }}
        >
          查询
        </button>
      </div>
    </div>
  )
}