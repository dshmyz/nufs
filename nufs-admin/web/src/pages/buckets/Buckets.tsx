import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { getBuckets, createBucket, deleteBucket, BucketInfo } from '../../api/client'

export default function Buckets() {
  const { clusterId } = useParams()
  const [buckets, setBuckets] = useState<BucketInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [showCreate, setShowCreate] = useState(false)
  const [newBucketName, setNewBucketName] = useState('')

  useEffect(() => {
    if (!clusterId) return
    getBuckets(clusterId)
      .then(setBuckets)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [clusterId])

  const handleCreate = async () => {
    if (!clusterId || !newBucketName) return
    try {
      await createBucket(clusterId, {
        name: newBucketName,
        policy: { replicationFactor: 3, storageTier: 'hot' }
      })
      setNewBucketName('')
      setShowCreate(false)
      const updated = await getBuckets(clusterId)
      setBuckets(updated)
    } catch (err) {
      console.error(err)
    }
  }

  const handleDelete = async (name: string) => {
    if (!clusterId) return
    try {
      await deleteBucket(clusterId, name)
      setBuckets(buckets.filter(b => b.name !== name))
    } catch (err) {
      console.error(err)
    }
  }

  if (loading) return <div>加载中...</div>

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '24px' }}>
        <h1 style={{ fontSize: '24px', fontWeight: 700 }}>Bucket 列表</h1>
        <button
          onClick={() => setShowCreate(true)}
          style={{
            padding: '8px 16px',
            background: '#2563eb',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer',
          }}
        >
          创建 Bucket
        </button>
      </div>

      {showCreate && (
        <div style={{
          background: '#fff',
          border: '1px solid #e2e6ec',
          borderRadius: '10px',
          padding: '20px',
          marginBottom: '24px',
        }}>
          <input
            value={newBucketName}
            onChange={(e) => setNewBucketName(e.target.value)}
            placeholder="Bucket 名称"
            style={{
              padding: '10px',
              border: '1px solid #e2e6ec',
              borderRadius: '6px',
              marginRight: '12px',
            }}
          />
          <button
            onClick={handleCreate}
            style={{
              padding: '10px 16px',
              background: '#16a34a',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
            }}
          >
            确认创建
          </button>
          <button
            onClick={() => setShowCreate(false)}
            style={{
              padding: '10px 16px',
              background: '#f5f6f8',
              border: '1px solid #e2e6ec',
              borderRadius: '6px',
              marginLeft: '8px',
              cursor: 'pointer',
            }}
          >
            取消
          </button>
        </div>
      )}

      <table style={{
        width: '100%',
        background: '#fff',
        border: '1px solid #e2e6ec',
        borderRadius: '10px',
        borderCollapse: 'collapse',
      }}>
        <thead>
          <tr style={{ background: '#f5f6f8' }}>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>名称</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>策略</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>大小</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>对象数</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作</th>
          </tr>
        </thead>
        <tbody>
          {buckets.map(bucket => (
            <tr key={bucket.name}>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{bucket.name}</td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                {bucket.policy.replicationFactor}副本 / {bucket.policy.storageTier}
              </td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{bucket.usage.size} GB</td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{bucket.usage.objects}</td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                <button
                  onClick={() => handleDelete(bucket.name)}
                  style={{
                    padding: '4px 12px',
                    background: '#fee2e2',
                    color: '#dc2626',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                  }}
                >
                  删除
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}