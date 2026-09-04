import { Fragment, useCallback, useEffect, useRef, useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  BucketInfo,
  BucketQuotaStatus,
  createBucket,
  deleteBucket,
  deleteBucketQuota,
  getAPIErrorMessage,
  getBucketQuota,
  getBuckets,
  setBucketQuota,
} from '../../api/client'
import './Buckets.css'

interface QuotaRowState {
  status?: BucketQuotaStatus
  loading: boolean
  error?: string
}

interface QuotaDraft {
  maxBytes: string
  maxObjects: string
}

type QuotaSeverity = 'neutral' | 'healthy' | 'warning' | 'critical'

const numberFormatter = new Intl.NumberFormat('zh-CN')

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '0 B'

  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
  const unitIndex = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1)
  const scaled = value / 1024 ** unitIndex
  const precision = scaled >= 100 || unitIndex === 0 ? 0 : scaled >= 10 ? 1 : 2
  return `${scaled.toFixed(precision)} ${units[unitIndex]}`
}

function quotaRatio(used: number, limit: number): number | null {
  return limit > 0 ? used / limit : null
}

function quotaSeverity(ratio: number | null): QuotaSeverity {
  if (ratio === null) return 'neutral'
  if (ratio >= 0.95) return 'critical'
  if (ratio >= 0.8) return 'warning'
  return 'healthy'
}

function formatPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(ratio < 0.1 ? 1 : 0)}%`
}

function validateQuotaValue(value: string, label: string): number {
  if (!/^(0|[1-9]\d*)$/.test(value)) {
    throw new Error(`${label}必须是非负整数`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${label}不能超过 ${numberFormatter.format(Number.MAX_SAFE_INTEGER)}`)
  }
  return parsed
}

function QuotaRail({
  label,
  used,
  limit,
  formatValue,
}: {
  label: string
  used: number
  limit: number
  formatValue: (value: number) => string
}) {
  const ratio = quotaRatio(used, limit)
  const severity = quotaSeverity(ratio)
  const width = ratio === null ? 0 : Math.min(Math.max(ratio * 100, 0), 100)

  return (
    <div className={`quota-rail quota-rail--${severity}`}>
      <div className="quota-rail__text">
        <span>{label}</span>
        <span>
          {ratio === null
            ? `${formatValue(used)} / 不限额`
            : `${formatValue(used)} / ${formatValue(limit)} · ${formatPercent(ratio)}`}
        </span>
      </div>
      {ratio !== null && (
        <div
          className="quota-rail__track"
          role="progressbar"
          aria-label={`${label}配额使用率`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.round(Math.min(Math.max(ratio * 100, 0), 100))}
          aria-valuetext={formatPercent(ratio)}
        >
          <span className="quota-rail__fill" style={{ width: `${width}%` }} />
        </div>
      )}
    </div>
  )
}

export default function Buckets() {
  const { clusterId } = useParams()
  const [buckets, setBuckets] = useState<BucketInfo[]>([])
  const [quotas, setQuotas] = useState<Record<string, QuotaRowState>>({})
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState('')
  const [actionError, setActionError] = useState('')
  const [showCreate, setShowCreate] = useState(false)
  const [newBucketName, setNewBucketName] = useState('')
  const [editingBucket, setEditingBucket] = useState<string | null>(null)
  const [quotaDraft, setQuotaDraft] = useState<QuotaDraft>({ maxBytes: '0', maxObjects: '0' })
  const [editError, setEditError] = useState('')
  const [busyAction, setBusyAction] = useState('')
  const [dataCluster, setDataCluster] = useState<string>()
  const pageRequest = useRef(0)
  const quotaRequests = useRef<Record<string, number>>({})
  const mutationRequest = useRef(0)
  const activeCluster = useRef(clusterId)
  activeCluster.current = clusterId
  const visibleBuckets = dataCluster === clusterId ? buckets : []
  const mutationBusy = busyAction !== ''

  const loadPage = useCallback(async () => {
    if (!clusterId) {
      setLoading(false)
      setLoadError('缺少集群标识')
      return
    }
    if (activeCluster.current !== clusterId) return

    const requestId = ++pageRequest.current
    setLoading(true)
    setLoadError('')
    setActionError('')

    try {
      const nextBuckets = await getBuckets(clusterId)
      if (activeCluster.current !== clusterId || pageRequest.current !== requestId) return

      setDataCluster(clusterId)
      setBuckets(nextBuckets)
      setQuotas(Object.fromEntries(nextBuckets.map((bucket) => [bucket.name, { loading: true }])))

      const results = await Promise.allSettled(
        nextBuckets.map(async (bucket) => ({
          name: bucket.name,
          status: await getBucketQuota(clusterId, bucket.name),
        })),
      )
      if (activeCluster.current !== clusterId || pageRequest.current !== requestId) return

      const nextQuotas: Record<string, QuotaRowState> = {}
      results.forEach((result, index) => {
        const name = nextBuckets[index].name
        nextQuotas[name] =
          result.status === 'fulfilled'
            ? { status: result.value.status, loading: false }
            : { loading: false, error: getAPIErrorMessage(result.reason, '配额加载失败') }
      })
      setQuotas(nextQuotas)
    } catch (error) {
      if (activeCluster.current !== clusterId || pageRequest.current !== requestId) return
      setBuckets([])
      setQuotas({})
      setDataCluster(clusterId)
      setLoadError(getAPIErrorMessage(error, 'Bucket 列表加载失败'))
    } finally {
      if (activeCluster.current === clusterId && pageRequest.current === requestId) setLoading(false)
    }
  }, [clusterId])

  useEffect(() => {
    mutationRequest.current += 1
    setBusyAction('')
    setEditingBucket(null)
    setEditError('')
    setShowCreate(false)
    setNewBucketName('')
    void loadPage()
    return () => {
      pageRequest.current += 1
    }
  }, [loadPage])

  const refreshQuota = async (bucketName: string) => {
    if (!clusterId) return
    const pageRequestId = pageRequest.current
    const requestId = (quotaRequests.current[bucketName] ?? 0) + 1
    quotaRequests.current[bucketName] = requestId
    setQuotas((current) => ({
      ...current,
      [bucketName]: { ...current[bucketName], loading: true, error: undefined },
    }))

    try {
      const status = await getBucketQuota(clusterId, bucketName)
      if (
        activeCluster.current !== clusterId ||
        pageRequest.current !== pageRequestId ||
        quotaRequests.current[bucketName] !== requestId
      ) return
      setQuotas((current) => ({ ...current, [bucketName]: { status, loading: false } }))
    } catch (error) {
      if (
        activeCluster.current !== clusterId ||
        pageRequest.current !== pageRequestId ||
        quotaRequests.current[bucketName] !== requestId
      ) return
      setQuotas((current) => ({
        ...current,
        [bucketName]: {
          ...current[bucketName],
          loading: false,
          error: getAPIErrorMessage(error, '配额加载失败'),
        },
      }))
    }
  }

  const handleCreate = async () => {
    if (!clusterId || !newBucketName.trim()) return
    const requestId = ++mutationRequest.current
    setBusyAction('create')
    setActionError('')
    try {
      await createBucket(clusterId, {
        name: newBucketName.trim(),
        policy: { replicationFactor: 3, storageTier: 'hot' },
      })
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      setNewBucketName('')
      setShowCreate(false)
      await loadPage()
    } catch (error) {
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      setActionError(getAPIErrorMessage(error, 'Bucket 创建失败'))
    } finally {
      if (activeCluster.current === clusterId && mutationRequest.current === requestId) setBusyAction('')
    }
  }

  const handleDelete = async (name: string) => {
    if (
      !clusterId ||
      dataCluster !== clusterId ||
      mutationBusy ||
      !window.confirm(`确认删除 Bucket“${name}”？此操作不可恢复。`)
    ) return
    const requestId = ++mutationRequest.current
    setBusyAction(`delete:${name}`)
    setActionError('')
    try {
      await deleteBucket(clusterId, name)
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      await loadPage()
    } catch (error) {
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      setActionError(getAPIErrorMessage(error, 'Bucket 删除失败'))
    } finally {
      if (activeCluster.current === clusterId && mutationRequest.current === requestId) setBusyAction('')
    }
  }

  const openQuotaEditor = (name: string) => {
    if (dataCluster !== clusterId || mutationBusy) return
    const quota = quotas[name]?.status?.quota
    setEditingBucket(name)
    setQuotaDraft({
      maxBytes: String(quota?.max_bytes ?? 0),
      maxObjects: String(quota?.max_objects ?? 0),
    })
    setEditError('')
  }

  const handleSaveQuota = async (name: string) => {
    if (!clusterId || dataCluster !== clusterId || mutationBusy) return
    setEditError('')

    let maxBytes: number
    let maxObjects: number
    try {
      maxBytes = validateQuotaValue(quotaDraft.maxBytes, '容量配额')
      maxObjects = validateQuotaValue(quotaDraft.maxObjects, '对象配额')
    } catch (error) {
      setEditError(error instanceof Error ? error.message : '配额输入无效')
      return
    }

    const requestId = ++mutationRequest.current
    setBusyAction(`quota:${name}`)
    try {
      await setBucketQuota(clusterId, name, { max_bytes: maxBytes, max_objects: maxObjects })
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      setEditingBucket(null)
      await refreshQuota(name)
    } catch (error) {
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      setEditError(getAPIErrorMessage(error, '配额保存失败'))
    } finally {
      if (activeCluster.current === clusterId && mutationRequest.current === requestId) setBusyAction('')
    }
  }

  const handleClearQuota = async (name: string) => {
    if (
      !clusterId ||
      dataCluster !== clusterId ||
      mutationBusy ||
      !window.confirm(`确认清除 Bucket“${name}”的配额限制？`)
    ) return
    const requestId = ++mutationRequest.current
    setBusyAction(`quota:${name}`)
    setEditError('')
    setActionError('')
    try {
      await deleteBucketQuota(clusterId, name)
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      setEditingBucket(null)
      await refreshQuota(name)
    } catch (error) {
      if (activeCluster.current !== clusterId || mutationRequest.current !== requestId) return
      const message = getAPIErrorMessage(error, '配额清除失败')
      setEditError(message)
    } finally {
      if (activeCluster.current === clusterId && mutationRequest.current === requestId) setBusyAction('')
    }
  }

  // 聚合 KPI：总用量 / 对象 / 配额告警数
  let totalUsedBytes = 0, totalObjects = 0, quotaAlert = 0
  for (const b of visibleBuckets) {
    const st = quotas[b.name]?.status
    if (!st) continue
    if (st.usage) { totalUsedBytes += st.usage.used_bytes; totalObjects += st.usage.objects }
    const q = st.quota
    const ratio = q && st.usage
      ? Math.max(
          q.max_bytes > 0 ? st.usage.used_bytes / q.max_bytes : 0,
          q.max_objects > 0 ? st.usage.objects / q.max_objects : 0,
        )
      : null
    if (ratio !== null && ratio >= 0.8) quotaAlert++
  }

  return (
    <section className="buckets-page" aria-labelledby="buckets-title">
      <div className="buckets-page__header">
        <div>
          <h1 id="buckets-title">Bucket</h1>
          <p>{visibleBuckets.length > 0 ? `${numberFormatter.format(visibleBuckets.length)} 个 Bucket` : '存储桶与配额管理'}</p>
        </div>
        <button className="button button--primary" type="button" onClick={() => setShowCreate(true)} disabled={mutationBusy}>
          创建 Bucket
        </button>
      </div>

      {/* 聚合 KPI */}
      {visibleBuckets.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 12, marginBottom: 14 }}>
          <div className="stat"><div className="label">Bucket 总数</div><div className="value" style={{ fontSize: 20 }}>{visibleBuckets.length}</div></div>
          <div className="stat"><div className="label">总已用</div><div className="value" style={{ fontSize: 20, color: 'var(--accent)' }}>{formatBytes(totalUsedBytes)}</div></div>
          <div className="stat"><div className="label">对象总数</div><div className="value" style={{ fontSize: 20 }}>{numberFormatter.format(totalObjects)}</div></div>
          <div className="stat"><div className="label">配额告警</div><div className="value" style={{ fontSize: 20, color: quotaAlert > 0 ? 'var(--warn)' : 'var(--ok)' }}>{quotaAlert}</div></div>
        </div>
      )}

      {showCreate && (
        <form
          className="bucket-create"
          onSubmit={(event) => {
            event.preventDefault()
            void handleCreate()
          }}
        >
          <label htmlFor="new-bucket-name">Bucket 名称</label>
          <input
            id="new-bucket-name"
            value={newBucketName}
            onChange={(event) => setNewBucketName(event.target.value)}
            autoFocus
          />
          <div className="bucket-create__actions">
            <button
              className="button button--primary"
              type="submit"
              disabled={!newBucketName.trim() || mutationBusy}
            >
              {busyAction === 'create' ? '创建中…' : '创建'}
            </button>
            <button className="button" type="button" onClick={() => setShowCreate(false)} disabled={mutationBusy}>
              取消
            </button>
          </div>
        </form>
      )}

      {actionError && (
        <div className="page-message page-message--error" role="alert">
          <span>{actionError}</span>
          <button type="button" onClick={() => setActionError('')}>关闭</button>
        </div>
      )}

      {loadError ? (
        <div className="page-state page-state--error" role="alert">
          <strong>无法加载 Bucket</strong>
          <span>{loadError}</span>
          <button className="button" type="button" onClick={() => void loadPage()}>重试</button>
        </div>
      ) : (
        <div className="bucket-table-wrap" aria-busy={loading}>
          <table className="bucket-table">
            <thead>
              <tr>
                <th scope="col">名称</th>
                <th scope="col">策略</th>
                <th scope="col">当前用量</th>
                <th scope="col">容量配额</th>
                <th scope="col">对象配额</th>
                <th scope="col">配额状态</th>
                <th scope="col">操作</th>
              </tr>
            </thead>
            <tbody>
              {loading && visibleBuckets.length === 0 && (
                <tr>
                  <td className="bucket-table__empty" colSpan={7}>正在加载…</td>
                </tr>
              )}
              {!loading && visibleBuckets.length === 0 && (
                <tr>
                  <td className="bucket-table__empty" colSpan={7}>暂无 Bucket</td>
                </tr>
              )}
              {visibleBuckets.map((bucket) => {
                const quotaState = quotas[bucket.name]
                const status = quotaState?.status
                const quota = status?.quota
                const usage = status?.usage
                const quotaUnavailable = !status || quotaState?.loading || Boolean(quotaState?.error)
                const bytesRatio = usage && quota ? quotaRatio(usage.used_bytes, quota.max_bytes) : null
                const objectsRatio = usage && quota ? quotaRatio(usage.objects, quota.max_objects) : null
                const combinedRatio =
                  bytesRatio === null && objectsRatio === null
                    ? null
                    : Math.max(bytesRatio ?? 0, objectsRatio ?? 0)
                const severity = quotaSeverity(combinedRatio)
                const quotaBusy = busyAction === `quota:${bucket.name}`
                const editorId = `quota-editor-${encodeURIComponent(bucket.name)}`

                return (
                  <Fragment key={bucket.name}>
                    <tr>
                      <td className="bucket-table__name">{bucket.name}</td>
                      <td>{bucket.policy.replicationFactor} 副本 / {bucket.policy.storageTier}</td>
                      <td>
                        <span className="metric-value">{usage ? formatBytes(usage.used_bytes) : '—'}</span>
                        <span className="metric-subvalue">{usage ? `${numberFormatter.format(usage.objects)} 个对象` : '—'}</span>
                      </td>
                      <td>
                        {quotaUnavailable ? '—' : quota && quota.max_bytes > 0 ? formatBytes(quota.max_bytes) : '不限额'}
                      </td>
                      <td>
                        {quotaUnavailable ? '—' : quota && quota.max_objects > 0 ? numberFormatter.format(quota.max_objects) : '不限额'}
                      </td>
                      <td className="quota-status-cell">
                        {quotaState?.loading ? (
                          <span className="quota-loading">配额加载中…</span>
                        ) : quotaState?.error ? (
                          <div className="quota-error" role="alert">
                            <span>{quotaState.error}</span>
                            <button type="button" onClick={() => void refreshQuota(bucket.name)}>重试</button>
                          </div>
                        ) : status && usage ? (
                          <>
                            <span className={`quota-summary quota-summary--${severity}`}>
                              {severity === 'critical'
                                ? '严重'
                                : severity === 'warning'
                                  ? '警告'
                                  : severity === 'healthy'
                                    ? '正常'
                                    : '不限额'}
                            </span>
                            <QuotaRail label="容量" used={usage.used_bytes} limit={quota?.max_bytes ?? 0} formatValue={formatBytes} />
                            <QuotaRail
                              label="对象"
                              used={usage.objects}
                              limit={quota?.max_objects ?? 0}
                              formatValue={(value) => numberFormatter.format(value)}
                            />
                          </>
                        ) : (
                          <span>—</span>
                        )}
                      </td>
                      <td>
                        <div className="row-actions">
                          <button
                            className="text-button"
                            type="button"
                            onClick={() => openQuotaEditor(bucket.name)}
                            aria-label={`编辑 ${bucket.name} 的配额`}
                            aria-expanded={editingBucket === bucket.name}
                            aria-controls={editorId}
                            disabled={quotaUnavailable || mutationBusy}
                          >
                            配额
                          </button>
                          <button
                            className="text-button text-button--danger"
                            type="button"
                            onClick={() => void handleDelete(bucket.name)}
                            aria-label={`删除 Bucket ${bucket.name}`}
                            disabled={mutationBusy}
                          >
                            {busyAction === `delete:${bucket.name}` ? '删除中…' : '删除'}
                          </button>
                        </div>
                      </td>
                    </tr>
                    {editingBucket === bucket.name && (
                      <tr className="quota-editor-row" id={editorId}>
                        <td colSpan={7}>
                          <form
                            className="quota-editor"
                            noValidate
                            onSubmit={(event) => {
                              event.preventDefault()
                              void handleSaveQuota(bucket.name)
                            }}
                          >
                            <div className="quota-editor__heading">
                              <strong>编辑 {bucket.name} 配额</strong>
                              <span>设置为 0 表示不限额</span>
                            </div>
                            <label htmlFor={`quota-bytes-${bucket.name}`}>
                              容量上限（字节）
                              <input
                                id={`quota-bytes-${bucket.name}`}
                                type="number"
                                min="0"
                                step="1"
                                inputMode="numeric"
                                value={quotaDraft.maxBytes}
                                onChange={(event) => setQuotaDraft((current) => ({ ...current, maxBytes: event.target.value }))}
                                disabled={mutationBusy}
                              />
                            </label>
                            <label htmlFor={`quota-objects-${bucket.name}`}>
                              对象上限
                              <input
                                id={`quota-objects-${bucket.name}`}
                                type="number"
                                min="0"
                                step="1"
                                inputMode="numeric"
                                value={quotaDraft.maxObjects}
                                onChange={(event) => setQuotaDraft((current) => ({ ...current, maxObjects: event.target.value }))}
                                disabled={mutationBusy}
                              />
                            </label>
                            <div className="quota-editor__actions">
                              <button className="button button--primary" type="submit" disabled={mutationBusy}>
                                {quotaBusy ? '保存中…' : '保存'}
                              </button>
                              <button className="button" type="button" onClick={() => setEditingBucket(null)} disabled={mutationBusy}>
                                取消
                              </button>
                              <button
                                className="button button--danger"
                                type="button"
                                onClick={() => void handleClearQuota(bucket.name)}
                                disabled={mutationBusy}
                              >
                                清除配额
                              </button>
                            </div>
                            {editError && <p className="quota-editor__error" role="alert">{editError}</p>}
                          </form>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
