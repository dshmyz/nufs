import axios from 'axios'

const baseURL = '/api/v1'

function encodePathSegment(segment: string): string {
  if (segment === '.') return '%252E'
  if (segment === '..') return '%252E%252E'
  return encodeURIComponent(segment)
}

function bucketResourcePath(clusterId: string, bucketName: string, suffix = ''): string {
  let segment = encodeURIComponent(bucketName)
  let marker = ''
  if (bucketName === '.') {
    segment = '%252E'
    marker = 'dot'
  } else if (bucketName === '..') {
    segment = '%252E%252E'
    marker = 'dotdot'
  }

  const path = `/clusters/${encodePathSegment(clusterId)}/buckets/${segment}${suffix}`
  return marker ? `${path}?bucket_path=${marker}` : path
}

export function getAPIErrorMessage(error: unknown, fallback: string): string {
  if (axios.isAxiosError(error)) {
    const data = error.response?.data
    if (typeof data === 'string' && data.trim()) {
      return data.trim()
    }
    if (data && typeof data === 'object') {
      const candidate = 'error' in data ? data.error : 'message' in data ? data.message : undefined
      if (typeof candidate === 'string' && candidate.trim()) {
        return candidate.trim()
      }
    }
    if (!error.response && error.message) {
      return error.message
    }
  }
  if (error instanceof Error && error.message) {
    return error.message
  }
  return fallback
}

export const api = axios.create({
  baseURL,
  timeout: 5000,
})

// Auto-attach JWT token from localStorage
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Handle 401 (token expired) → redirect to login
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token')
      window.location.reload()
    }
    return Promise.reject(error)
  }
)

// Auth
export async function login(username: string, password: string): Promise<string> {
  const resp = await api.post('/auth/login', { username, password })
  return resp.data.token
}

// Clusters
export async function listClusters(): Promise<ClusterView[]> {
  const resp = await api.get('/clusters')
  return resp.data
}

export async function getGlobalOverview(): Promise<AggregatedResult> {
  const resp = await api.get('/clusters/all/overview')
  return resp.data
}

export async function getClusterOverview(clusterId: string): Promise<any> {
  const resp = await api.get(`/clusters/${clusterId}`)
  return resp.data
}

export interface ClusterReadiness {
  cluster?: string
  status: 'ready' | 'degraded' | 'not_ready'
  can_write_rf: number
  nodes_online: number
  nodes_total: number
  leader_stable: boolean
  degradation_state: string
  chunks_total: number
  chunks_under_replicated: number
  repair_queue_depth: number
  checks: Record<string, string>
  timestamp: string
}

export async function getClusterReadiness(clusterId: string): Promise<ClusterReadiness> {
  const resp = await api.get(`/clusters/${clusterId}/readiness`)
  return resp.data
}

// Cluster management (dynamic add/remove via UI)
export async function addCluster(cluster: {
  id: string
  region: string
  metad_ops_url: string
  description: string
}): Promise<void> {
  await api.post('/admin/clusters', cluster)
}

export async function removeCluster(id: string): Promise<void> {
  await api.delete(`/admin/clusters/${id}`)
}

export async function getClusterAuditLogs(limit = 50): Promise<ClusterAuditLog[]> {
  const resp = await api.get('/admin/clusters/audit', { params: { limit } })
  return resp.data
}

export async function getWriteOpsStatus(clusterId: string): Promise<WriteOpsStatus> {
  const resp = await api.get(`/clusters/${clusterId}/write-ops/status`)
  return resp.data
}

// Nodes
export async function getNodes(clusterId: string): Promise<NodeInfo[]> {
  const resp = await api.get(`/clusters/${clusterId}/nodes`)
  return resp.data
}

export async function decommissionNode(clusterId: string, nodeId: string): Promise<void> {
  await api.post(`/clusters/${clusterId}/nodes/${nodeId}/decommission`)
}

// Buckets
export async function getBuckets(clusterId: string): Promise<BucketInfo[]> {
  const resp = await api.get(`/clusters/${encodePathSegment(clusterId)}/buckets`)
  return resp.data
}

export async function createBucket(clusterId: string, bucket: CreateBucketRequest): Promise<void> {
  await api.post(`/clusters/${encodePathSegment(clusterId)}/buckets`, bucket)
}

export async function deleteBucket(clusterId: string, bucketName: string): Promise<void> {
  await api.delete(bucketResourcePath(clusterId, bucketName))
}

export async function getBucketQuota(clusterId: string, bucketName: string): Promise<BucketQuotaStatus> {
  const resp = await api.get(bucketResourcePath(clusterId, bucketName, '/quota'))
  return resp.data
}

export async function setBucketQuota(
  clusterId: string,
  bucketName: string,
  quota: BucketQuota,
): Promise<BucketQuotaStatus> {
  const resp = await api.put(bucketResourcePath(clusterId, bucketName, '/quota'), quota)
  return resp.data
}

export async function deleteBucketQuota(clusterId: string, bucketName: string): Promise<void> {
  await api.delete(bucketResourcePath(clusterId, bucketName, '/quota'))
}

// Chunks
export async function getChunk(clusterId: string, chunkId: string): Promise<ChunkInfo> {
  const resp = await api.get(`/clusters/${clusterId}/chunks/${chunkId}`)
  return resp.data
}

export async function verifyChunk(clusterId: string, chunkId: string): Promise<void> {
  await api.post(`/clusters/${clusterId}/chunks/${chunkId}/verify`)
}

// Repair
export async function triggerRepair(clusterId: string): Promise<void> {
  await api.post(`/clusters/${clusterId}/repair/trigger`)
}

export async function getRepairQueue(clusterId: string): Promise<RepairQueue> {
  const resp = await api.get(`/clusters/${clusterId}/repair/queue`)
  return resp.data
}

// GC
export async function triggerGC(clusterId: string): Promise<void> {
  await api.post(`/clusters/${clusterId}/gc/scan`)
}

// Rebalance
export async function triggerRebalance(clusterId: string): Promise<void> {
  await api.post(`/clusters/${clusterId}/rebalance/trigger`)
}

// Raft
export async function getRaftStatus(clusterId: string): Promise<RaftStatus> {
  const resp = await api.get(`/clusters/${clusterId}/raft/status`)
  return resp.data
}

// Audit
export async function getAuditLogs(clusterId: string, params?: { limit?: number; offset?: number }): Promise<AuditLog[]> {
  const resp = await api.get(`/clusters/${clusterId}/audit`, { params })
  return resp.data
}

// Types
export interface ClusterView {
  name: string
  region: string
  description: string
  metad_ops_url: string
  health: 'healthy' | 'unhealthy' | 'unknown'
  lastCheck: string
  source: 'static' | 'dynamic'
}

export interface ClusterAuditLog {
  id: number
  cluster_id: string
  action: 'add' | 'remove' | 'update'
  operator: string
  detail: string
  created_at: string
}

export interface AggregatedResult {
  results: Record<string, any>
  failures: Record<string, string>
}

export interface NodeInfo {
  id: string
  cluster: string
  address: string
  status: 'online' | 'offline'
  capacity: number
  used: number
}

export interface BucketInfo {
  name: string
  cluster: string
  created: string
  policy: {
    replicationFactor: number
    storageTier: string
  }
  usage: {
    size: number
    objects: number
  }
}

export interface BucketQuota {
  max_bytes: number
  max_objects: number
}

export interface BucketQuotaStatus {
  bucket: string
  quota: BucketQuota | null
  usage: {
    name: string
    used_bytes: number
    objects: number
  }
  ratios: {
    bytes: number
    objects: number
  }
}

export interface CreateBucketRequest {
  name: string
  policy: {
    replicationFactor: number
    storageTier: string
  }
}

export interface ChunkInfo {
  id: string
  cluster: string
  bucket: string
  size: number
  replicas: string[]
  status: string
}

export interface RepairQueue {
  cluster: string
  pending: number
  inProgress: number
  completed: number
}

export interface RaftStatus {
  cluster: string
  leader: string
  term: number
  commit: number
  applied: number
}

export interface AuditLog {
  cluster: string
  timestamp: string
  user: string
  action: string
  resource: string
  result: string
}

export interface BackgroundTaskStatus {
  id: string
  type: string
  state: string
  target: string
  attempt_count: number
  last_error?: string
  updated_at: number
}

export interface WriteOpsStatus {
  cluster: string
  attempts: Record<string, number>
  recovery_task: BackgroundTaskStatus
  gc_task: BackgroundTaskStatus
}
