import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  BackfillDetail,
  EntryRow,
  MergeDelivery,
  OverviewResult,
  PageResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export async function loadBackfillDetail(
  backfillId: number,
  deliveryMode?: string,
): Promise<BackfillDetail> {
  const query = deliveryMode ? `?${new URLSearchParams({ delivery_mode: deliveryMode })}` : ''
  const response = await request(`/api/backfill/${backfillId}/detail${query}`)
  if (!response.ok) throw new Error('回填详情读取失败')
  return await response.json()
}

export async function loadMergeDelivery(
  mergeId: number,
  backfillId: number,
  deliveryMode?: string,
): Promise<MergeDelivery> {
  const response = await request(`/api/backfill/merge/${mergeId}/evaluate`, {
    method: 'POST',
    body: JSON.stringify({ backfill_id: backfillId, delivery_mode: deliveryMode ?? null }),
  })
  const payload = await response.json()
  if (!response.ok) throw new Error(payload?.detail ?? '合并入口交付边界读取失败')
  return payload
}
