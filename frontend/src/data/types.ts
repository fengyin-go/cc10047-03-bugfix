/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

export type PartitionResult = {
  partition: string
  status: string
  rows: number
  accepted_rows: number
  usable: boolean
  delivered: boolean
  basis: string
  reason: string
}

export type MergeTodo = {
  type: string
  todo: string
  partition?: string
  partitions?: string[]
  status?: string
  reason?: string
}

export type DeliveryBoundary = {
  mode: string
  label: string
  priority: number
  mode_conflict: boolean
  complete: boolean
  delivered_partitions: string[]
  held_partitions: string[]
  rejected_partitions: PartitionResult[]
  rejected_failed: string[]
  rejected_no_output: string[]
  next_batch_read_scope: string[]
  reason: string
}

export type BackfillDetail = {
  ok: boolean
  message?: string
  task_id: string
  table: string
  status: string
  progress_percent: number
  progress_basis: string
  completed_rows: number
  usable_rows: number
  delivered_rows: number
  complete: boolean
  successful_partitions: string[]
  failed_partition_names: string[]
  empty_partitions: string[]
  usable_backfill_range: string[]
  usable_range_detail: PartitionResult[]
  delivered_partitions: string[]
  held_partitions: string[]
  rejected_partitions: PartitionResult[]
  delivery_mode: string
  delivery_mode_label: string
  delivery_boundary: DeliveryBoundary
  next_batch_read_scope: string[]
  merge_todos: MergeTodo[]
}

export type MergeDelivery = {
  ok: boolean
  message?: string
  delivery_mode: string
  delivery_mode_label: string
  accepted_partitions: string[]
  delivered_partitions: string[]
  held_partitions: string[]
  rejected_partitions: PartitionResult[]
  rejected_no_output: string[]
  next_batch_read_scope: string[]
  merge_todos: MergeTodo[]
  merge_pending: { has_todos: boolean; todos: MergeTodo[] }
}
