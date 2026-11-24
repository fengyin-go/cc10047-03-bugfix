<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <section
      v-if="isBackfill || isBatchMerge"
      class="delivery-panel"
      data-testid="delivery-panel"
    >
      <header class="delivery-head">
        <div>
          <h3>{{ isBackfill ? '逐分区回填进度与交付边界' : '合并入口交付边界' }}</h3>
          <p class="page-desc">
            进度与可用回填数据范围来自逐分区实际结果；失败 / 无产出分区只保留依据，不计入完成。
          </p>
        </div>
        <div class="delivery-controls">
          <label v-if="isBatchMerge" class="filter-item">
            <span>回填任务 ID</span>
            <input v-model="linkedBackfillId" data-testid="linked-backfill" placeholder="如 1" />
          </label>
          <label class="filter-item">
            <span>交付口径</span>
            <select v-model="deliveryMode" data-testid="delivery-mode">
              <option value="">按任务配置</option>
              <option value="partition">按分区交付</option>
              <option value="batch">整批确认</option>
            </select>
          </label>
          <button class="btn" data-testid="load-delivery" type="button" @click="loadDelivery()">
            查看交付边界
          </button>
        </div>
      </header>

      <p v-if="deliveryMessage" class="error-text" data-testid="delivery-message">{{ deliveryMessage }}</p>

      <template v-if="backfillDetail">
        <div class="progress-row">
          <div class="progress-track">
            <div
              class="progress-fill"
              :class="{ complete: backfillDetail.complete }"
              :style="{ width: `${backfillDetail.progress_percent}%` }"
              data-testid="backfill-progress"
            />
          </div>
          <strong data-testid="backfill-progress-value">{{ backfillDetail.progress_percent }}%</strong>
          <span class="progress-basis">{{ backfillDetail.progress_basis }}</span>
        </div>
        <div class="delivery-grid">
          <div>
            <h4>可用回填数据范围</h4>
            <p data-testid="usable-range">
              {{ backfillDetail.usable_backfill_range.join('、') || '（无）' }}
              · {{ backfillDetail.usable_rows }} 行
            </p>
          </div>
          <div>
            <h4>本批交付 / 留在回填侧</h4>
            <p data-testid="delivery-split">
              交付：{{ backfillDetail.delivered_partitions.join('、') || '（无）' }} ·
              留待：{{ backfillDetail.held_partitions.join('、') || '（无）' }}
            </p>
            <p class="page-desc" data-testid="delivery-reason">
              {{ backfillDetail.delivery_boundary.reason }}
            </p>
          </div>
          <div>
            <h4>下一批读取范围</h4>
            <p data-testid="next-scope">{{ backfillDetail.next_batch_read_scope.join('、') || '（无）' }}</p>
          </div>
        </div>
        <table class="data-table partition-table">
          <thead>
            <tr>
              <th>分区</th>
              <th>状态</th>
              <th>实际行数</th>
              <th>交付</th>
              <th>依据</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="item in backfillDetail.usable_range_detail"
              :key="item.partition"
              :data-testid="`partition-${item.partition}`"
            >
              <td>{{ item.partition }}</td>
              <td>{{ item.status }}</td>
              <td>{{ item.rows }}</td>
              <td>{{ item.delivered ? '已交付' : item.usable ? '留在回填侧' : '拒绝' }}</td>
              <td>{{ item.basis }}：{{ item.reason }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="backfillDetail.merge_todos.length" class="todo-list" data-testid="merge-todos">
          <span v-for="(item, index) in backfillDetail.merge_todos" :key="index" class="todo-item">
            {{ item.todo }}
          </span>
        </p>
      </template>

      <template v-else-if="mergeDelivery">
        <div class="delivery-grid">
          <div>
            <h4>合并入口接收</h4>
            <p data-testid="merge-accepted">
              {{ mergeDelivery.accepted_partitions.join('、') || '（无）' }}
            </p>
          </div>
          <div>
            <h4>拒绝的失效范围</h4>
            <p data-testid="merge-rejected">
              {{
                mergeDelivery.rejected_partitions.map((item) => `${item.partition}(${item.status})`).join('、') ||
                '（无）'
              }}
            </p>
          </div>
          <div>
            <h4>下一批读取范围</h4>
            <p data-testid="merge-next-scope">
              {{ mergeDelivery.next_batch_read_scope.join('、') || '（无）' }}
            </p>
          </div>
        </div>
        <p v-if="mergeDelivery.merge_todos.length" class="todo-list" data-testid="merge-todos">
          <span v-for="(item, index) in mergeDelivery.merge_todos" :key="index" class="todo-item">
            {{ item.todo }}
          </span>
        </p>
        <p v-else class="page-desc">合并待办已清空，交付结论完整。</p>
      </template>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  loadBackfillDetail,
  loadMergeDelivery,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { BackfillDetail, EntryRow, MergeDelivery } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const isBackfill = meta.key === 'backfill_pipeline'
const isBatchMerge = meta.key === 'batch_merge'
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const deliveryMode = ref('')
const linkedBackfillId = ref('1')
const backfillDetail = ref<BackfillDetail | null>(null)
const mergeDelivery = ref<MergeDelivery | null>(null)
const deliveryMessage = ref('')

const stats = computed(() => {
  if (isBackfill && backfillDetail.value) {
    return [
      { label: meta.metrics[0], value: total.value },
      { label: '回填进度', value: `${backfillDetail.value.progress_percent}%` },
      { label: '可用回填数据范围', value: backfillDetail.value.usable_backfill_range.join('、') || '（无）' },
    ]
  }
  return [
    { label: meta.metrics[0], value: total.value },
    { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
    {
      label: meta.metrics[2],
      value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
    },
  ]
})
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    if (isBackfill) await loadDelivery(Number(row.id))
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function loadDelivery(backfillId?: number) {
  deliveryMessage.value = ''
  backfillDetail.value = null
  mergeDelivery.value = null
  const mode = deliveryMode.value || undefined
  try {
    if (isBackfill) {
      const id = backfillId ?? Number(rows.value[0]?.id ?? 1)
      backfillDetail.value = await loadBackfillDetail(id, mode)
    } else if (isBatchMerge) {
      const mergeId = Number(rows.value[0]?.id ?? 1)
      mergeDelivery.value = await loadMergeDelivery(mergeId, Number(linkedBackfillId.value) || 1, mode)
    }
  } catch (error) {
    deliveryMessage.value = error instanceof Error ? error.message : '交付边界读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    if (isBackfill) await loadDelivery()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
