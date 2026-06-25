import { invoke } from '@tauri-apps/api/core'
import type {
  CreateDataSourceInput,
  CreateSyncJobInput,
  IncrementalAssessment,
  DashboardSnapshot,
  ProbeResult,
  SchemaCatalog,
  StructureDiffPreview,
  SyncJobSummary,
  SyncPlanPreview,
  SyncRunSummary,
  TableCatalog,
  DataSourceSummary,
} from '../types'

export async function fetchDashboardSnapshot() {
  return invoke<DashboardSnapshot>('dashboard_snapshot')
}

export async function listDataSources() {
  return invoke<DataSourceSummary[]>('list_data_sources')
}

export async function probeDataSource(input: CreateDataSourceInput) {
  return invoke<ProbeResult>('probe_data_source', { input })
}

export async function createDataSource(input: CreateDataSourceInput) {
  return invoke<DataSourceSummary>('create_data_source', { input })
}

export async function fetchSchemas(dataSourceId: string) {
  return invoke<SchemaCatalog[]>('list_schemas', { dataSourceId })
}

export async function fetchSchemaTables(dataSourceId: string, schemaName: string) {
  return invoke<TableCatalog[]>('list_schema_tables', { dataSourceId, schemaName })
}

export async function planSyncJob(input: CreateSyncJobInput) {
  return invoke<SyncPlanPreview>('plan_sync_job', { input })
}

export async function createSyncJob(input: CreateSyncJobInput) {
  return invoke<SyncJobSummary>('create_sync_job', { input })
}

export async function listSyncJobs() {
  return invoke<SyncJobSummary[]>('list_sync_jobs')
}

export async function previewStructureDiff(jobId: string) {
  return invoke<StructureDiffPreview>('preview_structure_diff', { jobId })
}

export async function runSyncJob(jobId: string) {
  return invoke<SyncRunSummary>('run_sync_job', { jobId })
}

export async function listSyncRuns() {
  return invoke<SyncRunSummary[]>('list_sync_runs')
}

export async function assessIncrementalSync(jobId: string) {
  return invoke<IncrementalAssessment>('assess_incremental_sync', { jobId })
}
