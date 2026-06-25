export type DataSourceKind = 'mysql' | 'postgresql'
export type SyncMode = 'full' | 'incremental' | 'full_then_incremental'

export type CreateDataSourceInput = {
  name: string
  kind: DataSourceKind
  host: string
  port: number
  databaseName: string
  username: string
  password: string
}

export type ProbeResult = {
  currentDatabase: string
  databaseVersion: string
  schemaCount: number
  tableCount: number
}

export type DataSourceSummary = {
  id: string
  name: string
  kind: DataSourceKind
  host: string
  port: number
  databaseName: string
  username: string
  lastProbeStatus: string | null
  lastProbeAt: string | null
  lastError: string | null
}

export type DashboardSnapshot = {
  dataSourceCount: number
  syncJobCount: number
  mysqlSourceCount: number
  postgresSourceCount: number
  syncRunCount: number
  runningSyncCount: number
}

export type SchemaCatalog = {
  name: string
  tableCount: number
}

export type TableColumn = {
  name: string
  dataType: string
  nullable: boolean
  isPrimaryKey: boolean
  defaultValue: string | null
}

export type TableCatalog = {
  schemaName: string
  name: string
  rowEstimate: number | null
  columns: TableColumn[]
}

export type CreateSyncJobInput = {
  name: string
  sourceId: string
  targetId: string
  mode: SyncMode
  selectedTables: string[]
  strategy: string
}

export type SyncPlanPreview = {
  summary: string
  riskLevel: 'low' | 'medium' | 'high'
  estimatedStages: number
  steps: string[]
}

export type SyncJobSummary = {
  id: string
  name: string
  sourceName: string
  targetName: string
  mode: string
  status: string
  strategy: string
  selectedTables: string[]
  createdAt: string
}

export type SyncRunSummary = {
  id: string
  jobId: string
  jobName: string
  status: string
  tablesTotal: number
  tablesCompleted: number
  rowsCopied: number
  startedAt: string
  finishedAt: string | null
  detail: string | null
}

export type TableDiffItem = {
  tableKey: string
  status: string
  statements: string[]
  notes: string[]
  transferableColumns: number
}

export type StructureDiffPreview = {
  sourceName: string
  targetName: string
  summary: string
  items: TableDiffItem[]
}

export type CdcSourceCapability = {
  supported: boolean
  ready: boolean
  engine: string
  details: string[]
}

export type CdcCheckpoint = {
  jobId: string
  mode: string
  payload: string
  updatedAt: string
}

export type IncrementalAssessment = {
  jobName: string
  sourceName: string
  targetName: string
  sourceCapability: CdcSourceCapability
  checkpoint: CdcCheckpoint | null
  eventModel: string[]
  nextActions: string[]
}

export type AppError = {
  message: string
}
