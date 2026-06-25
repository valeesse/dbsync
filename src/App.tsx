import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import './App.css'
import {
  assessIncrementalSync,
  createDataSource,
  createSyncJob,
  fetchDashboardSnapshot,
  fetchSchemaTables,
  fetchSchemas,
  listDataSources,
  listSyncJobs,
  listSyncRuns,
  planSyncJob,
  previewStructureDiff,
  probeDataSource,
  runSyncJob,
} from './lib/api'
import type {
  AppError,
  CreateDataSourceInput,
  CreateSyncJobInput,
  DataSourceKind,
  DataSourceSummary,
  DashboardSnapshot,
  IncrementalAssessment,
  ProbeResult,
  SchemaCatalog,
  StructureDiffPreview,
  SyncJobSummary,
  SyncPlanPreview,
  SyncRunSummary,
  TableCatalog,
} from './types'

const initialDataSourceForm: CreateDataSourceInput = {
  name: '',
  kind: 'mysql',
  host: '127.0.0.1',
  port: 3306,
  databaseName: '',
  username: '',
  password: '',
}

const initialSyncForm: CreateSyncJobInput = {
  name: '',
  sourceId: '',
  targetId: '',
  mode: 'full',
  selectedTables: [],
  strategy: 'snapshot_then_cdc',
}

function App() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null)
  const [dataSources, setDataSources] = useState<DataSourceSummary[]>([])
  const [syncJobs, setSyncJobs] = useState<SyncJobSummary[]>([])
  const [syncRuns, setSyncRuns] = useState<SyncRunSummary[]>([])
  const [schemas, setSchemas] = useState<SchemaCatalog[]>([])
  const [tables, setTables] = useState<TableCatalog[]>([])
  const [selectedSourceId, setSelectedSourceId] = useState('')
  const [selectedSchemaName, setSelectedSchemaName] = useState('')
  const [dataSourceForm, setDataSourceForm] =
    useState<CreateDataSourceInput>(initialDataSourceForm)
  const [syncForm, setSyncForm] = useState<CreateSyncJobInput>(initialSyncForm)
  const [probeResult, setProbeResult] = useState<ProbeResult | null>(null)
  const [syncPlan, setSyncPlan] = useState<SyncPlanPreview | null>(null)
  const [diffPreview, setDiffPreview] = useState<StructureDiffPreview | null>(null)
  const [incrementalAssessment, setIncrementalAssessment] =
    useState<IncrementalAssessment | null>(null)
  const [selectedJobId, setSelectedJobId] = useState('')
  const [busyAction, setBusyAction] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const availableTargets = useMemo(
    () => dataSources.filter((source) => source.id !== syncForm.sourceId),
    [dataSources, syncForm.sourceId],
  )

  useEffect(() => {
    void refreshWorkspace()
  }, [])

  useEffect(() => {
    if (!selectedSourceId) {
      return
    }

    void loadSchemas(selectedSourceId)
  }, [selectedSourceId])

  useEffect(() => {
    if (!selectedSourceId || !selectedSchemaName) {
      return
    }

    void loadTables(selectedSourceId, selectedSchemaName)
  }, [selectedSourceId, selectedSchemaName])

  async function refreshWorkspace() {
    setBusyAction('workspace')
    setError(null)
    try {
      const [nextSnapshot, nextSources, nextJobs, nextRuns] = await Promise.all([
        fetchDashboardSnapshot(),
        listDataSources(),
        listSyncJobs(),
        listSyncRuns(),
      ])

      setSnapshot(nextSnapshot)
      setDataSources(nextSources)
      setSyncJobs(nextJobs)
      setSyncRuns(nextRuns)
      if (!nextSources.length) {
        setSchemas([])
        setTables([])
        setSelectedSchemaName('')
      }
      setSelectedJobId((current) =>
        current && nextJobs.some((job) => job.id === current) ? current : (nextJobs[0]?.id ?? ''),
      )
      setSelectedSourceId((current) =>
        current && nextSources.some((source) => source.id === current)
          ? current
          : (nextSources[0]?.id ?? ''),
      )
      setSyncForm((current) => ({
        ...current,
        sourceId:
          current.sourceId && nextSources.some((source) => source.id === current.sourceId)
            ? current.sourceId
            : (nextSources[0]?.id ?? ''),
        targetId:
          current.targetId && nextSources.some((source) => source.id === current.targetId)
            ? current.targetId
            : (nextSources[1]?.id ?? ''),
      }))
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function loadSchemas(dataSourceId: string) {
    setBusyAction('schemas')
    setError(null)
    try {
      const nextSchemas = await fetchSchemas(dataSourceId)
      setSchemas(nextSchemas)
      setSelectedSchemaName((current) =>
        current && nextSchemas.some((schema) => schema.name === current)
          ? current
          : (nextSchemas[0]?.name ?? ''),
      )
    } catch (cause) {
      setError(extractError(cause))
      setSchemas([])
      setTables([])
      setSelectedSchemaName('')
    } finally {
      setBusyAction(null)
    }
  }

  async function loadTables(dataSourceId: string, schema: string) {
    setBusyAction('tables')
    setError(null)
    try {
      const nextTables = await fetchSchemaTables(dataSourceId, schema)
      setTables(nextTables)
    } catch (cause) {
      setError(extractError(cause))
      setTables([])
    } finally {
      setBusyAction(null)
    }
  }

  async function handleProbe(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setBusyAction('probe')
    setError(null)
    setProbeResult(null)

    try {
      setProbeResult(await probeDataSource(dataSourceForm))
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function handleSaveDataSource() {
    setBusyAction('save-source')
    setError(null)

    try {
      await createDataSource(dataSourceForm)
      setProbeResult(null)
      setDataSourceForm({
        ...initialDataSourceForm,
        kind: dataSourceForm.kind,
        host: dataSourceForm.host,
        port: dataSourceForm.kind === 'mysql' ? 3306 : 5432,
      })
      await refreshWorkspace()
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function handlePlanSync() {
    setBusyAction('plan-sync')
    setError(null)
    setSyncPlan(null)

    try {
      setSyncPlan(await planSyncJob(syncForm))
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function handleCreateSyncJob() {
    setBusyAction('save-sync')
    setError(null)

    try {
      await createSyncJob(syncForm)
      setSyncPlan(null)
      setSyncForm((current) => ({ ...current, name: '' }))
      await refreshWorkspace()
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function handlePreviewDiff(jobId: string) {
    setBusyAction('diff')
    setError(null)

    try {
      setSelectedJobId(jobId)
      setDiffPreview(await previewStructureDiff(jobId))
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function handleAssessIncremental(jobId: string) {
    setBusyAction('cdc')
    setError(null)

    try {
      setSelectedJobId(jobId)
      setIncrementalAssessment(await assessIncrementalSync(jobId))
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  async function handleRunSync(jobId: string) {
    setBusyAction('run-sync')
    setError(null)

    try {
      setSelectedJobId(jobId)
      await runSyncJob(jobId)
      await refreshWorkspace()
    } catch (cause) {
      setError(extractError(cause))
    } finally {
      setBusyAction(null)
    }
  }

  function toggleSelectedTable(tableKey: string) {
    setSyncForm((current) => ({
      ...current,
      selectedTables: current.selectedTables.includes(tableKey)
        ? current.selectedTables.filter((item) => item !== tableKey)
        : [...current.selectedTables, tableKey],
    }))
  }

  const canSaveSource = Boolean(
    probeResult &&
      dataSourceForm.name &&
      dataSourceForm.databaseName &&
      dataSourceForm.username &&
      dataSourceForm.password,
  )

  return (
    <div className="shell">
      <header className="hero">
        <div>
          <p className="eyebrow">DBSYNC DESKTOP</p>
          <h1>替代 Navicat 的高性能异构数据库同步工作台</h1>
          <p className="hero-copy">
            基于 Rust 原生同步内核与 Tauri 桌面壳，面向 MySQL / PostgreSQL
            的结构浏览、结构对比、全量同步与增量同步扩展。
          </p>
        </div>
        <div className="hero-status">
          <span className={`status-pill ${busyAction ? 'active' : ''}`}>
            {busyAction ? `执行中: ${busyAction}` : '本地工作区已就绪'}
          </span>
          <button type="button" className="ghost-button" onClick={() => void refreshWorkspace()}>
            刷新工作区
          </button>
        </div>
      </header>

      {error ? <div className="banner error">{error}</div> : null}

      <section className="metrics-grid">
        <MetricCard label="已保存数据源" value={snapshot?.dataSourceCount ?? 0} hint="本地 SQLite 元数据库" accent="amber" />
        <MetricCard label="同步任务草稿" value={snapshot?.syncJobCount ?? 0} hint="全量 / 增量模式" accent="teal" />
        <MetricCard label="MySQL 数据源" value={snapshot?.mysqlSourceCount ?? 0} hint="基于 information_schema 采集" accent="crimson" />
        <MetricCard label="PostgreSQL 数据源" value={snapshot?.postgresSourceCount ?? 0} hint="为 logical replication 预留" accent="navy" />
        <MetricCard label="运行历史" value={snapshot?.syncRunCount ?? 0} hint={`${snapshot?.runningSyncCount ?? 0} 个运行中`} accent="teal" />
      </section>

      <main className="workspace">
        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Connection Studio</p>
              <h2>新增并探测数据源</h2>
            </div>
            {probeResult ? <div className="badge success">已探测: {probeResult.databaseVersion}</div> : null}
          </div>

          <form className="source-form" onSubmit={(event) => void handleProbe(event)}>
            <label>
              名称
              <input value={dataSourceForm.name} onChange={(event) => setDataSourceForm((current) => ({ ...current, name: event.target.value }))} placeholder="例如: analytics-prod" />
            </label>
            <label>
              类型
              <select
                value={dataSourceForm.kind}
                onChange={(event) => {
                  const kind = event.target.value as DataSourceKind
                  setDataSourceForm((current) => ({ ...current, kind, port: kind === 'mysql' ? 3306 : 5432 }))
                }}
              >
                <option value="mysql">MySQL</option>
                <option value="postgresql">PostgreSQL</option>
              </select>
            </label>
            <label>
              主机
              <input value={dataSourceForm.host} onChange={(event) => setDataSourceForm((current) => ({ ...current, host: event.target.value }))} placeholder="127.0.0.1" />
            </label>
            <label>
              端口
              <input type="number" value={dataSourceForm.port} onChange={(event) => setDataSourceForm((current) => ({ ...current, port: Number(event.target.value) }))} />
            </label>
            <label>
              数据库
              <input value={dataSourceForm.databaseName} onChange={(event) => setDataSourceForm((current) => ({ ...current, databaseName: event.target.value }))} placeholder={dataSourceForm.kind === 'mysql' ? 'orders' : 'warehouse'} />
            </label>
            <label>
              用户名
              <input value={dataSourceForm.username} onChange={(event) => setDataSourceForm((current) => ({ ...current, username: event.target.value }))} placeholder="readonly_sync" />
            </label>
            <label className="full-span">
              密码
              <input type="password" value={dataSourceForm.password} onChange={(event) => setDataSourceForm((current) => ({ ...current, password: event.target.value }))} placeholder="密码仅写入系统凭据存储" />
            </label>
            <div className="form-actions full-span">
              <button type="submit" className="primary-button" disabled={busyAction === 'probe'}>
                测试连接
              </button>
              <button type="button" className="secondary-button" disabled={!canSaveSource || busyAction === 'save-source'} onClick={() => void handleSaveDataSource()}>
                保存到工作区
              </button>
            </div>
          </form>

          {probeResult ? (
            <div className="probe-card">
              <div>
                <span>版本</span>
                <strong>{probeResult.databaseVersion}</strong>
              </div>
              <div>
                <span>当前库</span>
                <strong>{probeResult.currentDatabase}</strong>
              </div>
              <div>
                <span>Schema 数</span>
                <strong>{probeResult.schemaCount}</strong>
              </div>
              <div>
                <span>采样表数</span>
                <strong>{probeResult.tableCount}</strong>
              </div>
            </div>
          ) : null}
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Saved Sources</p>
              <h2>已注册数据源</h2>
            </div>
            <span className="badge neutral">{dataSources.length} 个</span>
          </div>
          <div className="source-list">
            {dataSources.map((source) => (
              <button type="button" key={source.id} className={`source-card ${selectedSourceId === source.id ? 'selected' : ''}`} onClick={() => setSelectedSourceId(source.id)}>
                <div className="source-card-header">
                  <strong>{source.name}</strong>
                  <span className={`kind-chip ${source.kind}`}>{source.kind}</span>
                </div>
                <p>{source.host}:{source.port}</p>
                <p>{source.databaseName}</p>
              </button>
            ))}
            {!dataSources.length ? <div className="empty-state">先新增一个可连接的数据源，目录树和同步任务会自动激活。</div> : null}
          </div>
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Catalog Explorer</p>
              <h2>结构浏览与表级明细</h2>
            </div>
            {selectedSchemaName ? <span className="badge neutral">{selectedSchemaName}</span> : null}
          </div>
          <div className="catalog-layout">
            <aside className="schema-rail">
              {schemas.map((schema) => (
                <button type="button" key={schema.name} className={`schema-pill ${selectedSchemaName === schema.name ? 'selected' : ''}`} onClick={() => setSelectedSchemaName(schema.name)}>
                  <span>{schema.name}</span>
                  <small>{schema.tableCount} tables</small>
                </button>
              ))}
              {!schemas.length ? <div className="empty-rail">选择已保存数据源后加载 schema。</div> : null}
            </aside>
            <div className="table-grid">
              {tables.map((table) => {
                const tableKey = `${selectedSchemaName}.${table.name}`
                const checked = syncForm.selectedTables.includes(tableKey)
                return (
                  <article className="table-card" key={table.name}>
                    <div className="table-card-header">
                      <div>
                        <h3>{table.name}</h3>
                        <p>{table.rowEstimate ?? 'unknown'} rows · {table.columns.length} columns</p>
                      </div>
                      <label className="check-tag">
                        <input type="checkbox" checked={checked} onChange={() => toggleSelectedTable(tableKey)} />
                        选入同步
                      </label>
                    </div>
                    <div className="column-list">
                      {table.columns.map((column) => (
                        <div className="column-row" key={column.name}>
                          <strong>{column.name}</strong>
                          <span>{column.dataType}</span>
                          <small>{column.nullable ? 'NULL' : 'NOT NULL'}</small>
                        </div>
                      ))}
                    </div>
                  </article>
                )
              })}
              {!tables.length ? <div className="empty-state">这里会展示当前 schema 下的表和字段结构。</div> : null}
            </div>
          </div>
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Sync Studio</p>
              <h2>同步任务规划</h2>
            </div>
            <span className="badge neutral">MVP: 单向全量 / 增量草稿</span>
          </div>
          <div className="sync-grid">
            <div className="sync-form">
              <label>
                任务名
                <input value={syncForm.name} onChange={(event) => setSyncForm((current) => ({ ...current, name: event.target.value }))} placeholder="mysql-orders-to-pg-warehouse" />
              </label>
              <label>
                源数据源
                <select value={syncForm.sourceId} onChange={(event) => setSyncForm((current) => ({ ...current, sourceId: event.target.value }))}>
                  <option value="">请选择</option>
                  {dataSources.map((source) => (
                    <option value={source.id} key={source.id}>
                      {source.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                目标数据源
                <select value={syncForm.targetId} onChange={(event) => setSyncForm((current) => ({ ...current, targetId: event.target.value }))}>
                  <option value="">请选择</option>
                  {availableTargets.map((source) => (
                    <option value={source.id} key={source.id}>
                      {source.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                模式
                <select value={syncForm.mode} onChange={(event) => setSyncForm((current) => ({ ...current, mode: event.target.value as CreateSyncJobInput['mode'] }))}>
                  <option value="full">全量初始化</option>
                  <option value="incremental">增量预留</option>
                  <option value="full_then_incremental">全量 + 增量</option>
                </select>
              </label>
              <label className="full-span">
                执行策略
                <select value={syncForm.strategy} onChange={(event) => setSyncForm((current) => ({ ...current, strategy: event.target.value }))}>
                  <option value="snapshot_then_cdc">snapshot_then_cdc</option>
                  <option value="snapshot_only">snapshot_only</option>
                  <option value="cdc_only">cdc_only</option>
                </select>
              </label>
            </div>
            <div className="sync-preview">
              <div className="selection-summary">
                <span>已选择表</span>
                <strong>{syncForm.selectedTables.length}</strong>
              </div>
              <div className="selected-tables">
                {syncForm.selectedTables.map((table) => (
                  <span className="table-tag" key={table}>
                    {table}
                  </span>
                ))}
                {!syncForm.selectedTables.length ? <p>先在上面的结构浏览区勾选需要同步的表。</p> : null}
              </div>
              <div className="form-actions">
                <button type="button" className="primary-button" onClick={() => void handlePlanSync()} disabled={busyAction === 'plan-sync'}>
                  生成同步计划
                </button>
                <button type="button" className="secondary-button" onClick={() => void handleCreateSyncJob()} disabled={!syncPlan || busyAction === 'save-sync'}>
                  保存任务草稿
                </button>
              </div>
              {syncPlan ? (
                <div className="plan-card">
                  <div className="plan-headline">
                    <strong>{syncPlan.riskLevel.toUpperCase()} 风险</strong>
                    <span>{syncPlan.estimatedStages} 个阶段</span>
                  </div>
                  <p>{syncPlan.summary}</p>
                  <ul>
                    {syncPlan.steps.map((step) => (
                      <li key={step}>{step}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Task Drafts</p>
              <h2>已保存同步任务</h2>
            </div>
            <span className="badge neutral">{syncJobs.length} 个</span>
          </div>
          <div className="job-list">
            {syncJobs.map((job) => (
              <article key={job.id} className={`job-card ${selectedJobId === job.id ? 'selected-job' : ''}`}>
                <div className="job-card-header">
                  <strong>{job.name}</strong>
                  <span className="badge neutral">{job.status}</span>
                </div>
                <p>{job.sourceName} → {job.targetName}</p>
                <p>{job.mode} · {job.selectedTables.length} tables</p>
                <div className="job-actions">
                  <button type="button" className="ghost-button" onClick={() => void handlePreviewDiff(job.id)}>
                    结构 Diff
                  </button>
                  <button type="button" className="ghost-button" onClick={() => void handleAssessIncremental(job.id)}>
                    增量准备度
                  </button>
                  <button type="button" className="primary-button" onClick={() => void handleRunSync(job.id)}>
                    执行全量同步
                  </button>
                </div>
              </article>
            ))}
            {!syncJobs.length ? <div className="empty-state">先保存一个同步计划草稿，后续执行器和进度监控会接在这里。</div> : null}
          </div>
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Structure Diff</p>
              <h2>源目标结构变更计划</h2>
            </div>
            {diffPreview ? <span className="badge neutral">{diffPreview.sourceName} → {diffPreview.targetName}</span> : null}
          </div>
          {diffPreview ? (
            <div className="diff-list">
              <p className="diff-summary">{diffPreview.summary}</p>
              {diffPreview.items.map((item) => (
                <article className="diff-card" key={item.tableKey}>
                  <div className="table-card-header">
                    <strong>{item.tableKey}</strong>
                    <span className="badge neutral">{item.status}</span>
                  </div>
                  <p>可传输列: {item.transferableColumns}</p>
                  {item.statements.length ? (
                    <pre className="code-block">{item.statements.join(';\n')};</pre>
                  ) : (
                    <p>无需 DDL 变更。</p>
                  )}
                  {item.notes.length ? (
                    <ul className="note-list">
                      {item.notes.map((note) => (
                        <li key={note}>{note}</li>
                      ))}
                    </ul>
                  ) : null}
                </article>
              ))}
            </div>
          ) : (
            <div className="empty-state">从同步任务卡片点击“结构 Diff”后，这里会展示建表 / 加列 / 类型冲突计划。</div>
          )}
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Incremental Readiness</p>
              <h2>CDC 能力与 Checkpoint</h2>
            </div>
            {incrementalAssessment ? (
              <span className={`badge ${incrementalAssessment.sourceCapability.ready ? 'success' : 'neutral'}`}>
                {incrementalAssessment.sourceCapability.engine}
              </span>
            ) : null}
          </div>
          {incrementalAssessment ? (
            <div className="cdc-grid">
              <div className="plan-card">
                <div className="plan-headline">
                  <strong>{incrementalAssessment.jobName}</strong>
                  <span>{incrementalAssessment.sourceName} → {incrementalAssessment.targetName}</span>
                </div>
                <p>CDC 就绪: {incrementalAssessment.sourceCapability.ready ? '是' : '否'}</p>
                <ul>
                  {incrementalAssessment.sourceCapability.details.map((detail) => (
                    <li key={detail}>{detail}</li>
                  ))}
                </ul>
              </div>
              <div className="plan-card">
                <strong>统一事件模型</strong>
                <ul>
                  {incrementalAssessment.eventModel.map((field) => (
                    <li key={field}>{field}</li>
                  ))}
                </ul>
              </div>
              <div className="plan-card">
                <strong>Checkpoint</strong>
                {incrementalAssessment.checkpoint ? (
                  <>
                    <p>{incrementalAssessment.checkpoint.updatedAt}</p>
                    <pre className="code-block">{incrementalAssessment.checkpoint.payload}</pre>
                  </>
                ) : (
                  <p>当前还没有保存的位点。</p>
                )}
              </div>
            </div>
          ) : (
            <div className="empty-state">点击“增量准备度”后，这里会展示源端 CDC 配置、事件模型和本地 checkpoint。</div>
          )}
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="panel-kicker">Run History</p>
              <h2>全量同步执行记录</h2>
            </div>
            <span className="badge neutral">{syncRuns.length} 条</span>
          </div>
          <div className="run-list">
            {syncRuns.map((run) => (
              <article className="job-card" key={run.id}>
                <div className="job-card-header">
                  <strong>{run.jobName}</strong>
                  <span className={`badge ${run.status === 'succeeded' ? 'success' : 'neutral'}`}>{run.status}</span>
                </div>
                <p>{run.tablesCompleted}/{run.tablesTotal} tables · {run.rowsCopied} rows</p>
                <p>{run.startedAt}</p>
                {run.detail ? <p>{run.detail}</p> : null}
              </article>
            ))}
            {!syncRuns.length ? <div className="empty-state">执行一次全量同步后，这里会出现运行历史与行数统计。</div> : null}
          </div>
        </section>
      </main>
    </div>
  )
}

function MetricCard({
  label,
  value,
  hint,
  accent,
}: {
  label: string
  value: number
  hint: string
  accent: 'amber' | 'teal' | 'crimson' | 'navy'
}) {
  return (
    <article className={`metric-card ${accent}`}>
      <p>{label}</p>
      <strong>{value}</strong>
      <span>{hint}</span>
    </article>
  )
}

function extractError(cause: unknown) {
  if (typeof cause === 'object' && cause && 'message' in cause) {
    return String((cause as AppError).message)
  }

  return '发生了未知错误。'
}

export default App
