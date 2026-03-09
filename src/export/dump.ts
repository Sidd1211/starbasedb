import { executeOperation } from '.'
import { StarbaseDBConfiguration } from '../handler'
import { DataSource } from '../types'
import { createResponse } from '../utils'

// Constants

/** Rows per batch — keeps memory usage constant for any DB size. */
const DEFAULT_CHUNK_SIZE = 1_000

/**
 * Wall-clock budget per DO invocation (ms).
 * Cloudflare enforces a 30s limit; we leave headroom for alarm scheduling.
 */
const EXECUTION_BUDGET_MS = 24_000

/** Yield between batches so other requests are not starved. */
const BREATHING_INTERVAL_MS = 50

/** DO storage key that identifies the currently-active async export job. */
const ACTIVE_JOB_KEY = 'async_dump_active_job'

/** Prefix for persisted DumpJobState entries in DO storage. */
const JOB_PREFIX = 'async_dump_job:'

/** R2 minimum part size for multipart upload (5 MiB). */
const R2_MIN_PART_BYTES = 5 * 1024 * 1024

// Minimal R2 interface shim
//
// Using a structural sub-type instead of the global R2Bucket avoids the
// "types returned by head() are incompatible" error that arises when
// @cloudflare/workers-types and lib.dom disagree on the Headers interface.

interface R2UploadedPartResult {
    etag: string
}

interface R2MultipartHandle {
    uploadPart(
        partNumber: number,
        value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob
    ): Promise<R2UploadedPartResult>
    complete(
        uploadedParts: Array<{ partNumber: number; etag: string }>
    ): Promise<void>
    abort(): Promise<void>
}

interface R2ObjectBody {
    readonly body: ReadableStream
}

export interface R2BucketShim {
    createMultipartUpload(
        key: string,
        options?: Record<string, unknown>
    ): Promise<R2MultipartHandle & { uploadId: string }>
    resumeMultipartUpload(key: string, uploadId: string): R2MultipartHandle
    get(key: string): Promise<R2ObjectBody | null>
}

// Types

export type DumpFormat = 'sql' | 'csv' | 'json'

export interface DumpJobState {
    jobId: string
    status: 'processing' | 'completed' | 'failed'
    format: DumpFormat
    callbackUrl?: string
    chunkSize: number
    /** All user-table names discovered at job start, in order. */
    tables: string[]
    /** Which table we are currently on (index into `tables`). */
    tableIndex: number
    /** Last rowid seen in the current table — used for keyset pagination. */
    lastRowid: number
    totalRows: number
    processedRows: number
    startedAt: string
    completedAt?: string
    error?: string
    /** R2 object key, e.g. "dump_20240101-170000.sql" */
    r2Key: string
    /** Active multipart upload ID (undefined once the upload is completed). */
    uploadId?: string
    /** Uploaded parts accumulated across invocation cycles. */
    r2Parts: Array<{ partNumber: number; etag: string }>
}

// Helpers

function generateJobId(): string {
    const now = new Date()
    const pad = (n: number, len = 2) => String(n).padStart(len, '0')
    const date = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}`
    const time = `${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`
    return `dump_${date}-${time}`
}

function escapeSQL(value: unknown): string {
    if (value === null || value === undefined) return 'NULL'
    if (typeof value === 'number' || typeof value === 'bigint') return String(value)
    if (typeof value === 'boolean') return value ? '1' : '0'
    return `'${String(value).replace(/'/g, "''")}'`
}

function rowToInsert(table: string, row: Record<string, unknown>): string {
    const vals = Object.values(row).map(escapeSQL).join(', ')
    return `INSERT INTO ${table} VALUES (${vals});`
}

function rowToCSV(row: Record<string, unknown>, headers: string[]): string {
    return headers
        .map((h) => {
            const v = row[h]
            if (v === null || v === undefined) return ''
            const s = String(v)
            return s.includes(',') || s.includes('"') || s.includes('\n')
                ? `"${s.replace(/"/g, '""')}"`
                : s
        })
        .join(',')
}

function dumpContentType(format: DumpFormat): string {
    if (format === 'csv') return 'text/csv'
    if (format === 'json') return 'application/json'
    return 'application/x-sqlite3'
}

function dumpFilename(jobId: string, format: DumpFormat): string {
    return `${jobId}.${format}`
}

async function sleep(ms: number): Promise<void> {
    await new Promise<void>((r) => setTimeout(r, ms))
}

// Original synchronous route — preserved for backward compatibility

/**
 * dumpDatabaseRoute — original implementation, unchanged.
 * Works for small databases that finish within the 30-second window.
 * For large databases the caller should use AsyncDumpManager.startDump().
 */
export async function dumpDatabaseRoute(
    dataSource: DataSource,
    config: StarbaseDBConfiguration
): Promise<Response> {
    try {
        // Get all table names
        const tablesResult = await executeOperation(
            [{ sql: "SELECT name FROM sqlite_master WHERE type='table';" }],
            dataSource,
            config
        )

        const tables = tablesResult.map((row: any) => row.name)
        let dumpContent = 'SQLite format 3\0' // SQLite file header

        // Iterate through all tables
        for (const table of tables) {
            // Get table schema
            const schemaResult = await executeOperation(
                [
                    {
                        sql: `SELECT sql FROM sqlite_master WHERE type='table' AND name='${table}';`,
                    },
                ],
                dataSource,
                config
            )

            if (schemaResult.length) {
                const schema = schemaResult[0].sql
                dumpContent += `\n-- Table: ${table}\n${schema};\n\n`
            }

            // Get table data
            const dataResult = await executeOperation(
                [{ sql: `SELECT * FROM ${table};` }],
                dataSource,
                config
            )

            for (const row of dataResult) {
                const values = Object.values(row).map((value) =>
                    typeof value === 'string'
                        ? `'${value.replace(/'/g, "''")}'`
                        : value
                )
                dumpContent += `INSERT INTO ${table} VALUES (${values.join(', ')});\n`
            }

            dumpContent += '\n'
        }

        // Create a Blob from the dump content
        const blob = new Blob([dumpContent], { type: 'application/x-sqlite3' })

        const headers = new Headers({
            'Content-Type': 'application/x-sqlite3',
            'Content-Disposition': 'attachment; filename="database_dump.sql"',
        })

        return new Response(blob, { headers })
    } catch (error: any) {
        console.error('Database Dump Error:', error)
        return createResponse(undefined, 'Failed to create database dump', 500)
    }
}

// AsyncDumpManager — handles databases of any size via R2 + DO Alarms

/**
 * Manages long-running database exports.
 *
 * How to wire into StarbaseDBDurableObject:
 *
 * 1. In fetch():
 *      const mgr = this._makeDumpManager()
 *      if (method === 'POST' && path === '/export/dump') {
 *          return mgr.startDump(await request.json().catch(() => ({})))
 *      }
 *      if (method === 'GET' && path.startsWith('/export/dump/status/')) {
 *          return mgr.getStatus(path.split('/').pop()!)
 *      }
 *      if (method === 'GET' && path.startsWith('/export/dump/download/')) {
 *          return mgr.download(path.split('/').pop()!)
 *      }
 *
 * 2. In alarm():
 *      const activeJob = await this.storage.get('async_dump_active_job')
 *      if (activeJob) { await this._makeDumpManager().resumeFromAlarm(); return }
 *
 * 3. _makeDumpManager() helper on the DO:
 *      private _makeDumpManager() {
 *          return new AsyncDumpManager(
 *              this.storage,
 *              (this.env as any).DATABASE_DUMPS,
 *              this.executeQuery.bind(this),
 *              (ms) => this.setAlarm(Date.now() + ms)
 *          )
 *      }
 */
export class AsyncDumpManager {
    private readonly storage: DurableObjectStorage
    private readonly r2: R2BucketShim | undefined
    private readonly executeQuery: (opts: {
        sql: string
        params?: unknown[]
        isRaw?: boolean
    }) => Promise<unknown>
    private readonly scheduleAlarm: (delayMs: number) => Promise<void>
    private readonly ctx: DurableObjectState | undefined

    constructor(
        storage: DurableObjectStorage,
        /**
         * Pass env.DATABASE_DUMPS here. Typed as `unknown` so that the
         * workers-types / lib.dom R2Bucket version mismatch cannot propagate
         * into this file — we cast internally to our minimal R2BucketShim.
         */
        r2Binding: unknown,
        executeQuery: (opts: {
            sql: string
            params?: unknown[]
            isRaw?: boolean
        }) => Promise<unknown>,
        scheduleAlarm: (delayMs: number) => Promise<void>,
        ctx?: DurableObjectState
    ) {
        this.storage = storage
        this.r2 = r2Binding as R2BucketShim | undefined
        this.executeQuery = executeQuery
        this.scheduleAlarm = scheduleAlarm
        this.ctx = ctx
    }

    // Public API

    /**
     * POST /export/dump
     *
     * Starts a new export job.
     * - Small databases: returns the dump file directly (HTTP 200, fast-path).
     * - Large databases: returns HTTP 202 with { jobId, status, message }.
     */
    async startDump(opts: {
        format?: DumpFormat
        callbackUrl?: string
        chunkSize?: number
    }): Promise<Response> {
        const format: DumpFormat = opts.format ?? 'sql'
        const chunkSize = Math.max(1, opts.chunkSize ?? DEFAULT_CHUNK_SIZE)
        const jobId = generateJobId()

        // Discover user tables (skip internal StarbaseDB tmp_ tables)
        const tableRows = (await this.executeQuery({
            sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'tmp_%' AND name NOT IN ('sqlite_sequence', 'sqlite_schema');",
        })) as Array<{ name: string }>
        const tables = tableRows.map((r) => r.name)

        const job: DumpJobState = {
            jobId,
            status: 'processing',
            format,
            callbackUrl: opts.callbackUrl,
            chunkSize,
            tables,
            tableIndex: 0,
            lastRowid: 0,
            totalRows: 0, // updated incrementally as rows are processed
            processedRows: 0,
            startedAt: new Date().toISOString(),
            r2Key: dumpFilename(jobId, format),
            r2Parts: [],
        }

        await this._saveJob(job)

        await this._runCycle(job)

        // Reload from storage — _runCycle may have updated status to
        // 'completed' or 'failed' and persisted that change.
        const updatedJob = (await this._loadJob(job.jobId)) ?? job

        if (updatedJob.status === 'failed') {
            return Response.json(
                { error: updatedJob.error ?? 'Export failed' },
                { status: 500 }
            )
        }

        if (updatedJob.status === 'completed') {
            // Fast-path: entire export completed within the time budget
            return this._buildResponse(updatedJob)
        }

        // Async-path: job continues via DO alarms; caller should poll
        return Response.json(
            {
                jobId,
                status: 'processing',
                message:
                    `Export started. Poll /export/dump/status/${jobId} for progress.` +
                    ` Download at /export/dump/download/${jobId} once complete.`,
            },
            { status: 202 }
        )
    }

    /**
     * Called from the DO's alarm() handler.
     * Resumes any in-progress export from its last checkpoint.
     */
    async resumeFromAlarm(): Promise<void> {
        const jobId = await this.storage.get<string>(ACTIVE_JOB_KEY)
        if (!jobId) return
        const job = await this._loadJob(jobId)
        if (!job || job.status !== 'processing') return
        await this._runCycle(job)
    }

    /** GET /export/dump/status/:jobId */
    async getStatus(jobId: string): Promise<Response> {
        const job = await this._loadJob(jobId)
        if (!job) {
            return Response.json({ error: 'Job not found' }, { status: 404 })
        }
        const pct =
            job.totalRows > 0
                ? Math.round((job.processedRows / job.totalRows) * 100)
                : 100

        return Response.json({
            jobId: job.jobId,
            status: job.status,
            format: job.format,
            progress: {
                totalRows: job.totalRows,
                processedRows: job.processedRows,
                percentComplete: pct,
                startedAt: job.startedAt,
                ...(job.completedAt && { completedAt: job.completedAt }),
            },
            ...(job.status === 'completed' && {
                downloadUrl: `/export/dump/download/${job.jobId}`,
            }),
            ...(job.status === 'failed' && { error: job.error }),
        })
    }

    /** GET /export/dump/download/:jobId */
    async download(jobId: string): Promise<Response> {
        const job = await this._loadJob(jobId)
        if (!job) {
            return Response.json({ error: 'Job not found' }, { status: 404 })
        }
        if (job.status === 'processing') {
            return Response.json(
                { error: 'Export still in progress' },
                { status: 409 }
            )
        }
        if (job.status === 'failed') {
            return Response.json(
                { error: job.error ?? 'Export failed' },
                { status: 500 }
            )
        }
        return this._buildResponse(job)
    }

    // Core export loop

    /**
     * Runs one time-bounded cycle of the export.
     * Returns true if the export completed, false if it was checkpointed
     * and an alarm was scheduled to resume.
     */
    private async _runCycle(job: DumpJobState): Promise<boolean> {
        try {
        const deadline = Date.now() + EXECUTION_BUDGET_MS

        // Start R2 multipart upload on the very first cycle
        if (!job.uploadId && this.r2) {
            const mu = await this.r2.createMultipartUpload(job.r2Key, {
                httpMetadata: {
                    contentType: dumpContentType(job.format),
                    contentDisposition: `attachment; filename="${job.r2Key}"`,
                },
            })
            job.uploadId = mu.uploadId
            await this._saveJob(job)
        }

        let buffer = ''
        let partNumber = job.r2Parts.length + 1

        // File-level header — written once at the very beginning
        if (job.processedRows === 0) {
            if (job.format === 'sql') {
                buffer +=
                    `-- StarbaseDB SQL Dump\n` +
                    `-- Started: ${job.startedAt}\n` +
                    `PRAGMA foreign_keys=OFF;\n` +
                    `BEGIN TRANSACTION;\n\n`
            } else if (job.format === 'json') {
                buffer += '[\n'
            }
        }

        for (; job.tableIndex < job.tables.length; job.tableIndex++) {
            const table = job.tables[job.tableIndex]

            // SQL: include CREATE TABLE statement once per table
            if (job.format === 'sql' && job.lastRowid === 0) {
                const schemaRows = (await this.executeQuery({
                    sql: `SELECT sql FROM sqlite_master WHERE type='table' AND name='${table}';`,
                })) as Array<{ sql: string }>
                if (schemaRows.length && schemaRows[0].sql) {
                    buffer += `-- Table: ${table}\n${schemaRows[0].sql};\n\n`
                }
            }

            let csvHeaders: string[] | null = null

            // Row-fetch loop for this table
            while (true) {
                // Deadline check — persist checkpoint and schedule alarm
                if (Date.now() >= deadline) {
                    if (buffer.length > 0 && this.r2 && job.uploadId) {
                        const part = await this._uploadPart(
                            job,
                            buffer,
                            partNumber
                        )
                        job.r2Parts.push(part)
                        partNumber++
                        buffer = ''
                    }
                    await this._saveJob(job)
                    await this.storage.put(ACTIVE_JOB_KEY, job.jobId)
                    await this.scheduleAlarm(1_000)
                    return false
                }

                const rows = (await this.executeQuery({
                    sql: `SELECT *, rowid AS __rowid FROM "${table}" WHERE rowid > ${job.lastRowid} ORDER BY rowid LIMIT ${job.chunkSize};`,
                })) as Array<Record<string, unknown>>

                if (rows.length === 0) break

                // CSV: write header row once per table (at offset 0)
                if (job.format === 'csv' && job.lastRowid === 0 && rows.length > 0) {
                    csvHeaders = Object.keys(rows[0]).filter((k) => k !== '__rowid')
                    buffer += csvHeaders.join(',') + '\n'
                }
                if (!csvHeaders && rows.length > 0) {
                    csvHeaders = Object.keys(rows[0]).filter((k) => k !== '__rowid')
                }

                for (const row of rows) {
                    // Strip the internal pagination column before serialising
                    const { __rowid, ...cleanRow } = row
                    if (job.format === 'sql') {
                        buffer += rowToInsert(table, cleanRow) + '\n'
                    } else if (job.format === 'csv') {
                        buffer += rowToCSV(cleanRow, csvHeaders!) + '\n'
                    } else {
                        // JSON: comma-separated objects
                        buffer +=
                            (job.processedRows > 0 ? ',\n' : '') +
                            JSON.stringify(cleanRow)
                    }
                    job.processedRows++
                    job.totalRows++
                }

                // Advance keyset cursor to the last rowid in this batch
                const lastRow = rows[rows.length - 1]
                job.lastRowid = lastRow.__rowid as number

                // Flush to R2 when buffer is large enough for a part
                if (
                    this.r2 &&
                    job.uploadId &&
                    buffer.length >= R2_MIN_PART_BYTES
                ) {
                    const part = await this._uploadPart(job, buffer, partNumber)
                    job.r2Parts.push(part)
                    partNumber++
                    buffer = ''
                    await this._saveJob(job)
                }

                // Breathing interval: brief yield so other DO requests can run
                await sleep(BREATHING_INTERVAL_MS)
            }

            job.lastRowid = 0 // reset cursor before advancing to next table
        }

        // All tables done — write footer and finalise
        if (job.format === 'sql') {
            buffer += '\nCOMMIT;\nPRAGMA foreign_keys=ON;\n'
        } else if (job.format === 'json') {
            buffer += '\n]'
        }

        if (this.r2 && job.uploadId) {
            // Upload final (possibly sub-5 MiB) part and complete multipart
            if (buffer.length > 0) {
                const part = await this._uploadPart(job, buffer, partNumber)
                job.r2Parts.push(part)
            }
            const mu = this.r2.resumeMultipartUpload(job.r2Key, job.uploadId)
            await mu.complete(job.r2Parts)
            job.uploadId = undefined
        } else {
            // No R2 binding — persist small dump directly in DO storage
            await this.storage.put(`export_data:${job.jobId}`, buffer)
        }

        job.status = 'completed'
        job.completedAt = new Date().toISOString()
        await this._saveJob(job)
        await this.storage.delete(ACTIVE_JOB_KEY)

        // Fire callback URL via waitUntil so it survives invocation end
        if (job.callbackUrl) {
            this._fireCallback(job)
        }

        return true
        } catch (err: any) {
            job.status = 'failed'
            job.error = err?.message ?? String(err)
            await this._saveJob(job)
            await this.storage.delete(ACTIVE_JOB_KEY)
            // Abort any in-flight R2 multipart upload to avoid orphaned parts
            if (job.uploadId && this.r2) {
                try {
                    await this.r2.resumeMultipartUpload(job.r2Key, job.uploadId).abort()
                } catch { /* ignore */ }
            }
            return false
        }
    }

    // Utilities

    private async _uploadPart(
        job: DumpJobState,
        data: string,
        partNumber: number
    ): Promise<{ partNumber: number; etag: string }> {
        const mu = this.r2!.resumeMultipartUpload(job.r2Key, job.uploadId!)
        const part = await mu.uploadPart(
            partNumber,
            new TextEncoder().encode(data)
        )
        return { partNumber, etag: part.etag }
    }

    private async _buildResponse(job: DumpJobState): Promise<Response> {
        const ct = dumpContentType(job.format)
        const cd = `attachment; filename="${job.r2Key}"`

        if (this.r2) {
            const obj = await this.r2.get(job.r2Key)
            if (!obj) {
                return Response.json(
                    { error: 'Dump file not found in R2' },
                    { status: 404 }
                )
            }
            return new Response(obj.body, {
                headers: { 'Content-Type': ct, 'Content-Disposition': cd },
            })
        }

        // Fallback: small database stored in DO storage (no R2 configured)
        const data = await this.storage.get<string>(
            `export_data:${job.jobId}`
        )
        if (!data) {
            return Response.json({ error: 'Dump data missing' }, { status: 404 })
        }
        return new Response(data, {
            headers: { 'Content-Type': ct, 'Content-Disposition': cd },
        })
    }

    private _fireCallback(job: DumpJobState): void {
        if (!job.callbackUrl) return
        const promise = fetch(job.callbackUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                jobId: job.jobId,
                status: job.status,
                format: job.format,
                totalRows: job.totalRows,
                completedAt: job.completedAt,
                downloadUrl: `/export/dump/download/${job.jobId}`,
                ...(job.error && { error: job.error }),
            }),
        }).catch(() => { /* best-effort */ })
        if (this.ctx) {
            this.ctx.waitUntil(promise)
        }
    }

    private async _saveJob(job: DumpJobState): Promise<void> {
        await this.storage.put(`${JOB_PREFIX}${job.jobId}`, job)
    }

    private async _loadJob(jobId: string): Promise<DumpJobState | null> {
        return (
            (await this.storage.get<DumpJobState>(
                `${JOB_PREFIX}${jobId}`
            )) ?? null
        )
    }
}
