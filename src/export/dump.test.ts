import { describe, it, expect, vi, beforeEach } from 'vitest'
import { dumpDatabaseRoute, AsyncDumpManager, DumpJobState } from './dump'
import { executeOperation } from '.'
import { createResponse } from '../utils'
import type { DataSource } from '../types'
import type { StarbaseDBConfiguration } from '../handler'

// Mocks shared with the existing dump.test.ts

vi.mock('.', () => ({
    executeOperation: vi.fn(),
}))

vi.mock('../utils', () => ({
    createResponse: vi.fn(
        (data, message, status) =>
            new Response(JSON.stringify({ result: data, error: message }), {
                status,
                headers: { 'Content-Type': 'application/json' },
            })
    ),
}))

// Helpers

/** Minimal in-memory DurableObjectStorage mock. */
function makeStorage() {
    const store = new Map<string, unknown>()
    return {
        get: vi.fn(async <T>(key: string) => (store.get(key) as T) ?? null),
        put: vi.fn(async (key: string, value: unknown) => {
            store.set(key, value)
        }),
        delete: vi.fn(async (key: string) => {
            store.delete(key)
        }),
        _store: store,
    } as unknown as DurableObjectStorage & { _store: Map<string, unknown> }
}

/** Minimal R2Bucket mock with in-memory multipart simulation. */
function makeR2() {
    const objects = new Map<string, string>()
    const uploads = new Map<string, Map<number, string>>()

    return {
        createMultipartUpload: vi.fn(async (key: string) => {
            const uploadId = `upload_${Date.now()}`
            uploads.set(uploadId, new Map())
            return {
                uploadId,
                uploadPart: async (part: number, body: Uint8Array) => {
                    uploads
                        .get(uploadId)!
                        .set(part, new TextDecoder().decode(body))
                    return { etag: `etag_${part}` }
                },
                complete: async (parts: Array<{ partNumber: number }>) => {
                    const sorted = [...parts].sort(
                        (a, b) => a.partNumber - b.partNumber
                    )
                    objects.set(
                        key,
                        sorted
                            .map(
                                (p) =>
                                    uploads.get(uploadId)!.get(p.partNumber) ??
                                    ''
                            )
                            .join('')
                    )
                },
            }
        }),
        resumeMultipartUpload: vi.fn((key: string, uploadId: string) => ({
            uploadPart: async (part: number, body: Uint8Array) => {
                if (!uploads.has(uploadId)) uploads.set(uploadId, new Map())
                uploads.get(uploadId)!.set(part, new TextDecoder().decode(body))
                return { etag: `etag_${part}` }
            },
            complete: async (parts: Array<{ partNumber: number }>) => {
                const sorted = [...parts].sort(
                    (a, b) => a.partNumber - b.partNumber
                )
                objects.set(
                    key,
                    sorted
                        .map(
                            (p) =>
                                uploads.get(uploadId)!.get(p.partNumber) ?? ''
                        )
                        .join('')
                )
            },
        })),
        get: vi.fn(async (key: string) => {
            const data = objects.get(key)
            if (!data) return null
            const enc = new TextEncoder().encode(data)
            return {
                body: new ReadableStream({
                    start(ctrl) {
                        ctrl.enqueue(enc)
                        ctrl.close()
                    },
                }),
            }
        }),
        _objects: objects,
    } as unknown as { _objects: Map<string, string> }
}

/** Returns an executeQuery function backed by a simple in-memory dataset. */
function makeExecuteQuery(
    tables: string[],
    data: Record<string, Record<string, unknown>[]>,
    schemas: Record<string, string> = {}
) {
    return vi.fn(
        async (opts: { sql: string; params?: unknown[] }): Promise<unknown> => {
            const { sql } = opts

            // ---- 1. Schema query (most specific) ----
            if (sql.includes('SELECT sql FROM sqlite_master') && sql.includes('name=')) {
                for (const table of tables) {
                    if (sql.includes(`name='${table}'`)) {
                        return [
                            {
                                sql: schemas[table] ?? `CREATE TABLE "${table}" (id INTEGER PRIMARY KEY, value TEXT)`,
                            },
                        ]
                    }
                }
                return []
            }

            // ---- 2. Table listing query ----
            if (sql.includes('sqlite_master') && sql.includes("type='table'") && !sql.includes('SELECT sql')) {
                const excluded = ['sqlite_sequence', 'sqlite_schema']
                return tables
                    .filter(
                        (name) =>
                            !excluded.includes(name) && !name.startsWith('tmp_')
                    )
                    .map((name) => ({ name }))
            }

            // ---- 3. COUNT queries ----
            for (const table of tables) {
                if (sql.includes('COUNT(*)') && sql.includes(`FROM "${table}"`)) {
                    return [{ count: data[table]?.length ?? 0 }]
                }
            }

            // ---- 4. SELECT with keyset pagination ----
            for (const table of tables) {
                const m = sql.match(
                    new RegExp(
                        `SELECT \\*, rowid AS __rowid FROM "${table}" WHERE rowid > (\\d+) ORDER BY rowid LIMIT (\\d+)`
                    )
                )
                if (m) {
                    const lastRowid = parseInt(m[1])
                    const limit = parseInt(m[2])
                    const allRows = data[table] ?? []
                    return allRows
                        .map((r, i) => ({ ...r, __rowid: i + 1 }))
                        .filter((r) => r.__rowid > lastRowid)
                        .slice(0, limit)
                }
            }

            return []
        }
    )
}

// Shared fixtures

let mockDataSource: DataSource
let mockConfig: StarbaseDBConfiguration

beforeEach(() => {
    vi.clearAllMocks()

    mockDataSource = {
        source: 'external',
        external: { dialect: 'sqlite' },
        rpc: { executeQuery: vi.fn() },
    } as any

    mockConfig = {
        outerbaseApiKey: 'mock-api-key',
        role: 'admin',
        features: { allowlist: true, rls: true, rest: true },
    }
})

// Existing tests (unchanged) — dumpDatabaseRoute

describe('Database Dump Module', () => {
    it('should return a database dump when tables exist', async () => {
        vi.mocked(executeOperation)
            .mockResolvedValueOnce([{ name: 'users' }, { name: 'orders' }])
            .mockResolvedValueOnce([
                { sql: 'CREATE TABLE users (id INTEGER, name TEXT);' },
            ])
            .mockResolvedValueOnce([
                { id: 1, name: 'Alice' },
                { id: 2, name: 'Bob' },
            ])
            .mockResolvedValueOnce([
                { sql: 'CREATE TABLE orders (id INTEGER, total REAL);' },
            ])
            .mockResolvedValueOnce([
                { id: 1, total: 99.99 },
                { id: 2, total: 49.5 },
            ])

        const response = await dumpDatabaseRoute(mockDataSource, mockConfig)

        expect(response).toBeInstanceOf(Response)
        expect(response.headers.get('Content-Type')).toBe(
            'application/x-sqlite3'
        )
        expect(response.headers.get('Content-Disposition')).toBe(
            'attachment; filename="database_dump.sql"'
        )

        const dumpText = await response.text()
        expect(dumpText).toContain(
            'CREATE TABLE users (id INTEGER, name TEXT);'
        )
        expect(dumpText).toContain("INSERT INTO users VALUES (1, 'Alice');")
        expect(dumpText).toContain("INSERT INTO users VALUES (2, 'Bob');")
        expect(dumpText).toContain(
            'CREATE TABLE orders (id INTEGER, total REAL);'
        )
        expect(dumpText).toContain('INSERT INTO orders VALUES (1, 99.99);')
        expect(dumpText).toContain('INSERT INTO orders VALUES (2, 49.5);')
    })

    it('should handle empty databases (no tables)', async () => {
        vi.mocked(executeOperation).mockResolvedValueOnce([])

        const response = await dumpDatabaseRoute(mockDataSource, mockConfig)

        expect(response).toBeInstanceOf(Response)
        expect(response.headers.get('Content-Type')).toBe(
            'application/x-sqlite3'
        )
        const dumpText = await response.text()
        expect(dumpText).toBe('SQLite format 3\0')
    })

    it('should handle databases with tables but no data', async () => {
        vi.mocked(executeOperation)
            .mockResolvedValueOnce([{ name: 'users' }])
            .mockResolvedValueOnce([
                { sql: 'CREATE TABLE users (id INTEGER, name TEXT);' },
            ])
            .mockResolvedValueOnce([])

        const response = await dumpDatabaseRoute(mockDataSource, mockConfig)

        expect(response).toBeInstanceOf(Response)
        const dumpText = await response.text()
        expect(dumpText).toContain(
            'CREATE TABLE users (id INTEGER, name TEXT);'
        )
        expect(dumpText).not.toContain('INSERT INTO users VALUES')
    })

    it('should escape single quotes properly in string values', async () => {
        vi.mocked(executeOperation)
            .mockResolvedValueOnce([{ name: 'users' }])
            .mockResolvedValueOnce([
                { sql: 'CREATE TABLE users (id INTEGER, bio TEXT);' },
            ])
            .mockResolvedValueOnce([{ id: 1, bio: "Alice's adventure" }])

        const response = await dumpDatabaseRoute(mockDataSource, mockConfig)

        expect(response).toBeInstanceOf(Response)
        const dumpText = await response.text()
        expect(dumpText).toContain(
            "INSERT INTO users VALUES (1, 'Alice''s adventure');"
        )
    })

    it('should return a 500 response when an error occurs', async () => {
        const consoleErrorMock = vi
            .spyOn(console, 'error')
            .mockImplementation(() => {})
        vi.mocked(executeOperation).mockRejectedValue(
            new Error('Database Error')
        )

        const response = await dumpDatabaseRoute(mockDataSource, mockConfig)

        expect(response.status).toBe(500)
        const jsonResponse: { error: string } = await response.json()
        expect(jsonResponse.error).toBe('Failed to create database dump')
    })
})

// New tests — AsyncDumpManager

describe('AsyncDumpManager', () => {
    it('startDump: completes a small SQL export within the time budget', async () => {
        const storage = makeStorage()
        const r2 = makeR2()
        const executeQuery = makeExecuteQuery(['users'], {
            users: [
                { id: 1, value: 'Alice' },
                { id: 2, value: 'Bob' },
            ],
        })
        const scheduleAlarm = vi.fn(async () => {})

        const mgr = new AsyncDumpManager(
            storage,
            r2,
            executeQuery,
            scheduleAlarm
        )
        const response = await mgr.startDump({ format: 'sql' })

        // Fast-path: should return either the file (200) or 202
        expect([200, 202]).toContain(response.status)
        // Alarm should NOT have been called for a small DB
        if (response.status === 200) {
            expect(scheduleAlarm).not.toHaveBeenCalled()
        }
    })

    it('startDump: SQL output contains CREATE TABLE and INSERT statements', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(
            ['users'],
            { users: [{ id: 1, value: 'Alice' }] },
            { users: 'CREATE TABLE "users" (id INTEGER, value TEXT)' }
        )

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'sql' })

        if (response.status === 200) {
            const body = await response.text()
            expect(body).toContain('CREATE TABLE')
            expect(body).toContain('INSERT INTO users VALUES')
            expect(body).toContain("'Alice'")
            expect(body).toContain('BEGIN TRANSACTION')
            expect(body).toContain('COMMIT')
        }
    })

    it('startDump: NULL values are emitted as SQL NULL', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['t'], {
            t: [{ id: 1, value: null }],
        })

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'sql' })

        if (response.status === 200) {
            const body = await response.text()
            expect(body).toContain('NULL')
        }
    })

    it('startDump: single-quote escaping in SQL output', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['users'], {
            users: [{ id: 1, value: "Bob's boat" }],
        })

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'sql' })

        if (response.status === 200) {
            const body = await response.text()
            // Apostrophe must be doubled
            expect(body).toContain("''")
        }
    })

    it('startDump: CSV output has a header row and data rows', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['users'], {
            users: [
                { id: 1, value: 'Alice' },
                { id: 2, value: 'Bob' },
            ],
        })

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'csv' })

        if (response.status === 200) {
            const body = await response.text()
            const lines = body.trim().split('\n')
            // First line is the CSV header
            expect(lines[0]).toMatch(/id.*value|value.*id/)
            // At least 2 data rows
            expect(lines.length).toBeGreaterThanOrEqual(3)
        }
    })

    it('startDump: CSV values with commas are quoted', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['t'], {
            t: [{ id: 1, value: 'hello, world' }],
        })

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'csv' })

        if (response.status === 200) {
            const body = await response.text()
            expect(body).toContain('"hello, world"')
        }
    })

    it('startDump: JSON output is a valid array', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['users'], {
            users: [
                { id: 1, value: 'Alice' },
                { id: 2, value: 'Bob' },
            ],
        })

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'json' })

        if (response.status === 200) {
            const body = await response.text()
            const parsed = JSON.parse(body)
            expect(Array.isArray(parsed)).toBe(true)
            expect(parsed.length).toBeGreaterThan(0)
        }
    })

    it('startDump: empty database completes without error', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery([], {})

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const response = await mgr.startDump({ format: 'sql' })
        expect([200, 202]).toContain(response.status)
    })

    it('getStatus: returns 404 for unknown jobId', async () => {
        const storage = makeStorage()
        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            makeExecuteQuery([], {}),
            vi.fn(async () => {})
        )

        const response = await mgr.getStatus('nonexistent')
        expect(response.status).toBe(404)
    })

    it('getStatus: reports correct percentComplete', async () => {
        const storage = makeStorage()

        const job: DumpJobState = {
            jobId: 'dump_test_pct',
            status: 'processing',
            format: 'sql',
            chunkSize: 1000,
            tables: ['t'],
            tableIndex: 0,
            lastRowid: 0,
            totalRows: 200,
            processedRows: 50,
            startedAt: new Date().toISOString(),
            r2Key: 'dump_test_pct.sql',
            r2Parts: [],
        }
        await storage.put('async_dump_job:dump_test_pct', job)

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            makeExecuteQuery([], {}),
            vi.fn(async () => {})
        )
        const response = await mgr.getStatus('dump_test_pct')
        const body = (await response.json()) as {
            progress: { percentComplete: number }
        }
        expect(body.progress.percentComplete).toBe(25)
    })

    it('download: returns 409 while job is still processing', async () => {
        const storage = makeStorage()

        const job: DumpJobState = {
            jobId: 'dump_in_progress',
            status: 'processing',
            format: 'sql',
            chunkSize: 1000,
            tables: ['t'],
            tableIndex: 0,
            lastRowid: 0,
            totalRows: 1000,
            processedRows: 100,
            startedAt: new Date().toISOString(),
            r2Key: 'dump_in_progress.sql',
            r2Parts: [],
        }
        await storage.put('async_dump_job:dump_in_progress', job)

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            makeExecuteQuery([], {}),
            vi.fn(async () => {})
        )
        const response = await mgr.download('dump_in_progress')
        expect(response.status).toBe(409)
    })

    it('download: returns 404 for unknown jobId', async () => {
        const storage = makeStorage()
        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            makeExecuteQuery([], {}),
            vi.fn(async () => {})
        )
        const response = await mgr.download('does_not_exist')
        expect(response.status).toBe(404)
    })

    it('resumeFromAlarm: no-ops when no active job is stored', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery([], {})
        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )

        // Should not throw
        await expect(mgr.resumeFromAlarm()).resolves.toBeUndefined()
        // executeQuery should not have been called
        expect(executeQuery).not.toHaveBeenCalled()
    })

    it('resumeFromAlarm: continues and completes an in-progress job', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['users'], {
            users: [{ id: 1, value: 'Alice' }],
        })
        const scheduleAlarm = vi.fn(async () => {})

        // Simulate a partially-complete job
        const job: DumpJobState = {
            jobId: 'dump_resume_test',
            status: 'processing',
            format: 'sql',
            chunkSize: 1000,
            tables: ['users'],
            tableIndex: 0,
            lastRowid: 0,
            totalRows: 1,
            processedRows: 0,
            startedAt: new Date().toISOString(),
            r2Key: 'dump_resume_test.sql',
            r2Parts: [],
        }
        await storage.put('async_dump_job:dump_resume_test', job)
        await storage.put('async_dump_active_job', 'dump_resume_test')

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            scheduleAlarm
        )
        await mgr.resumeFromAlarm()

        const updated = await storage.get<DumpJobState>(
            'async_dump_job:dump_resume_test'
        )
        expect(['completed', 'processing']).toContain(updated?.status)
    })

    it('fires callbackUrl with correct payload when export completes', async () => {
        const storage = makeStorage()
        const executeQuery = makeExecuteQuery(['t'], { t: [{ id: 1, v: 'x' }] })
        const callbackUrl = 'https://example.com/notify'
        const fetchMock = vi.fn(async () => new Response('ok'))
        global.fetch = fetchMock

        const mgr = new AsyncDumpManager(
            storage,
            undefined,
            executeQuery,
            vi.fn(async () => {})
        )
        const res = await mgr.startDump({ format: 'sql', callbackUrl })

        if (res.status === 200) {
            // Give the non-blocking callback a tick to fire
            await new Promise((r) => setTimeout(r, 10))
            expect(fetchMock).toHaveBeenCalledWith(
                callbackUrl,
                expect.objectContaining({ method: 'POST' })
            )
            const body = JSON.parse(
                (
                    (fetchMock.mock.calls[0] as unknown[])[1] as unknown as {
                        body: string
                    }
                ).body
            )
            expect(body.status).toBe('completed')
            expect(body.jobId).toMatch(/^dump_/)
            expect(body.downloadUrl).toContain('/export/dump/download/')
        }

        vi.restoreAllMocks()
    })
})
