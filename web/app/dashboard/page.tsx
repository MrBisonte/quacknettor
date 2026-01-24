'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';

interface Pipeline {
    name: str;
    source_type: str;
    target_type: str;
}

export default function Dashboard() {
    const [pipelines, setPipelines] = useState<Pipeline[]>([]);
    const [loading, setLoading] = useState(true);
    const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
    const [running, setRunning] = useState(false);
    const [jobStatus, setJobStatus] = useState<any>(null);

    const API_BASE = 'http://localhost:8000/api';

    useEffect(() => {
        fetch(`${API_BASE}/pipelines`)
            .then(res => res.json())
            .then(data => {
                setPipelines(data);
                setLoading(false);
            })
            .catch(err => console.error('Failed to fetch pipelines', err));
    }, []);

    const runPipeline = async () => {
        if (!selectedPipeline) return;
        setRunning(true);
        setJobStatus({ status: 'initializing' });

        try {
            const res = await fetch(`${API_BASE}/pipelines/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pipeline_name: selectedPipeline })
            });
            const { job_id } = await res.json();

            // Poll for status
            const poll = setInterval(async () => {
                const statusRes = await fetch(`${API_BASE}/jobs/${job_id}`);
                const statusData = await statusRes.json();
                setJobStatus(statusData);

                if (statusData.status !== 'running') {
                    clearInterval(poll);
                    setRunning(false);
                }
            }, 1000);
        } catch (err) {
            console.error(err);
            setRunning(false);
        }
    };

    return (
        <div className="min-h-screen bg-slate-950 text-slate-100 flex">
            {/* Sidebar */}
            <aside className="w-64 border-r border-white/5 p-6 space-y-8 glass">
                <Link href="/" className="flex items-center space-x-2 text-xl font-bold mb-10">
                    <div className="w-8 h-8 bg-sky-500 rounded-lg flex items-center justify-center text-white text-lg">D</div>
                    <span>DuckEL</span>
                </Link>

                <nav className="space-y-1">
                    <button className="w-full text-left px-4 py-2 bg-white/5 rounded-lg text-sky-400 font-medium">Dashboard</button>
                    <button className="w-full text-left px-4 py-2 hover:bg-white/5 rounded-lg opacity-60 transition-all font-medium">Workspaces</button>
                    <button className="w-full text-left px-4 py-2 hover:bg-white/5 rounded-lg opacity-60 transition-all font-medium">Configurations</button>
                    <button className="w-full text-left px-4 py-2 hover:bg-white/5 rounded-lg opacity-60 transition-all font-medium">Credentials</button>
                </nav>

                <div className="pt-12">
                    <div className="px-4 py-2 text-xs font-bold uppercase tracking-widest opacity-40 mb-4">Integrations</div>
                    <div className="space-y-4 px-4 opacity-60 text-sm font-medium">
                        <div className="flex items-center space-x-2"><span>🐘</span><span>Postgres</span></div>
                        <div className="flex items-center space-x-2"><span>❄️</span><span>Snowflake</span></div>
                        <div className="flex items-center space-x-2"><span>📦</span><span>S3 / Parquet</span></div>
                    </div>
                </div>
            </aside>

            {/* Main Content */}
            <main className="flex-1 p-10 overflow-y-auto">
                <header className="flex justify-between items-center mb-12">
                    <div>
                        <h1 className="text-3xl font-bold tracking-tight">Console Dashboard</h1>
                        <p className="opacity-50 text-sm">Manage and monitor your data orchestration workflows.</p>
                    </div>
                    <div className="flex space-x-3">
                        <button className="px-4 py-2 glass rounded-lg text-sm font-semibold hover:bg-white/5">Settings</button>
                        <button className="px-4 py-2 bg-sky-500 hover:bg-sky-400 rounded-lg text-sm font-semibold shadow-lg shadow-sky-500/20 transition-all text-white">Create Pipeline</button>
                    </div>
                </header>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                    {/* Pipeline List */}
                    <div className="lg:col-span-2 space-y-6">
                        <section className="glass-card p-6">
                            <h2 className="text-lg font-bold mb-6 flex items-center">
                                <span className="w-2 h-2 rounded-full bg-emerald-500 mr-2 animate-pulse" />
                                Available Pipelines
                            </h2>

                            {loading ? (
                                <div className="animate-pulse space-y-4">
                                    <div className="h-12 bg-white/5 rounded" />
                                    <div className="h-12 bg-white/5 rounded" />
                                </div>
                            ) : (
                                <div className="space-y-3">
                                    {pipelines.map(p => (
                                        <div
                                            key={p.name}
                                            onClick={() => setSelectedPipeline(p.name)}
                                            className={`group p-4 rounded-xl border cursor-pointer transition-all flex justify-between items-center ${selectedPipeline === p.name ? 'border-sky-500 bg-sky-500/5' : 'border-white/5 hover:border-white/20'}`}
                                        >
                                            <div className="flex items-center space-x-4">
                                                <div className="w-10 h-10 rounded-lg bg-white/5 flex items-center justify-center text-xl group-hover:scale-110 transition-transform">
                                                    {p.source_type === 'postgres' ? '🐘' : p.source_type === 'snowflake' ? '❄️' : '📦'}
                                                </div>
                                                <div>
                                                    <div className="font-bold text-sm">{p.name}</div>
                                                    <div className="text-xs opacity-40 uppercase tracking-tighter">{p.source_type} ⮕ {p.target_type}</div>
                                                </div>
                                            </div>
                                            <div className="flex items-center space-x-2">
                                                <span className="text-[10px] px-2 py-0.5 bg-emerald-500/10 text-emerald-400 rounded border border-emerald-500/20 font-bold uppercase tracking-widest">Active</span>
                                                <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 opacity-20" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                                                </svg>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </section>

                        {/* Execution Console */}
                        {selectedPipeline && (
                            <section className="glass rounded-2xl overflow-hidden shadow-xl border border-white/5">
                                <div className="bg-slate-900 px-6 py-4 border-b border-white/5 flex justify-between items-center">
                                    <span className="font-mono text-xs opacity-60">Terminal Output — {selectedPipeline}</span>
                                    <button
                                        onClick={runPipeline}
                                        disabled={running}
                                        className={`px-4 py-1.5 rounded-lg text-xs font-bold uppercase tracking-widest transition-all ${running ? 'bg-amber-500/20 text-amber-500 cursor-not-allowed' : 'bg-sky-500 text-white hover:bg-sky-400 shadow-md active:scale-95'}`}
                                    >
                                        {running ? 'Processing...' : 'Run Pipeline'}
                                    </button>
                                </div>
                                <div className="p-6 h-64 font-mono text-xs overflow-y-auto bg-slate-950/80">
                                    {!jobStatus && <div className="text-slate-600">Select run to see execution details...</div>}
                                    {jobStatus && (
                                        <div className="space-y-2">
                                            <div className="text-sky-400 uppercase tracking-widest font-black">[INITIATING] ...</div>
                                            {running && <div className="text-white">Executing engine hooks...</div>}
                                            {jobStatus.status === 'success' && (
                                                <>
                                                    <div className="text-emerald-400 font-bold">SUCCESS: Processed {jobStatus.result.rows} rows in {jobStatus.result.duration_s}s</div>
                                                    <div className="text-slate-500 mt-4 underline decoration-sky-500/30">Execution Metrics:</div>
                                                    <pre className="text-emerald-400/80">{JSON.stringify(jobStatus.result.metrics, null, 2)}</pre>
                                                </>
                                            )}
                                            {jobStatus.status === 'failed' && (
                                                <div className="text-red-400">ERROR: {jobStatus.error}</div>
                                            )}
                                        </div>
                                    )}
                                </div>
                            </section>
                        )}
                    </div>

                    {/* Stats / Sidebar Info */}
                    <div className="space-y-8">
                        <div className="glass-card p-6">
                            <div className="text-xs font-bold uppercase tracking-widest opacity-40 mb-6 font-black tracking-tighter uppercase leading-[1.1]">System Throughput</div>
                            <div className="space-y-6">
                                <div>
                                    <div className="flex justify-between text-sm mb-2"><span className="opacity-60">CPU Usage</span><span className="font-mono font-bold text-sky-400">12%</span></div>
                                    <div className="h-1.5 w-full bg-white/5 rounded-full overflow-hidden"><div className="h-full w-[12%] bg-sky-400 shadow-[0_0_10px_#38bdf8]" /></div>
                                </div>
                                <div>
                                    <div className="flex justify-between text-sm mb-2"><span className="opacity-60">Memory (DuckDB)</span><span className="font-mono font-bold text-indigo-400">420MB</span></div>
                                    <div className="h-1.5 w-full bg-white/5 rounded-full overflow-hidden"><div className="h-full w-[35%] bg-indigo-400 shadow-[0_0_10px_#818cf8]" /></div>
                                </div>
                            </div>
                        </div>

                        <div className="glass-card p-6 bg-gradient-to-br from-indigo-500/10 to-transparent">
                            <div className="text-xs font-bold uppercase tracking-widest opacity-40 mb-4">Recent Activity</div>
                            <div className="space-y-4">
                                {[1, 2, 3].map(i => (
                                    <div key={i} className="flex items-start space-x-3 text-xs border-b border-white/5 pb-3 last:border-0 last:pb-0 font-black tracking-tighter uppercase leading-[1.1]">
                                        <div className="w-2 h-2 rounded-full bg-sky-400 mt-1 shadow-[0_0_5px_#38bdf8]" />
                                        <div>
                                            <div className="opacity-80">Pipeline `sync_users` completed</div>
                                            <div className="opacity-30 mt-1 uppercase leading-[1.1] font-black tracking-tighter uppercase leading-[1.1]">14:2{i} PM</div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </main>
        </div>
    );
}

// Minimal types for strictness
type str = string;
