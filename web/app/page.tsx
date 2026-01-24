import Link from 'next/link';

export default function LandingPage() {
  return (
    <div className="min-h-screen hero-gradient">
      {/* Navigation */}
      <nav className="container mx-auto px-6 py-8 flex justify-between items-center relative z-10">
        <div className="flex items-center space-x-2">
          <div className="w-10 h-10 bg-gradient-to-tr from-sky-400 to-indigo-500 rounded-xl flex items-center justify-center font-bold text-white text-2xl">
            D
          </div>
          <span className="text-2xl font-bold tracking-tight">DuckEL</span>
        </div>
        <div className="hidden md:flex space-x-8 text-sm font-medium opacity-80">
          <a href="#features" className="hover:text-sky-400 transition-colors">Features</a>
          <a href="#pipelines" className="hover:text-sky-400 transition-colors">Pipelines</a>
          <a href="#docs" className="hover:text-sky-400 transition-colors">Docs</a>
        </div>
        <Link href="/dashboard" className="px-6 py-2.5 bg-sky-500 hover:bg-sky-400 text-white rounded-full font-semibold transition-all shadow-lg shadow-sky-500/20 active:scale-95">
          Launch Console
        </Link>
      </nav>

      {/* Hero Section */}
      <main className="container mx-auto px-6 pt-20 pb-32 text-center relative">
        <div className="absolute top-0 left-1/2 -translate-x-1/2 w-full max-w-4xl h-full hero-gradient pointer-events-none" />

        <div className="inline-flex items-center space-x-2 px-3 py-1 bg-sky-500/10 border border-sky-500/20 rounded-full mb-8">
          <span className="flex h-2 w-2 rounded-full bg-sky-400 animate-pulse" />
          <span className="text-xs font-semibold text-sky-400 uppercase tracking-widest">v2.0 Agentic Edition</span>
        </div>

        <h1 className="text-6xl md:text-8xl font-black tracking-tighter mb-8 leading-[1.1]">
          Orchestrate Data <br />
          <span className="gradient-text">At The Speed Of Logic.</span>
        </h1>

        <p className="max-w-2xl mx-auto text-xl text-slate-400 mb-12 leading-relaxed">
          The next-generation EL engine powered by DuckDB.
          Blazing fast, agent-ready, and designed for local-first data engineering.
        </p>

        <div className="flex flex-col md:flex-row justify-center items-center space-y-4 md:space-y-0 md:space-x-6">
          <Link href="/dashboard" className="w-full md:w-auto px-10 py-4 bg-white text-slate-950 rounded-xl font-bold text-lg hover:bg-sky-50 transition-all active:scale-95">
            Build a Pipeline
          </Link>
          <a href="#demo" className="w-full md:w-auto px-10 py-4 glass text-white rounded-xl font-bold text-lg hover:bg-white/5 transition-all active:scale-95">
            View Live Demo
          </a>
        </div>

        {/* Console Preview */}
        <div className="mt-24 max-w-5xl mx-auto glass rounded-2xl overflow-hidden shadow-2xl border-white/5 p-1">
          <div className="bg-slate-950/50 rounded-xl p-4 md:p-8">
            <div className="flex items-center space-x-2 mb-6 border-b border-white/5 pb-4">
              <div className="flex space-x-1.5">
                <div className="w-3 h-3 rounded-full bg-red-500/50" />
                <div className="w-3 h-3 rounded-full bg-amber-500/50" />
                <div className="w-3 h-3 rounded-full bg-emerald-500/50" />
              </div>
              <div className="bg-white/5 rounded px-4 py-1 text-xs font-mono opacity-50 flex-1 text-center">
                duckel.cloud/console/p_77a281
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8 text-left">
              <div className="space-y-4">
                <div className="h-4 w-1/3 bg-white/10 rounded" />
                <div className="h-10 bg-white/5 rounded-lg border border-white/10" />
                <div className="h-4 w-1/2 bg-white/10 rounded" />
                <div className="h-10 bg-white/5 rounded-lg border border-white/10" />
              </div>
              <div className="space-y-4">
                <div className="h-full min-h-[120px] bg-sky-500/5 rounded-lg border border-sky-500/10 flex items-center justify-center">
                  <span className="text-sky-400 font-mono text-sm">Streaming 50,000 rows/s...</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>

      {/* Feature Grid */}
      <section id="features" className="container mx-auto px-6 py-24 border-t border-white/5">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          <div className="glass-card p-8">
            <div className="w-12 h-12 bg-sky-500/10 rounded-lg flex items-center justify-center mb-6 text-sky-400">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
            </div>
            <h3 className="text-xl font-bold mb-3">Sub-Second Delivery</h3>
            <p className="text-slate-400 text-sm leading-relaxed">
              Leverage the power of DuckDB to process millions of rows locally without the overhead of heavy cloud JVMs.
            </p>
          </div>

          <div className="glass-card p-8">
            <div className="w-12 h-12 bg-indigo-500/10 rounded-lg flex items-center justify-center mb-6 text-indigo-400">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
            <h3 className="text-xl font-bold mb-3">Modern UI Stack</h3>
            <p className="text-slate-400 text-sm leading-relaxed">
              Built with Next.js 15 and FastAPI for a reactive, low-latency experience that Streamlit simply can't match.
            </p>
          </div>

          <div className="glass-card p-8">
            <div className="w-12 h-12 bg-pink-500/10 rounded-lg flex items-center justify-center mb-6 text-pink-400">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
              </svg>
            </div>
            <h3 className="text-xl font-bold mb-3">Agentic Pipeline Synthesis</h3>
            <p className="text-slate-400 text-sm leading-relaxed">
              Built-in "Jules" integration allows you to describe your data needs in natural language and get working code.
            </p>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="container mx-auto px-6 py-12 border-t border-white/5 opacity-50 flex justify-between items-center text-xs">
        <div>© 2026 DuckEL Engine. Local-first, Logic-fast.</div>
        <div className="flex space-x-6">
          <a href="#" className="hover:text-white transition-colors">Twitter</a>
          <a href="#" className="hover:text-white transition-colors">GitHub</a>
          <a href="#" className="hover:text-white transition-colors">Privacy</a>
        </div>
      </footer>
    </div>
  );
}
