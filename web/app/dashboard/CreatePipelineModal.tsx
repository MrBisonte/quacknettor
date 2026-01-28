'use client';

import { useState } from 'react';

interface CreatePipelineModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSuccess: () => void;
}

export default function CreatePipelineModal({ isOpen, onClose, onSuccess }: CreatePipelineModalProps) {
    const [name, setName] = useState('');
    const [sourceType, setSourceType] = useState('parquet');
    const [targetType, setTargetType] = useState('parquet');
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState<string | null>(null);

    if (!isOpen) return null;

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setIsSubmitting(true);
        setError(null);

        // In a real implementation, we would POST to /api/pipelines/create
        // For now, we'll simulate a success or add a stub endpoint later
        try {
            console.log("Creating pipeline:", { name, sourceType, targetType });
            // Simulate network delay
            await new Promise(resolve => setTimeout(resolve, 800));

            // Mock success
            onSuccess();
            onClose();
        } catch (err) {
            setError('Failed to create pipeline');
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
            <div className="bg-slate-900 border border-white/10 rounded-2xl shadow-2xl w-full max-w-md overflow-hidden animate-in fade-in zoom-in duration-200">
                <div className="p-6 border-b border-white/5 flex justify-between items-center">
                    <h3 className="text-lg font-bold">Create New Pipeline</h3>
                    <button onClick={onClose} className="text-slate-400 hover:text-white transition-colors">
                        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                            <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
                        </svg>
                    </button>
                </div>

                <form onSubmit={handleSubmit} className="p-6 space-y-6">
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-300">Pipeline Name</label>
                        <input
                            type="text"
                            value={name}
                            onChange={(e) => setName(e.target.value)}
                            placeholder="e.g., daily_sales_sync"
                            className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500 transition-all"
                            required
                        />
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                            <label className="text-sm font-medium text-slate-300">Source Type</label>
                            <select
                                value={sourceType}
                                onChange={(e) => setSourceType(e.target.value)}
                                className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg focus:outline-none focus:border-sky-500 transition-all appearance-none"
                            >
                                <option value="parquet">Parquet File</option>
                                <option value="postgres">Postgres</option>
                                <option value="snowflake">Snowflake</option>
                                <option value="csv">CSV File</option>
                            </select>
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium text-slate-300">Target Type</label>
                            <select
                                value={targetType}
                                onChange={(e) => setTargetType(e.target.value)}
                                className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg focus:outline-none focus:border-sky-500 transition-all appearance-none"
                            >
                                <option value="parquet">Parquet</option>
                                <option value="postgres">Postgres</option>
                                <option value="snowflake">Snowflake</option>
                            </select>
                        </div>
                    </div>

                    {error && <div className="text-red-400 text-sm">{error}</div>}

                    <div className="pt-4 flex justify-end space-x-3">
                        <button
                            type="button"
                            onClick={onClose}
                            className="px-4 py-2 rounded-lg text-sm font-medium text-slate-300 hover:text-white hover:bg-white/5 transition-colors"
                        >
                            Cancel
                        </button>
                        <button
                            type="submit"
                            disabled={isSubmitting}
                            className="px-6 py-2 bg-sky-500 hover:bg-sky-400 text-white rounded-lg font-semibold shadow-lg shadow-sky-500/20 transition-all active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            {isSubmitting ? 'Creating...' : 'Create Pipeline'}
                        </button>
                    </div>
                </form>
            </div>
        </div>
    );
}
