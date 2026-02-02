// ==UserScript==
// @name         Telegram Download Manager Pro v20
// @namespace    https://telegram-download-manager
// @version      20.0
// @description  Perfect queue management - skip complete, replace incomplete
// @match        https://web.telegram.org/*
// @match        https://webk.telegram.org/*
// @match        https://webz.telegram.org/*
// @grant        unsafeWindow
// @run-at       document-idle
// ==/UserScript==

(function () {
    'use strict';

    // ==================== CONFIG ====================
    const CONFIG = {
        MAX_CONCURRENT: 2,
        SCAN_INTERVAL: 800,
        NAV_DELAY: 600,
        STORAGE_KEY: 'tdm_v20',
        REFRESH_DELAY: 500,
    };

    const contentRangeRegex = /^bytes (\d+)-(\d+)\/(\d+)$/;

    // ==================== STATE ====================
    const State = {
        isScanning: false,
        folderHandle: null,
        folderName: '',
        folderFiles: new Map(),           // filename.lower -> { name, size, modified }
        queue: [],                         // Current session queue
        activeDownloads: new Map(),        // id -> { abort }
        processedUrls: new Set(),          // URLs processed this session

        // PERSISTENT: Track all downloaded files with their expected sizes
        downloadedFiles: new Map(),        // filename.lower -> { size, url, ts, complete }

        history: {
            completed: [],    // Successfully downloaded this session
            skipped: [],      // Skipped (already complete)
            replaced: [],     // Re-downloaded (was incomplete)
            failed: []
        },

        speedHistory: [],
        logs: [],
        settings: { direction: 'right', skipExisting: true, skipComplete: true },
        stats: { completed: 0, skipped: 0, replaced: 0, failed: 0, totalBytes: 0, scanned: 0 },
        lastRender: 0,
        renderScheduled: false,
        panelSize: { width: 1050, height: 780 },
        activeTab: 'queue',
        catalogueFilter: 'all',
        catalogueSearch: '',
        lastMediaUrl: null,
    };

    // ==================== STORAGE ====================
    function loadData() {
        try {
            const saved = localStorage.getItem(CONFIG.STORAGE_KEY);
            if (saved) {
                const data = JSON.parse(saved);
                Object.assign(State.settings, data.settings || {});
                Object.assign(State.stats, data.stats || {});
                State.panelSize = data.panelSize || State.panelSize;

                // Restore downloaded files registry
                if (data.downloadedFiles) {
                    State.downloadedFiles = new Map(data.downloadedFiles);
                }

                // Restore history (limited)
                if (data.history) {
                    State.history.completed = (data.history.completed || []).slice(0, 200);
                    State.history.skipped = (data.history.skipped || []).slice(0, 100);
                    State.history.replaced = (data.history.replaced || []).slice(0, 100);
                    State.history.failed = (data.history.failed || []).slice(0, 50);
                }
            }
        } catch (e) { console.error('[TDM] Load error:', e); }
    }

    function saveData() {
        try {
            localStorage.setItem(CONFIG.STORAGE_KEY, JSON.stringify({
                settings: State.settings,
                stats: State.stats,
                panelSize: State.panelSize,
                downloadedFiles: Array.from(State.downloadedFiles.entries()).slice(-1000),
                history: {
                    completed: State.history.completed.slice(0, 200),
                    skipped: State.history.skipped.slice(0, 100),
                    replaced: State.history.replaced.slice(0, 100),
                    failed: State.history.failed.slice(0, 50),
                }
            }));
        } catch (e) { console.error('[TDM] Save error:', e); }
    }

    // ==================== STYLES ====================
    const CSS = `
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap');

        :root {
            --bg1:#08080c;--bg2:#0c0c12;--bg3:#101018;--bgh:#16161f;
            --bdr:#1a1a28;--txt:#e2e8f0;--txt2:#94a3b8;--txt3:#64748b;
            --blue:#3b82f6;--cyan:#22d3ee;--green:#10b981;--yellow:#fbbf24;
            --red:#f87171;--purple:#a78bfa;--orange:#fb923c;--pink:#f472b6;
        }

        *{box-sizing:border-box}

        #tdm-panel{
            position:fixed;top:20px;left:50%;transform:translateX(-50%);
            width:1050px;height:780px;min-width:800px;min-height:600px;
            background:var(--bg1);border:1px solid var(--bdr);border-radius:14px;
            z-index:999999;font-family:'Inter',-apple-system,sans-serif;color:var(--txt);
            box-shadow:0 30px 100px rgba(0,0,0,0.9);display:none;flex-direction:column;overflow:hidden;
        }

        .rh{position:absolute;z-index:10}
        .rh-n{top:-5px;left:10px;right:10px;height:10px;cursor:n-resize}
        .rh-s{bottom:-5px;left:10px;right:10px;height:10px;cursor:s-resize}
        .rh-e{right:-5px;top:10px;bottom:10px;width:10px;cursor:e-resize}
        .rh-w{left:-5px;top:10px;bottom:10px;width:10px;cursor:w-resize}
        .rh-ne{top:-5px;right:-5px;width:15px;height:15px;cursor:ne-resize}
        .rh-nw{top:-5px;left:-5px;width:15px;height:15px;cursor:nw-resize}
        .rh-se{bottom:-5px;right:-5px;width:15px;height:15px;cursor:se-resize}
        .rh-sw{bottom:-5px;left:-5px;width:15px;height:15px;cursor:sw-resize}

        .hdr{background:linear-gradient(135deg,#1e40af,#7c3aed);padding:12px 16px;display:flex;justify-content:space-between;align-items:center;cursor:move;flex-shrink:0}
        .hdr-left{display:flex;align-items:center;gap:10px}
        .hdr h3{margin:0;font-size:14px;font-weight:700}
        .hdr small{font-size:10px;opacity:0.7;margin-left:8px}
        .hdr-btns{display:flex;gap:6px}
        .hdr-btn{background:rgba(255,255,255,0.15);border:none;color:#fff;width:26px;height:26px;border-radius:6px;cursor:pointer;font-size:14px}
        .hdr-btn:hover{background:rgba(255,255,255,0.25)}

        .bar{display:flex;gap:8px;padding:10px 16px;background:var(--bg2);border-bottom:1px solid var(--bdr);flex-wrap:wrap;align-items:center;flex-shrink:0}

        .folder-box{display:flex;align-items:center;gap:10px;padding:8px 12px;background:rgba(59,130,246,0.1);border:1px dashed var(--blue);border-radius:8px;cursor:pointer;flex:1;min-width:180px;max-width:280px}
        .folder-box:hover{background:rgba(59,130,246,0.15)}
        .folder-box.ok{border-style:solid;border-color:var(--green);background:rgba(16,185,129,0.1)}
        .folder-icon{font-size:18px}
        .folder-info{flex:1;min-width:0}
        .folder-name{font-size:12px;font-weight:600;color:var(--blue);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .folder-box.ok .folder-name{color:var(--green)}
        .folder-meta{font-size:10px;color:var(--txt3)}

        .btn{padding:7px 12px;border-radius:6px;border:none;font-size:11px;font-weight:600;cursor:pointer;display:flex;align-items:center;gap:4px;transition:all 0.15s;white-space:nowrap}
        .btn:disabled{opacity:0.4;cursor:not-allowed}
        .btn:hover:not(:disabled){transform:translateY(-1px);filter:brightness(1.1)}
        .btn-g{background:var(--green);color:#000}
        .btn-r{background:var(--red);color:#fff}
        .btn-y{background:var(--yellow);color:#000}
        .btn-x{background:var(--bg3);color:var(--txt2);border:1px solid var(--bdr)}

        .settings-group{display:flex;align-items:center;gap:10px;padding:4px 10px;background:var(--bg3);border-radius:6px;border:1px solid var(--bdr)}
        .setting-item{display:flex;align-items:center;gap:4px;font-size:10px;color:var(--txt2)}
        .setting-item input[type="checkbox"]{width:14px;height:14px;accent-color:var(--green)}
        .setting-item label{cursor:pointer;user-select:none}

        .dir-sel{display:flex;align-items:center;gap:4px;padding:3px;background:var(--bg3);border-radius:6px;border:1px solid var(--bdr)}
        .dir-btn{padding:5px 10px;border:none;border-radius:4px;cursor:pointer;font-size:10px;font-weight:600;background:transparent;color:var(--txt3)}
        .dir-btn.on{background:var(--blue);color:#fff}
        .dir-btn:hover:not(.on){background:var(--bgh);color:var(--txt)}

        .scan-status{display:flex;align-items:center;gap:8px;padding:4px 12px;background:var(--bg3);border-radius:6px;border:1px solid var(--bdr);font-size:11px;font-family:'JetBrains Mono',monospace}
        .scan-status .label{color:var(--txt3)}
        .scan-status .value{font-weight:600}
        .scan-status .value.found{color:var(--green)}
        .scan-status .value.skipped{color:var(--purple)}
        .scan-status .value.replaced{color:var(--orange)}

        .stats{display:flex;gap:4px;padding:8px 16px;background:var(--bg2);border-bottom:1px solid var(--bdr);flex-shrink:0}
        .stat{flex:1;text-align:center;padding:8px 6px;background:var(--bg3);border-radius:6px;cursor:default}
        .stat:hover{background:var(--bgh)}
        .stat-v{font-size:18px;font-weight:700;font-family:'JetBrains Mono',monospace}
        .stat-l{font-size:9px;color:var(--txt3);text-transform:uppercase;margin-top:2px}
        .s-cyan .stat-v{color:var(--cyan)}
        .s-green .stat-v{color:var(--green)}
        .s-yellow .stat-v{color:var(--yellow)}
        .s-red .stat-v{color:var(--red)}
        .s-purple .stat-v{color:var(--purple)}
        .s-blue .stat-v{color:var(--blue)}
        .s-orange .stat-v{color:var(--orange)}

        .tabs{display:flex;gap:4px;padding:8px 16px;background:var(--bg2);border-bottom:1px solid var(--bdr);flex-shrink:0}
        .tab{padding:8px 16px;border:none;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;background:var(--bg3);color:var(--txt3);transition:all 0.15s;display:flex;align-items:center;gap:6px}
        .tab:hover{background:var(--bgh);color:var(--txt)}
        .tab.active{background:var(--blue);color:#fff}
        .tab .badge{background:rgba(255,255,255,0.2);padding:2px 6px;border-radius:10px;font-size:10px}
        .tab.active .badge{background:rgba(255,255,255,0.3)}

        .active-sec{background:linear-gradient(180deg,rgba(34,211,238,0.08),rgba(34,211,238,0.02));border-bottom:1px solid rgba(34,211,238,0.2);flex-shrink:0;display:none;max-height:280px;overflow:hidden}
        .active-hdr{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;border-bottom:1px solid rgba(34,211,238,0.1)}
        .active-title{font-size:12px;font-weight:600;color:var(--cyan);display:flex;align-items:center;gap:8px}
        .active-title::before{content:'';width:8px;height:8px;background:var(--cyan);border-radius:50%;animation:pulse 1.5s infinite}
        @keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(34,211,238,0.5);opacity:1}50%{box-shadow:0 0 10px 5px rgba(34,211,238,0);opacity:0.6}}
        .active-speed{font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:700;color:var(--green);background:rgba(16,185,129,0.15);padding:4px 14px;border-radius:20px}
        .active-list{padding:10px 16px;max-height:200px;overflow-y:auto}
        .active-list::-webkit-scrollbar{width:4px}
        .active-list::-webkit-scrollbar-thumb{background:var(--cyan);border-radius:2px}

        .dl-card{background:linear-gradient(135deg,rgba(34,211,238,0.08),rgba(59,130,246,0.04));border:1px solid rgba(34,211,238,0.2);border-radius:10px;padding:12px;margin-bottom:8px}
        .dl-card:last-child{margin-bottom:0}
        .dl-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
        .dl-info{flex:1;min-width:0}
        .dl-name{font-size:12px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:6px;color:#fff}
        .dl-meta{display:flex;flex-wrap:wrap;gap:12px;font-size:10px;font-family:'JetBrains Mono',monospace}
        .dl-m{display:flex;align-items:center;gap:4px;color:var(--txt2)}
        .dl-m b{font-weight:600}
        .dl-m.sz b{color:var(--cyan)}
        .dl-m.sp b{color:var(--green)}
        .dl-m.et b{color:var(--yellow)}
        .dl-m.wr b{color:var(--orange)}
        .dl-cancel{background:rgba(248,113,113,0.2);border:none;color:var(--red);width:26px;height:26px;border-radius:6px;cursor:pointer;font-size:13px;flex-shrink:0}
        .dl-cancel:hover{background:rgba(248,113,113,0.3)}
        .dl-prog{margin-top:8px}
        .prog-bar{height:8px;background:rgba(0,0,0,0.5);border-radius:4px;overflow:hidden}
        .prog-fill{height:100%;background:linear-gradient(90deg,var(--green),var(--cyan));border-radius:4px;transition:width 0.15s}
        .prog-info{display:flex;justify-content:space-between;font-size:9px;font-family:'JetBrains Mono',monospace;margin-top:4px;color:var(--txt3)}
        .prog-info .pct{color:var(--cyan);font-weight:600;font-size:11px}

        .speed-sec{padding:8px 16px;background:var(--bg2);border-bottom:1px solid var(--bdr);display:none;flex-shrink:0}
        .speed-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
        .speed-lbl{font-size:10px;color:var(--txt3);text-transform:uppercase}
        .speed-val{font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:var(--green)}
        .speed-graph{height:30px;display:flex;align-items:flex-end;gap:2px;background:rgba(0,0,0,0.4);border-radius:4px;padding:3px}
        .sp-bar{flex:1;background:linear-gradient(180deg,var(--green),#059669);border-radius:2px;min-height:2px;transition:height 0.15s}

        .tab-content{flex:1;overflow:hidden;display:none;flex-direction:column}
        .tab-content.active{display:flex}

        .content-body{flex:1;overflow-y:auto;padding:12px 16px}
        .content-body::-webkit-scrollbar{width:6px}
        .content-body::-webkit-scrollbar-thumb{background:var(--blue);border-radius:3px}

        .cols{display:flex;flex:1;overflow:hidden}
        .col{flex:1;display:flex;flex-direction:column;border-right:1px solid var(--bdr);min-width:0}
        .col:last-child{border-right:none}
        .col-hdr{padding:8px 12px;background:var(--bg2);border-bottom:1px solid var(--bdr);display:flex;justify-content:space-between;align-items:center;flex-shrink:0}
        .col-t{font-size:11px;font-weight:600;color:var(--txt2);text-transform:uppercase;display:flex;align-items:center;gap:6px}
        .col-c{font-size:10px;color:var(--txt3);font-family:'JetBrains Mono',monospace;background:var(--bg3);padding:2px 8px;border-radius:10px}
        .col-body{flex:1;overflow-y:auto;padding:6px}
        .col-body::-webkit-scrollbar{width:4px}
        .col-body::-webkit-scrollbar-thumb{background:var(--blue);border-radius:2px}

        .li{display:flex;align-items:center;gap:8px;padding:8px 10px;margin-bottom:4px;background:var(--bg2);border-radius:6px;border-left:3px solid var(--bdr);font-size:11px}
        .li:hover{background:var(--bgh)}
        .li.pending{border-left-color:var(--yellow)}
        .li.completed{border-left-color:var(--green)}
        .li.skipped{border-left-color:var(--purple)}
        .li.replaced{border-left-color:var(--orange)}
        .li.failed{border-left-color:var(--red)}
        .li.complete{border-left-color:var(--green)}
        .li.incomplete{border-left-color:var(--orange)}
        .li.folder{border-left-color:var(--blue)}
        .li-i{font-size:13px;flex-shrink:0}
        .li-info{flex:1;min-width:0}
        .li-n{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:500}
        .li-m{font-size:9px;color:var(--txt3);margin-top:2px;font-family:'JetBrains Mono',monospace}
        .li-status{font-size:9px;padding:2px 6px;border-radius:4px;font-weight:600;flex-shrink:0}
        .li-status.complete{background:rgba(16,185,129,0.2);color:var(--green)}
        .li-status.incomplete{background:rgba(251,146,60,0.2);color:var(--orange)}
        .li-status.in-folder{background:rgba(59,130,246,0.2);color:var(--blue)}
        .li-status.in-queue{background:rgba(251,191,36,0.2);color:var(--yellow)}
        .li-status.unknown{background:rgba(148,163,184,0.2);color:var(--txt2)}
        .li-acts{display:flex;gap:3px;flex-shrink:0}
        .li-btn{width:22px;height:22px;border:none;border-radius:4px;cursor:pointer;font-size:10px;display:flex;align-items:center;justify-content:center}
        .li-btn.dl{background:var(--green);color:#000}
        .li-btn.dl:disabled{background:var(--bg3);opacity:0.4}
        .li-btn.rm{background:var(--bg3);color:var(--txt3)}
        .li-btn.rm:hover{background:rgba(248,113,113,0.2);color:var(--red)}

        .empty{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px;color:var(--txt3);text-align:center}
        .empty-i{font-size:36px;margin-bottom:10px;opacity:0.4}
        .empty-t{font-size:12px}

        .cat-filters{display:flex;gap:6px;padding:10px 16px;background:var(--bg2);border-bottom:1px solid var(--bdr);flex-shrink:0;flex-wrap:wrap;align-items:center}
        .cat-filter{padding:6px 12px;border:none;border-radius:6px;cursor:pointer;font-size:10px;font-weight:600;background:var(--bg3);color:var(--txt3);display:flex;align-items:center;gap:4px}
        .cat-filter:hover{background:var(--bgh);color:var(--txt)}
        .cat-filter.active{color:#fff}
        .cat-filter.all.active{background:var(--blue)}
        .cat-filter.complete.active{background:var(--green);color:#000}
        .cat-filter.incomplete.active{background:var(--orange);color:#000}
        .cat-filter.folder.active{background:var(--cyan);color:#000}
        .cat-filter.queue.active{background:var(--yellow);color:#000}
        .cat-filter .cnt{font-size:9px;padding:1px 5px;background:rgba(0,0,0,0.2);border-radius:8px}

        .cat-search{flex:1;max-width:220px;padding:6px 12px;border:1px solid var(--bdr);border-radius:6px;background:var(--bg3);color:var(--txt);font-size:11px;margin-left:auto}
        .cat-search:focus{outline:none;border-color:var(--blue)}
        .cat-search::placeholder{color:var(--txt3)}

        .log-i{padding:4px 8px;margin-bottom:2px;background:var(--bg2);border-radius:4px;font-size:10px;font-family:'JetBrains Mono',monospace;display:flex;gap:8px}
        .log-t{color:var(--txt3);flex-shrink:0;width:55px}
        .log-m{color:var(--txt2);word-break:break-word}
        .log-i.success .log-m{color:var(--green)}
        .log-i.warning .log-m{color:var(--yellow)}
        .log-i.error .log-m{color:var(--red)}
        .log-i.skip .log-m{color:var(--purple)}
        .log-i.info .log-m{color:var(--cyan)}
        .log-i.nav .log-m{color:var(--orange)}
        .log-i.replace .log-m{color:var(--orange)}

        .ftr{padding:8px 16px;background:var(--bg2);display:flex;justify-content:space-between;align-items:center;font-size:10px;color:var(--txt3);flex-shrink:0;border-top:1px solid var(--bdr)}
        .status{display:flex;align-items:center;gap:8px}
        .dot{width:8px;height:8px;border-radius:50%;background:var(--txt3)}
        .dot.on{background:var(--green);animation:pulse2 1.5s infinite}
        .dot.scan{background:var(--cyan);animation:pulse2 0.5s infinite}
        @keyframes pulse2{0%,100%{opacity:1}50%{opacity:0.3}}
        .ftr-s{display:flex;gap:16px;font-family:'JetBrains Mono',monospace}
        .ftr-s span{color:var(--green)}

        #tdm-fab{position:fixed;bottom:24px;right:24px;width:54px;height:54px;border-radius:14px;background:linear-gradient(135deg,#1e40af,#7c3aed);border:none;color:#fff;font-size:22px;cursor:pointer;z-index:999998;box-shadow:0 8px 30px rgba(30,64,175,0.5);transition:all 0.2s}
        #tdm-fab:hover{transform:scale(1.08)}
        .fab-b{position:absolute;top:-4px;right:-4px;background:var(--green);color:#000;font-size:10px;font-weight:700;padding:2px 6px;border-radius:10px}

        .toast{position:fixed;bottom:90px;right:24px;padding:10px 18px;border-radius:8px;font-size:12px;font-weight:600;z-index:999999;opacity:0;transform:translateY(10px);transition:all 0.3s;box-shadow:0 8px 24px rgba(0,0,0,0.3)}
        .toast.show{opacity:1;transform:translateY(0)}
        .toast.success{background:var(--green);color:#000}
        .toast.skip{background:var(--purple);color:#fff}
        .toast.replace{background:var(--orange);color:#000}
        .toast.error{background:var(--red);color:#fff}
    `;

    // ==================== UTILITIES ====================
    const fmt = {
        bytes: (b) => {
            if (!b || b <= 0) return '0 B';
            const k = 1024, u = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(b) / Math.log(k));
            return (b / Math.pow(k, i)).toFixed(1) + ' ' + u[i];
        },
        speed: (bps) => (!bps || bps <= 0) ? '0 B/s' : fmt.bytes(bps) + '/s',
        time: (sec) => {
            if (!sec || !isFinite(sec) || sec <= 0) return '--:--';
            if (sec < 60) return Math.ceil(sec) + 's';
            const m = Math.floor(sec / 60), s = Math.floor(sec % 60);
            return m >= 60 ? `${Math.floor(m/60)}h ${m%60}m` : `${m}m ${s}s`;
        },
        name: (n) => {
            if (!n) return `video_${Date.now()}.mp4`;
            let c = n.replace(/[<>:"/\\|?*\x00-\x1f]/g, '_').trim();
            if (!c.match(/\.\w{2,5}$/)) c += '.mp4';
            return c.slice(0, 180);
        },
        pct: (current, total) => {
            if (!total || total <= 0) return 0;
            return Math.round((current / total) * 100);
        }
    };

    const hashCode = (s) => {
        let h = 0, l = s.length, i = 0;
        if (l > 0) while (i < l) h = ((h << 5) - h + s.charCodeAt(i++)) | 0;
        return (h >>> 0).toString(36);
    };

    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const genId = () => Math.random().toString(36).substr(2, 8) + Date.now().toString(36).slice(-4);

    // ==================== LOGGING ====================
    function log(msg, type = 'info') {
        console.log(`[TDM] ${msg}`);
        State.logs.unshift({ time: new Date().toLocaleTimeString('en-US', { hour12: false }), msg, type, ts: Date.now() });
        if (State.logs.length > 500) State.logs.pop();
        scheduleRender();
    }

    function showToast(msg, type = 'success') {
        const t = document.getElementById('tdm-toast');
        if (!t) return;
        t.textContent = msg;
        t.className = 'toast show ' + type;
        setTimeout(() => t.classList.remove('show'), 2500);
    }

    // ==================== MEDIA DETECTION ====================

    function detectCurrentMedia() {
        let url = null;
        let type = 'video';
        let filename = null;

        // WEBZ (/a/) - Stories
        const storiesZ = document.getElementById('StoryViewer');
        if (storiesZ) {
            const video = storiesZ.querySelector('video');
            const videoSrc = video?.src || video?.currentSrc || video?.querySelector('source')?.src;
            if (videoSrc) {
                url = videoSrc;
                type = 'video';
            } else {
                const images = storiesZ.querySelectorAll('img.PVZ8TOWS');
                if (images.length > 0) {
                    url = images[images.length - 1]?.src;
                    type = 'image';
                }
            }
        }

        // WEBZ (/a/) - Media Viewer
        const mediaZ = document.querySelector('#MediaViewer .MediaViewerSlide--active');
        if (mediaZ && !url) {
            const videoPlayer = mediaZ.querySelector('.MediaViewerContent > .VideoPlayer');
            if (videoPlayer) {
                const video = videoPlayer.querySelector('video');
                if (video?.currentSrc) {
                    url = video.currentSrc;
                    type = 'video';
                }
            }
            if (!url) {
                const img = mediaZ.querySelector('.MediaViewerContent > div > img');
                if (img?.src) {
                    url = img.src;
                    type = 'image';
                }
            }
        }

        // WEBK (/k/) - Stories
        const storiesK = document.getElementById('stories-viewer');
        if (storiesK && !url) {
            const video = storiesK.querySelector('video.media-video');
            const videoSrc = video?.src || video?.currentSrc;
            if (videoSrc) {
                url = videoSrc;
                type = 'video';
            } else {
                const img = storiesK.querySelector('img.media-photo');
                if (img?.src) {
                    url = img.src;
                    type = 'image';
                }
            }
        }

        // WEBK (/k/) - Media Viewer
        const mediaK = document.querySelector('.media-viewer-whole');
        if (mediaK && !url) {
            const aspecter = mediaK.querySelector('.media-viewer-movers .media-viewer-aspecter');
            if (aspecter) {
                if (aspecter.querySelector('.ckin__player')) {
                    const video = aspecter.querySelector('video');
                    if (video?.src) {
                        url = video.src;
                        type = 'video';
                    }
                }
                if (!url) {
                    const video = aspecter.querySelector('video');
                    if (video?.src) {
                        url = video.src;
                        type = 'video';
                    }
                }
                if (!url) {
                    const img = aspecter.querySelector('img.thumbnail');
                    if (img?.src) {
                        url = img.src;
                        type = 'image';
                    }
                }
            }
        }

        if (!url) return null;

        // Extract filename from URL metadata
        try {
            const urlParts = url.split('/');
            const lastPart = decodeURIComponent(urlParts[urlParts.length - 1]);
            const metadata = JSON.parse(lastPart);
            if (metadata.fileName) filename = metadata.fileName;
        } catch (e) {}

        // Try DOM for filename
        if (!filename) {
            const selectors = [
                '.MediaViewerContent .document-name',
                '.media-viewer-caption',
                '.message-document-name',
                '[class*="fileName"]',
                '.document-name'
            ];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el?.textContent?.trim()) {
                    filename = el.textContent.trim();
                    break;
                }
            }
        }

        if (!filename) {
            const ext = type === 'video' ? 'mp4' : 'jpg';
            filename = `${hashCode(url)}.${ext}`;
        }

        filename = fmt.name(filename);
        return { url, type, filename };
    }

    function isMediaViewerOpen() {
        if (document.querySelector('#MediaViewer .MediaViewerSlide--active')) return true;
        if (document.getElementById('StoryViewer')) return true;
        if (document.querySelector('.media-viewer-whole .media-viewer-aspecter')) return true;
        if (document.getElementById('stories-viewer')) return true;
        return false;
    }

    // ==================== PERFECT QUEUE LOGIC ====================

    /**
     * Get file info from folder
     */
    function getFolderFileInfo(filename) {
        const key = filename.toLowerCase();
        const info = State.folderFiles.get(key);
        if (!info) return null;
        return {
            name: typeof info === 'object' ? info.name : key,
            size: typeof info === 'object' ? info.size : info,
            modified: typeof info === 'object' ? info.modified : null
        };
    }

    /**
     * Get expected size for a file (from our download records)
     */
    function getExpectedSize(filename) {
        const key = filename.toLowerCase();
        const record = State.downloadedFiles.get(key);
        return record?.size || null;
    }

    /**
     * Check if file is already in queue
     */
    function isInQueue(filename) {
        const key = filename.toLowerCase();
        return State.queue.some(item =>
            item.filename.toLowerCase() === key &&
            ['pending', 'downloading'].includes(item.status)
        );
    }

    /**
     * THE PERFECT DECISION FUNCTION
     * Returns: { action: 'add' | 'skip' | 'replace', reason: string }
     */
    function getQueueDecision(filename, url) {
        const key = filename.toLowerCase();

        // 1. URL already processed this session?
        if (State.processedUrls.has(url)) {
            return { action: 'skip', reason: 'URL already processed this session' };
        }

        // 2. Already in queue?
        if (isInQueue(filename)) {
            return { action: 'skip', reason: 'Already in queue' };
        }

        // 3. Check folder and completion status
        const folderFile = getFolderFileInfo(filename);
        const expectedSize = getExpectedSize(filename);

        if (folderFile) {
            // File exists in folder
            const folderSize = folderFile.size;

            if (expectedSize) {
                // We have a record of what size this file should be
                const completionPct = fmt.pct(folderSize, expectedSize);

                if (folderSize >= expectedSize) {
                    // 100% complete!
                    return {
                        action: 'skip',
                        reason: `Complete (${fmt.bytes(folderSize)})`,
                        complete: true
                    };
                } else {
                    // Incomplete! Offer to replace
                    return {
                        action: 'replace',
                        reason: `Incomplete: ${fmt.bytes(folderSize)} / ${fmt.bytes(expectedSize)} (${completionPct}%)`,
                        currentSize: folderSize,
                        expectedSize: expectedSize,
                        completionPct: completionPct
                    };
                }
            } else {
                // No expected size record
                if (State.settings.skipExisting && folderSize > 0) {
                    // Trust that existing file is complete
                    return {
                        action: 'skip',
                        reason: `Exists in folder (${fmt.bytes(folderSize)})`,
                        assumed: true
                    };
                } else if (folderSize === 0) {
                    // Empty file, definitely incomplete
                    return {
                        action: 'replace',
                        reason: 'Empty file in folder',
                        currentSize: 0
                    };
                }
            }
        }

        // 4. File not in folder - add to queue
        return { action: 'add', reason: 'New file' };
    }

    /**
     * Calculate catalogue status for a file
     */
    function getFileStatus(filename) {
        const key = filename.toLowerCase();
        const folderFile = getFolderFileInfo(filename);
        const expectedSize = getExpectedSize(filename);
        const inQueue = isInQueue(filename);

        if (inQueue) {
            return { status: 'queued', icon: '⏳', class: 'in-queue', label: 'In Queue' };
        }

        if (folderFile) {
            if (expectedSize) {
                if (folderFile.size >= expectedSize) {
                    return {
                        status: 'complete',
                        icon: '✅',
                        class: 'complete',
                        label: 'Complete',
                        size: folderFile.size,
                        expectedSize: expectedSize
                    };
                } else {
                    const pct = fmt.pct(folderFile.size, expectedSize);
                    return {
                        status: 'incomplete',
                        icon: '⚠️',
                        class: 'incomplete',
                        label: `${pct}%`,
                        size: folderFile.size,
                        expectedSize: expectedSize,
                        pct: pct
                    };
                }
            } else {
                return {
                    status: 'in-folder',
                    icon: '📁',
                    class: 'in-folder',
                    label: 'In Folder',
                    size: folderFile.size
                };
            }
        }

        if (expectedSize) {
            // We downloaded it before but it's not in folder now
            return {
                status: 'missing',
                icon: '❓',
                class: 'unknown',
                label: 'Missing',
                expectedSize: expectedSize
            };
        }

        return { status: 'unknown', icon: '📄', class: 'unknown', label: '' };
    }

    // ==================== QUEUE MANAGEMENT ====================

    function addToQueue(media) {
        if (!media?.url) return null;

        const { url, type, filename } = media;
        const decision = getQueueDecision(filename, url);

        // Mark URL as processed
        State.processedUrls.add(url);

        if (decision.action === 'skip') {
            State.stats.skipped++;
            State.history.skipped.unshift({
                filename,
                reason: decision.reason,
                ts: Date.now(),
                complete: decision.complete
            });
            if (State.history.skipped.length > 100) {
                State.history.skipped = State.history.skipped.slice(0, 80);
            }

            log(`⏭ Skip: ${filename} (${decision.reason})`, 'skip');
            saveData();
            scheduleRender();
            return 'skipped';
        }

        if (decision.action === 'replace') {
            State.stats.replaced++;
            State.history.replaced.unshift({
                filename,
                reason: decision.reason,
                currentSize: decision.currentSize,
                expectedSize: decision.expectedSize,
                ts: Date.now()
            });
            if (State.history.replaced.length > 100) {
                State.history.replaced = State.history.replaced.slice(0, 80);
            }

            log(`🔄 Replace: ${filename} (${decision.reason})`, 'replace');
        }

        // Add to queue
        const item = {
            id: genId(),
            url,
            filename,
            type,
            status: 'pending',
            isReplace: decision.action === 'replace',
            totalSize: decision.expectedSize || 0,
            downloaded: 0,
            bytesWritten: 0,
            progress: 0,
            speed: 0,
            eta: 0,
            addedAt: Date.now(),
        };

        State.queue.push(item);
        State.stats.scanned++;

        log(`📥 Queued: ${filename}${item.isReplace ? ' (replacing)' : ''}`, 'success');
        updateScanStatus();
        saveData();
        scheduleRender();
        processQueue();

        return decision.action === 'replace' ? 'replaced' : 'queued';
    }

    // ==================== DOWNLOAD ENGINE ====================

    function processQueue() {
        while (State.activeDownloads.size < CONFIG.MAX_CONCURRENT) {
            const next = State.queue.find(x => x.status === 'pending');
            if (!next || !State.folderHandle) break;
            downloadItem(next.id);
        }
    }

    async function downloadItem(id) {
        const item = State.queue.find(x => x.id === id);
        if (!item || item.status !== 'pending') return;

        // Double-check skip conditions
        if (!item.isReplace && State.settings.skipExisting) {
            const folderFile = getFolderFileInfo(item.filename);
            if (folderFile && folderFile.size > 0) {
                const expectedSize = getExpectedSize(item.filename);
                if (!expectedSize || folderFile.size >= expectedSize) {
                    item.status = 'skipped';
                    State.stats.skipped++;
                    log(`⏭ Skip (exists): ${item.filename}`, 'skip');
                    scheduleRender();
                    processQueue();
                    return;
                }
            }
        }

        item.status = 'downloading';
        item.startTime = Date.now();
        State.activeDownloads.set(id, { abort: false });
        scheduleRender();

        log(`⬇ Starting: ${item.filename}${item.isReplace ? ' (replacing)' : ''}`, 'info');

        try {
            // Delete existing file if replacing
            if (item.isReplace) {
                try {
                    await State.folderHandle.removeEntry(item.filename);
                    log(`🗑 Deleted old: ${item.filename}`, 'info');
                } catch (e) {
                    // File might not exist, that's ok
                }
            }

            const fileHandle = await State.folderHandle.getFileHandle(item.filename, { create: true });
            const writable = await fileHandle.createWritable();

            const finalSize = await fetchVideoChunked(item, writable);

            await writable.close();

            if (State.activeDownloads.get(id)?.abort) {
                try { await State.folderHandle.removeEntry(item.filename); } catch {}
                item.status = 'cancelled';
                log(`✕ Cancelled: ${item.filename}`, 'warning');
            } else {
                item.status = 'completed';
                item.progress = 100;
                State.stats.completed++;
                State.stats.totalBytes += finalSize;

                // Update folder files cache
                State.folderFiles.set(item.filename.toLowerCase(), {
                    name: item.filename,
                    size: finalSize,
                    modified: Date.now()
                });

                // Record the download with final size (IMPORTANT for completion tracking!)
                State.downloadedFiles.set(item.filename.toLowerCase(), {
                    size: finalSize,
                    url: item.url,
                    ts: Date.now(),
                    complete: true
                });

                State.history.completed.unshift({
                    filename: item.filename,
                    size: finalSize,
                    isReplace: item.isReplace,
                    ts: Date.now()
                });
                if (State.history.completed.length > 200) {
                    State.history.completed = State.history.completed.slice(0, 150);
                }

                const duration = ((Date.now() - item.startTime) / 1000).toFixed(1);
                log(`✅ Done: ${item.filename} (${fmt.bytes(finalSize)}) in ${duration}s`, 'success');
                showToast(`✅ ${item.filename}`, item.isReplace ? 'replace' : 'success');
                saveData();
            }
        } catch (e) {
            try { await State.folderHandle.removeEntry(item.filename); } catch {}

            if (State.activeDownloads.get(id)?.abort || e.message === 'Cancelled') {
                item.status = 'cancelled';
                log(`✕ Cancelled: ${item.filename}`, 'warning');
            } else {
                item.status = 'failed';
                State.stats.failed++;
                State.history.failed.unshift({
                    filename: item.filename,
                    url: item.url,
                    error: e.message,
                    ts: Date.now()
                });
                if (State.history.failed.length > 50) {
                    State.history.failed = State.history.failed.slice(0, 30);
                }
                log(`❌ Failed: ${item.filename} - ${e.message}`, 'error');
                saveData();
            }
        }

        State.activeDownloads.delete(id);
        scheduleRender();
        processQueue();
    }

    async function fetchVideoChunked(item, writable) {
        const dlState = State.activeDownloads.get(item.id);

        let nextOffset = 0;
        let totalSize = null;
        let bytesWritten = 0;
        let lastTime = Date.now();
        let lastBytes = 0;

        const fetchNextPart = async () => {
            if (dlState?.abort) throw new Error('Cancelled');

            const response = await fetch(item.url, {
                method: 'GET',
                headers: { 'Range': `bytes=${nextOffset}-` },
            });

            if (![200, 206].includes(response.status)) {
                throw new Error(`HTTP ${response.status}`);
            }

            const contentRange = response.headers.get('Content-Range');
            if (contentRange) {
                const match = contentRange.match(contentRangeRegex);
                if (match) {
                    const startOffset = parseInt(match[1]);
                    const endOffset = parseInt(match[2]);
                    const fileSize = parseInt(match[3]);

                    if (startOffset !== nextOffset) {
                        throw new Error('Gap in response');
                    }

                    nextOffset = endOffset + 1;
                    totalSize = fileSize;
                    item.totalSize = totalSize;
                }
            } else if (response.status === 200) {
                totalSize = parseInt(response.headers.get('Content-Length') || '0');
                item.totalSize = totalSize;
                nextOffset = totalSize;
            }

            const blob = await response.blob();
            await writable.write(blob);
            bytesWritten += blob.size;
            item.bytesWritten = bytesWritten;
            item.downloaded = bytesWritten;

            const now = Date.now();
            const timeDelta = (now - lastTime) / 1000;
            if (timeDelta >= 0.2) {
                const bytesDelta = bytesWritten - lastBytes;
                const speed = bytesDelta / timeDelta;

                item.progress = totalSize ? Math.round((bytesWritten / totalSize) * 100) : 0;
                item.speed = speed;
                item.eta = speed > 0 && totalSize ? (totalSize - bytesWritten) / speed : 0;

                lastTime = now;
                lastBytes = bytesWritten;
                scheduleRender();
            }
        };

        while (true) {
            if (dlState?.abort) throw new Error('Cancelled');
            await fetchNextPart();
            if (totalSize && nextOffset >= totalSize) break;
            if (!totalSize && nextOffset > 0) break;
        }

        return bytesWritten;
    }

    function cancelDownload(id) {
        const dl = State.activeDownloads.get(id);
        if (dl) dl.abort = true;
    }

    function removeItem(id) {
        const i = State.queue.findIndex(x => x.id === id);
        if (i >= 0) {
            const item = State.queue[i];
            State.processedUrls.delete(item.url);
            State.queue.splice(i, 1);
            scheduleRender();
        }
    }

    function clearAll() {
        State.activeDownloads.forEach(dl => dl.abort = true);
        State.queue = [];
        State.processedUrls.clear();
        State.speedHistory = [];
        State.stats.scanned = 0;
        State.lastMediaUrl = null;
        scheduleRender();
        log('🗑 Queue cleared', 'info');
    }

    function clearCompleted() {
        State.queue = State.queue.filter(x => ['pending', 'downloading'].includes(x.status));
        scheduleRender();
        log('🧹 Cleared completed', 'info');
    }

    // ==================== UI ====================
    function createUI() {
        const style = document.createElement('style');
        style.textContent = CSS;
        document.head.appendChild(style);

        const fab = document.createElement('button');
        fab.id = 'tdm-fab';
        fab.textContent = '📥';
        document.body.appendChild(fab);

        const toast = document.createElement('div');
        toast.id = 'tdm-toast';
        toast.className = 'toast';
        document.body.appendChild(toast);

        const panel = document.createElement('div');
        panel.id = 'tdm-panel';
        panel.style.width = State.panelSize.width + 'px';
        panel.style.height = State.panelSize.height + 'px';

        panel.innerHTML = `
            <div class="rh rh-n"></div><div class="rh rh-s"></div><div class="rh rh-e"></div><div class="rh rh-w"></div>
            <div class="rh rh-ne"></div><div class="rh rh-nw"></div><div class="rh rh-se"></div><div class="rh rh-sw"></div>

            <div class="hdr">
                <div class="hdr-left">
                    <span style="font-size:22px">⚡</span>
                    <div><h3>Download Manager Pro<small>v20 • Perfect Queue</small></h3></div>
                </div>
                <div class="hdr-btns">
                    <button class="hdr-btn" id="btn-min">─</button>
                    <button class="hdr-btn" id="btn-close">×</button>
                </div>
            </div>

            <div class="bar">
                <div class="folder-box" id="folder-box">
                    <span class="folder-icon">📁</span>
                    <div class="folder-info">
                        <div class="folder-name" id="folder-name">Click to select folder</div>
                        <div class="folder-meta" id="folder-meta"></div>
                    </div>
                </div>

                <div class="settings-group">
                    <div class="setting-item">
                        <input type="checkbox" id="chk-skip" ${State.settings.skipExisting ? 'checked' : ''}>
                        <label for="chk-skip">Skip existing</label>
                    </div>
                    <div class="setting-item">
                        <input type="checkbox" id="chk-complete" ${State.settings.skipComplete ? 'checked' : ''}>
                        <label for="chk-complete">Skip 100% only</label>
                    </div>
                </div>

                <div class="dir-sel">
                    <button class="dir-btn ${State.settings.direction==='left'?'on':''}" data-dir="left">← Older</button>
                    <button class="dir-btn ${State.settings.direction==='right'?'on':''}" data-dir="right">Newer →</button>
                </div>

                <button class="btn btn-g" id="btn-scan">🔍 Scan</button>
                <button class="btn btn-y" id="btn-stop" style="display:none">⏹ Stop</button>

                <div class="scan-status" id="scan-status" style="display:none">
                    <span class="label">Q:</span>
                    <span class="value found" id="scan-found">0</span>
                    <span class="label">S:</span>
                    <span class="value skipped" id="scan-skipped">0</span>
                    <span class="label">R:</span>
                    <span class="value replaced" id="scan-replaced">0</span>
                </div>

                <button class="btn btn-x" id="btn-refresh">🔄</button>
                <button class="btn btn-x" id="btn-clear-done">🧹</button>
                <button class="btn btn-r" id="btn-clear">🗑</button>
            </div>

            <div class="stats">
                <div class="stat s-cyan" title="Pending in queue"><div class="stat-v" id="s-q">0</div><div class="stat-l">Queue</div></div>
                <div class="stat s-yellow" title="Currently downloading"><div class="stat-v" id="s-a">0</div><div class="stat-l">Active</div></div>
                <div class="stat s-green" title="Successfully completed"><div class="stat-v" id="s-d">0</div><div class="stat-l">Done</div></div>
                <div class="stat s-purple" title="Skipped (already complete)"><div class="stat-v" id="s-sk">0</div><div class="stat-l">Skipped</div></div>
                <div class="stat s-orange" title="Replaced (was incomplete)"><div class="stat-v" id="s-rp">0</div><div class="stat-l">Replaced</div></div>
                <div class="stat s-blue" title="Files in selected folder"><div class="stat-v" id="s-ex">0</div><div class="stat-l">Folder</div></div>
                <div class="stat s-red" title="Failed downloads"><div class="stat-v" id="s-f">0</div><div class="stat-l">Failed</div></div>
            </div>

            <div class="active-sec" id="active-sec">
                <div class="active-hdr">
                    <div class="active-title">⬇ Downloading</div>
                    <div class="active-speed" id="total-speed">0 B/s</div>
                </div>
                <div class="active-list" id="active-list"></div>
            </div>

            <div class="speed-sec" id="speed-sec">
                <div class="speed-hdr">
                    <span class="speed-lbl">📊 Speed</span>
                    <span class="speed-val" id="speed-val">0 B/s</span>
                </div>
                <div class="speed-graph" id="speed-graph">${Array(50).fill('<div class="sp-bar" style="height:2px"></div>').join('')}</div>
            </div>

            <div class="tabs">
                <button class="tab active" data-tab="queue">📋 Queue <span class="badge" id="tab-q-cnt">0</span></button>
                <button class="tab" data-tab="catalogue">📚 Catalogue <span class="badge" id="tab-cat-cnt">0</span></button>
                <button class="tab" data-tab="logs">📝 Logs <span class="badge" id="tab-log-cnt">0</span></button>
            </div>

            <div class="tab-content active" id="tab-queue">
                <div class="cols">
                    <div class="col">
                        <div class="col-hdr"><span class="col-t">⏳ Pending</span><span class="col-c" id="q-pending-c">0</span></div>
                        <div class="col-body" id="q-pending-list"></div>
                    </div>
                    <div class="col">
                        <div class="col-hdr"><span class="col-t">✅ Completed</span><span class="col-c" id="q-done-c">0</span></div>
                        <div class="col-body" id="q-done-list"></div>
                    </div>
                    <div class="col">
                        <div class="col-hdr"><span class="col-t">⏭ Skipped / 🔄 Replaced</span><span class="col-c" id="q-skip-c">0</span></div>
                        <div class="col-body" id="q-skip-list"></div>
                    </div>
                </div>
            </div>

            <div class="tab-content" id="tab-catalogue">
                <div class="cat-filters">
                    <button class="cat-filter all active" data-filter="all">📁 All <span class="cnt" id="cat-all">0</span></button>
                    <button class="cat-filter complete" data-filter="complete">✅ Complete <span class="cnt" id="cat-complete">0</span></button>
                    <button class="cat-filter incomplete" data-filter="incomplete">⚠️ Incomplete <span class="cnt" id="cat-incomplete">0</span></button>
                    <button class="cat-filter folder" data-filter="folder">📁 Folder <span class="cnt" id="cat-folder">0</span></button>
                    <button class="cat-filter queue" data-filter="queue">⏳ Queue <span class="cnt" id="cat-queue">0</span></button>
                    <input type="text" class="cat-search" id="cat-search" placeholder="🔍 Search...">
                </div>
                <div class="content-body" id="cat-list"></div>
            </div>

            <div class="tab-content" id="tab-logs">
                <div class="content-body" id="log-list"></div>
            </div>

            <div class="ftr">
                <div class="status"><span class="dot" id="dot"></span><span id="status">Ready</span></div>
                <div class="ftr-s">Downloaded: <span id="total-dl">0 B</span></div>
            </div>
        `;

        document.body.appendChild(panel);
        setupDrag(panel);
        setupResize(panel);
        setupEvents(panel, fab);
    }

    function setupDrag(panel) {
        const hdr = panel.querySelector('.hdr');
        let drag = false, ox, oy;
        hdr.onmousedown = (e) => {
            if (e.target.closest('button')) return;
            drag = true;
            ox = e.clientX - panel.offsetLeft;
            oy = e.clientY - panel.offsetTop;
            panel.style.transform = 'none';
        };
        document.addEventListener('mousemove', (e) => {
            if (drag) {
                panel.style.left = Math.max(0, e.clientX - ox) + 'px';
                panel.style.top = Math.max(0, e.clientY - oy) + 'px';
            }
        });
        document.addEventListener('mouseup', () => drag = false);
    }

    function setupResize(panel) {
        const handles = panel.querySelectorAll('.rh');
        let resizing = null, sX, sY, sW, sH, sL, sT;
        handles.forEach(h => {
            h.addEventListener('mousedown', (e) => {
                e.preventDefault();
                resizing = h.className.replace('rh rh-', '');
                sX = e.clientX; sY = e.clientY;
                sW = panel.offsetWidth; sH = panel.offsetHeight;
                sL = panel.offsetLeft; sT = panel.offsetTop;
                panel.style.transform = 'none';
            });
        });
        document.addEventListener('mousemove', (e) => {
            if (!resizing) return;
            const dx = e.clientX - sX, dy = e.clientY - sY;
            let nW = sW, nH = sH, nL = sL, nT = sT;
            if (resizing.includes('e')) nW = Math.max(800, sW + dx);
            if (resizing.includes('w')) { nW = Math.max(800, sW - dx); nL = sL + (sW - nW); }
            if (resizing.includes('s')) nH = Math.max(600, sH + dy);
            if (resizing.includes('n')) { nH = Math.max(600, sH - dy); nT = sT + (sH - nH); }
            panel.style.width = nW + 'px'; panel.style.height = nH + 'px';
            panel.style.left = nL + 'px'; panel.style.top = nT + 'px';
            State.panelSize = { width: nW, height: nH };
        });
        document.addEventListener('mouseup', () => { if (resizing) { saveData(); resizing = null; } });
    }

    function setupEvents(panel, fab) {
        fab.onclick = () => panel.style.display = panel.style.display === 'none' ? 'flex' : 'none';
        panel.querySelector('#btn-close').onclick = () => panel.style.display = 'none';
        panel.querySelector('#btn-min').onclick = () => panel.style.display = 'none';
        panel.querySelector('#folder-box').onclick = selectFolder;
        panel.querySelector('#btn-scan').onclick = startScan;
        panel.querySelector('#btn-stop').onclick = stopScan;
        panel.querySelector('#btn-refresh').onclick = refreshFolder;
        panel.querySelector('#btn-clear').onclick = clearAll;
        panel.querySelector('#btn-clear-done').onclick = clearCompleted;

        panel.querySelector('#chk-skip').onchange = (e) => {
            State.settings.skipExisting = e.target.checked;
            saveData();
            log(`Skip existing: ${e.target.checked ? 'ON' : 'OFF'}`, 'info');
        };

        panel.querySelector('#chk-complete').onchange = (e) => {
            State.settings.skipComplete = e.target.checked;
            saveData();
            log(`Skip 100% only: ${e.target.checked ? 'ON' : 'OFF'}`, 'info');
        };

        panel.querySelectorAll('.dir-btn').forEach(btn => {
            btn.onclick = () => {
                State.settings.direction = btn.dataset.dir;
                panel.querySelectorAll('.dir-btn').forEach(b => b.classList.remove('on'));
                btn.classList.add('on');
                saveData();
            };
        });

        panel.querySelectorAll('.tab').forEach(tab => {
            tab.onclick = () => {
                State.activeTab = tab.dataset.tab;
                panel.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                panel.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
                panel.querySelector(`#tab-${State.activeTab}`).classList.add('active');
                scheduleRender();
            };
        });

        panel.querySelectorAll('.cat-filter').forEach(btn => {
            btn.onclick = () => {
                panel.querySelectorAll('.cat-filter').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                State.catalogueFilter = btn.dataset.filter;
                renderCatalogue();
            };
        });

        panel.querySelector('#cat-search').oninput = (e) => {
            State.catalogueSearch = e.target.value.toLowerCase();
            renderCatalogue();
        };
    }

    // ==================== RENDER ====================
    const RENDER_THROTTLE = 150;

    function scheduleRender() {
        if (State.renderScheduled) return;
        const elapsed = Date.now() - State.lastRender;
        if (elapsed >= RENDER_THROTTLE) { render(); }
        else {
            State.renderScheduled = true;
            setTimeout(() => { State.renderScheduled = false; render(); }, RENDER_THROTTLE - elapsed);
        }
    }

    function render() {
        State.lastRender = Date.now();
        renderStats();
        renderActive();
        renderQueue();
        renderCatalogue();
        renderLogs();
        renderSpeedGraph();
        updateFab();
        updateTabBadges();
        updateScanStatus();
    }

    function renderStats() {
        const pending = State.queue.filter(x => x.status === 'pending').length;
        const active = State.activeDownloads.size;
        document.getElementById('s-q').textContent = pending;
        document.getElementById('s-a').textContent = active;
        document.getElementById('s-d').textContent = State.stats.completed;
        document.getElementById('s-sk').textContent = State.stats.skipped;
        document.getElementById('s-rp').textContent = State.stats.replaced;
        document.getElementById('s-ex').textContent = State.folderFiles.size;
        document.getElementById('s-f').textContent = State.stats.failed;
        document.getElementById('total-dl').textContent = fmt.bytes(State.stats.totalBytes);
    }

    function updateTabBadges() {
        const pending = State.queue.filter(x => x.status === 'pending').length + State.activeDownloads.size;
        document.getElementById('tab-q-cnt').textContent = pending;
        document.getElementById('tab-cat-cnt').textContent = State.folderFiles.size + State.downloadedFiles.size;
        document.getElementById('tab-log-cnt').textContent = State.logs.length;
    }

    function updateScanStatus() {
        const el = document.getElementById('scan-status');
        if (State.isScanning) {
            el.style.display = 'flex';
            document.getElementById('scan-found').textContent = State.stats.scanned;
            document.getElementById('scan-skipped').textContent = State.stats.skipped;
            document.getElementById('scan-replaced').textContent = State.stats.replaced;
        } else {
            el.style.display = 'none';
        }
    }

    function renderActive() {
        const sec = document.getElementById('active-sec');
        const list = document.getElementById('active-list');
        const speedSec = document.getElementById('speed-sec');
        const items = State.queue.filter(x => x.status === 'downloading');

        if (items.length === 0) {
            sec.style.display = 'none';
            speedSec.style.display = 'none';
            return;
        }
        sec.style.display = 'block';
        speedSec.style.display = 'block';

        let totalSpeed = 0;
        items.forEach(i => totalSpeed += (i.speed || 0));
        document.getElementById('total-speed').textContent = fmt.speed(totalSpeed);
        document.getElementById('speed-val').textContent = fmt.speed(totalSpeed);

        list.innerHTML = items.map(item => `
            <div class="dl-card">
                <div class="dl-top">
                    <div class="dl-info">
                        <div class="dl-name" title="${item.filename}">${item.isReplace ? '🔄 ' : '📥 '}${item.filename}</div>
                        <div class="dl-meta">
                            <span class="dl-m sz">📦 <b>${fmt.bytes(item.downloaded)}</b> / ${fmt.bytes(item.totalSize)}</span>
                            <span class="dl-m sp">⚡ <b>${fmt.speed(item.speed)}</b></span>
                            <span class="dl-m et">⏱ <b>${fmt.time(item.eta)}</b></span>
                        </div>
                    </div>
                    <button class="dl-cancel" onclick="TDM.cancel('${item.id}')">✕</button>
                </div>
                <div class="dl-prog">
                    <div class="prog-bar"><div class="prog-fill" style="width:${item.progress}%"></div></div>
                    <div class="prog-info">
                        <span>${fmt.bytes(item.downloaded)} / ${fmt.bytes(item.totalSize)}</span>
                        <span class="pct">${item.progress}%</span>
                    </div>
                </div>
            </div>
        `).join('');
    }

    function renderQueue() {
        // Pending (only show pending, not completed/skipped)
        const pendingList = document.getElementById('q-pending-list');
        const pending = State.queue.filter(x => x.status === 'pending');
        document.getElementById('q-pending-c').textContent = pending.length;

        if (pending.length === 0) {
            pendingList.innerHTML = `<div class="empty"><div class="empty-i">📭</div><div class="empty-t">No pending downloads</div></div>`;
        } else {
            pendingList.innerHTML = pending.slice(0, 100).map(item => `
                <div class="li pending">
                    <span class="li-i">${item.isReplace ? '🔄' : '⏳'}</span>
                    <div class="li-info">
                        <div class="li-n" title="${item.filename}">${item.filename}</div>
                        <div class="li-m">${item.type}${item.isReplace ? ' • replacing' : ''}</div>
                    </div>
                    <div class="li-acts">
                        <button class="li-btn dl" onclick="TDM.dl('${item.id}')" ${State.folderHandle ? '' : 'disabled'}>▶</button>
                        <button class="li-btn rm" onclick="TDM.rm('${item.id}')">✕</button>
                    </div>
                </div>
            `).join('');
        }

        // Completed
        const doneList = document.getElementById('q-done-list');
        const completed = State.history.completed || [];
        document.getElementById('q-done-c').textContent = completed.length;

        if (completed.length === 0) {
            doneList.innerHTML = `<div class="empty"><div class="empty-i">✅</div><div class="empty-t">No completed downloads</div></div>`;
        } else {
            doneList.innerHTML = completed.slice(0, 100).map(f => `
                <div class="li completed">
                    <span class="li-i">${f.isReplace ? '🔄' : '✅'}</span>
                    <div class="li-info">
                        <div class="li-n" title="${f.filename}">${f.filename}</div>
                        <div class="li-m">${fmt.bytes(f.size)}${f.isReplace ? ' • replaced' : ''}</div>
                    </div>
                </div>
            `).join('');
        }

        // Skipped + Replaced
        const skipList = document.getElementById('q-skip-list');
        const skipped = State.history.skipped || [];
        const replaced = State.history.replaced || [];
        const combined = [...replaced.map(r => ({...r, type: 'replaced'})), ...skipped.map(s => ({...s, type: 'skipped'}))]
            .sort((a, b) => b.ts - a.ts);
        document.getElementById('q-skip-c').textContent = combined.length;

        if (combined.length === 0) {
            skipList.innerHTML = `<div class="empty"><div class="empty-i">📋</div><div class="empty-t">No skipped files</div></div>`;
        } else {
            skipList.innerHTML = combined.slice(0, 100).map(f => `
                <div class="li ${f.type}">
                    <span class="li-i">${f.type === 'replaced' ? '🔄' : (f.complete ? '✅' : '⏭')}</span>
                    <div class="li-info">
                        <div class="li-n" title="${f.filename}">${f.filename}</div>
                        <div class="li-m">${f.reason}</div>
                    </div>
                </div>
            `).join('');
        }
    }

    function renderCatalogue() {
        const list = document.getElementById('cat-list');
        const filter = State.catalogueFilter || 'all';
        const search = State.catalogueSearch || '';

        // Build catalogue from folder files + download records
        const catalogue = new Map();

        // Add folder files
        for (const [key, info] of State.folderFiles.entries()) {
            const name = typeof info === 'object' ? info.name : key;
            const size = typeof info === 'object' ? info.size : info;
            catalogue.set(key, { filename: name, size, inFolder: true });
        }

        // Add/update with download records
        for (const [key, info] of State.downloadedFiles.entries()) {
            const existing = catalogue.get(key) || {};
            catalogue.set(key, {
                ...existing,
                filename: existing.filename || key,
                expectedSize: info.size,
                downloadedAt: info.ts
            });
        }

        // Add status to each item
        let items = Array.from(catalogue.entries()).map(([key, item]) => {
            const status = getFileStatus(item.filename || key);
            return { ...item, key, ...status };
        });

        // Count by status
        const counts = {
            all: items.length,
            complete: items.filter(i => i.status === 'complete').length,
            incomplete: items.filter(i => i.status === 'incomplete').length,
            folder: items.filter(i => i.inFolder).length,
            queue: items.filter(i => i.status === 'queued').length,
        };

        document.getElementById('cat-all').textContent = counts.all;
        document.getElementById('cat-complete').textContent = counts.complete;
        document.getElementById('cat-incomplete').textContent = counts.incomplete;
        document.getElementById('cat-folder').textContent = counts.folder;
        document.getElementById('cat-queue').textContent = counts.queue;

        // Apply filter
        switch (filter) {
            case 'complete': items = items.filter(i => i.status === 'complete'); break;
            case 'incomplete': items = items.filter(i => i.status === 'incomplete'); break;
            case 'folder': items = items.filter(i => i.inFolder); break;
            case 'queue': items = items.filter(i => i.status === 'queued'); break;
        }

        // Apply search
        if (search) {
            items = items.filter(i => (i.filename || i.key).toLowerCase().includes(search));
        }

        // Sort by status then name
        items.sort((a, b) => {
            const order = { incomplete: 0, queued: 1, complete: 2, 'in-folder': 3, unknown: 4 };
            const diff = (order[a.status] || 99) - (order[b.status] || 99);
            if (diff !== 0) return diff;
            return (a.filename || a.key).localeCompare(b.filename || b.key);
        });

        if (items.length === 0) {
            list.innerHTML = `<div class="empty"><div class="empty-i">📚</div><div class="empty-t">No files found</div></div>`;
            return;
        }

        list.innerHTML = items.slice(0, 200).map(item => {
            const sizeInfo = item.expectedSize
                ? `${fmt.bytes(item.size || 0)} / ${fmt.bytes(item.expectedSize)}`
                : fmt.bytes(item.size || 0);

            return `
                <div class="li ${item.class}">
                    <span class="li-i">${item.icon}</span>
                    <div class="li-info">
                        <div class="li-n" title="${item.filename || item.key}">${item.filename || item.key}</div>
                        <div class="li-m">${sizeInfo}</div>
                    </div>
                    ${item.label ? `<span class="li-status ${item.class}">${item.label}</span>` : ''}
                </div>
            `;
        }).join('');
    }

    function renderLogs() {
        const list = document.getElementById('log-list');
        list.innerHTML = State.logs.slice(0, 300).map(l => `
            <div class="log-i ${l.type}">
                <span class="log-t">${l.time}</span>
                <span class="log-m">${l.msg}</span>
            </div>
        `).join('');
    }

    function renderSpeedGraph() {
        const items = State.queue.filter(x => x.status === 'downloading');
        if (items.length === 0) return;

        let totalSpeed = 0;
        items.forEach(i => totalSpeed += (i.speed || 0));
        State.speedHistory.push(totalSpeed);
        if (State.speedHistory.length > 50) State.speedHistory.shift();

        const bars = document.querySelectorAll('#speed-graph .sp-bar');
        const maxSpeed = Math.max(...State.speedHistory, 1024 * 1024);
        State.speedHistory.forEach((spd, i) => {
            if (bars[i]) bars[i].style.height = Math.max(2, (spd / maxSpeed) * 24) + 'px';
        });
    }

    function updateFab() {
        const fab = document.getElementById('tdm-fab');
        const total = State.queue.filter(x => x.status === 'pending').length + State.activeDownloads.size;
        let badge = fab.querySelector('.fab-b');
        if (total > 0) {
            if (!badge) { badge = document.createElement('span'); badge.className = 'fab-b'; fab.appendChild(badge); }
            badge.textContent = total;
        } else if (badge) {
            badge.remove();
        }
    }

    // ==================== FOLDER ====================
    async function selectFolder() {
        if (!('showDirectoryPicker' in window)) {
            alert('Use Chrome or Edge browser');
            return;
        }
        try {
            State.folderHandle = await window.showDirectoryPicker({ mode: 'readwrite' });
            State.folderName = State.folderHandle.name;
            document.getElementById('folder-box').classList.add('ok');
            document.getElementById('folder-name').textContent = State.folderName;
            await scanFolder();
            log(`📁 Selected: ${State.folderName}`, 'success');
        } catch (e) {
            if (e.name !== 'AbortError') log(`Folder error: ${e.message}`, 'error');
        }
    }

    async function scanFolder() {
        if (!State.folderHandle) return;
        State.folderFiles.clear();
        let count = 0;
        try {
            for await (const [name, handle] of State.folderHandle.entries()) {
                if (handle.kind === 'file') {
                    const file = await handle.getFile();
                    State.folderFiles.set(name.toLowerCase(), {
                        name,
                        size: file.size,
                        modified: file.lastModified
                    });
                    count++;
                }
            }
            document.getElementById('folder-meta').textContent = `${count} files`;
            scheduleRender();
        } catch (e) {
            log(`Scan error: ${e.message}`, 'error');
        }
    }

    async function refreshFolder() {
        if (!State.folderHandle) return alert('Select folder first');
        await scanFolder();
        showToast('📁 Folder refreshed', 'success');
    }

    // ==================== NAVIGATION ====================
    function navigate() {
        const dir = State.settings.direction;
        const key = dir === 'right' ? 'ArrowRight' : 'ArrowLeft';
        const keyCode = dir === 'right' ? 39 : 37;

        ['keydown', 'keyup'].forEach(t => {
            document.dispatchEvent(new KeyboardEvent(t, {
                key, code: key, keyCode, which: keyCode, bubbles: true
            }));
        });

        const selectors = dir === 'right'
            ? ['.media-viewer-switcher-right', '[class*="next"]']
            : ['.media-viewer-switcher-left', '[class*="prev"]'];

        for (const s of selectors) {
            const b = document.querySelector(s);
            if (b?.offsetParent !== null) {
                b.click();
                break;
            }
        }

        log(`→ Navigate ${dir}`, 'nav');
    }

    // ==================== SCANNING ====================
    async function startScan() {
        if (!State.folderHandle) return alert('Select folder first!');
        if (!isMediaViewerOpen()) return alert('Open a video/image in Telegram first!');

        State.isScanning = true;
        State.stats.scanned = 0;
        State.lastMediaUrl = null;

        document.getElementById('btn-scan').style.display = 'none';
        document.getElementById('btn-stop').style.display = 'inline-flex';
        document.getElementById('dot').className = 'dot scan';
        document.getElementById('status').textContent = 'Scanning...';

        log('🔍 Scan started', 'success');
        updateScanStatus();

        while (State.isScanning) {
            const media = detectCurrentMedia();

            if (!State.isScanning) break;

            if (media && media.url) {
                if (media.url !== State.lastMediaUrl && !State.processedUrls.has(media.url)) {
                    State.lastMediaUrl = media.url;

                    log(`🎬 Found: ${media.filename}`, 'info');
                    const result = addToQueue(media);

                    if (result) {
                        await sleep(CONFIG.NAV_DELAY);
                        navigate();
                    }
                } else if (media.url === State.lastMediaUrl) {
                    await sleep(300);
                    continue;
                } else {
                    State.lastMediaUrl = media.url;
                    await sleep(CONFIG.NAV_DELAY);
                    navigate();
                }
            } else {
                if (!isMediaViewerOpen()) {
                    log('📷 Media viewer closed', 'warning');
                    break;
                }
                await sleep(300);
                continue;
            }

            await sleep(CONFIG.SCAN_INTERVAL);
        }

        stopScan();
    }

    function stopScan() {
        State.isScanning = false;
        State.lastMediaUrl = null;
        document.getElementById('btn-scan').style.display = 'inline-flex';
        document.getElementById('btn-stop').style.display = 'none';
        document.getElementById('scan-status').style.display = 'none';
        document.getElementById('dot').className = 'dot';
        document.getElementById('status').textContent = 'Ready';
        log(`⏹ Stopped (Q:${State.stats.scanned} S:${State.stats.skipped} R:${State.stats.replaced})`, 'warning');
    }

    // ==================== GLOBAL API ====================
    window.TDM = {
        dl: downloadItem,
        cancel: cancelDownload,
        rm: removeItem,
        state: () => State,
        detect: detectCurrentMedia,
        decision: (f, u) => getQueueDecision(f, u),
        status: (f) => getFileStatus(f),
    };

    // ==================== INIT ====================
    function init() {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', init);
            return;
        }
        loadData();
        createUI();
        log('✅ Ready - v20 Perfect Queue', 'success');
    }

    init();
})();
