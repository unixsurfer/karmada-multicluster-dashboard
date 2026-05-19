#!/usr/bin/env python3
"""
karmada-multicluster-dashboard — terminal dashboard for Karmada multi-cluster deployments.

Displays a live replica panel (pods per cluster) and a traffic panel (streamed /info responses),
with optional Argo Rollouts status when a rollout controller is present.

Usage (with Argo Rollouts):
    python3 dashboard.py \\
        --namespace <ns> --rollout-name <name> --host-header <host> \\
        --members-kubeconfig <path> --karmada-kubeconfig <path> [--scenario s2|s4]

Usage (without Argo Rollouts / primitive deployments):
    python3 dashboard.py --no-rollout \\
        --namespace <ns> --host-header <host> --members-kubeconfig <path>

Cluster configuration (repeatable, default: 3 kind clusters on ports 8090-8092):
    --cluster label:context:port:metrics-port

    Example:
        --cluster member1:member1:8090:10254 \\
        --cluster member2:member2:8091:10255 \\
        --cluster member3:member3:8092:10256

Other options:
    --no-rollout                Disable Argo Rollouts status panel
    --karmada-context NAME      kubectl context for the Karmada API (default: karmada-apiserver)
    --karmada-kubeconfig PATH   kubeconfig for the Karmada API (Argo Rollouts mode only)
    --members-kubeconfig PATH   kubeconfig for member clusters (default: ~/.kube/members.config)
    --web-metrics-port N        localhost port for web-metrics readiness probe (default: 9095)

Scenarios:
    s2  Enable per-cluster NGINX ingress metrics panel
    s4  Enable web-metrics readiness badge (top-right)

Requires:
    - Port-forwards running to each member cluster's ingress on the configured ports
"""

import argparse
import curses
import json
import subprocess
import sys
import threading
import time
import urllib.request
from collections import deque

DEFAULT_CLUSTERS = [
    ("member1", "member1", 8090, 10254),
    ("member2", "member2", 8091, 10255),
    ("member3", "member3", 8092, 10256),
]

REFRESH_REPLICAS_S      = 2
REFRESH_ROLLOUT_S       = 2
TRAFFIC_INTERVAL_S      = 0.5
TRAFFIC_LINES           = 40
TRAFFIC_TIMEOUT_S       = 2
METRICS_INTERVAL_S      = 5
METRICS_TIMEOUT_S       = 3
WEB_METRICS_INTERVAL_S  = 2

# ── ANSI colour pairs (initialised in main) ───────────────────────────────────
C_HEADER   = 1   # bold white on blue
C_STABLE   = 2   # green
C_CANARY   = 3   # yellow
C_ERROR    = 4   # red
C_DIM      = 5   # dim white
C_BORDER   = 6   # cyan
C_TITLE    = 7   # bold cyan


class RolloutStatus:
    def __init__(self):
        self.phase            = ""
        self.current_step     = 0
        self.total_steps      = 0
        self.set_weight       = 0
        self.actual_weight    = 0
        self.replicas         = 0
        self.updated_replicas = 0
        self.error            = None
        self.lock             = threading.Lock()


class ClusterState:
    def __init__(self, label, context, port, metrics_port):
        self.label        = label
        self.context      = context
        self.port         = port
        self.metrics_port = metrics_port
        self.pods         = []
        self.pod_err      = None
        self.traffic      = deque(maxlen=TRAFFIC_LINES)
        self.lock         = threading.Lock()


class MetricState:
    def __init__(self):
        self.stable_delta = None
        self.canary_delta = None
        self.error        = None
        self._prev_stable = None
        self._prev_canary = None
        self.lock         = threading.Lock()


class WebMetricsState:
    def __init__(self):
        self.readiness = None
        self.error     = None
        self.lock      = threading.Lock()


def _kubectl_get_pods(context, namespace, kubeconfig):
    env = {"KUBECONFIG": kubeconfig, "PATH": __import__("os").environ.get("PATH", "")}
    result = subprocess.run(
        [
            "kubectl", "--context", context,
            "-n", namespace,
            "get", "pods",
            "-o", "json",
            "--field-selector", "status.phase=Running",
        ],
        capture_output=True, text=True, timeout=8, env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "kubectl error")

    data = json.loads(result.stdout)
    pods = []
    for item in data.get("items", []):
        meta   = item.get("metadata", {})
        spec   = item.get("spec", {})
        status = item.get("status", {})

        name  = meta.get("name", "")
        phase = status.get("phase", "")

        containers  = status.get("containerStatuses", [])
        ready_count = sum(1 for c in containers if c.get("ready"))
        total_count = len(containers)
        ready_str   = f"{ready_count}/{total_count}"

        labels        = meta.get("labels", {})
        pod_tmpl_hash = labels.get("pod-template-hash", "")

        version = labels.get("rollouts-pod-template-hash", "")
        for c in spec.get("containers", []):
            img = c.get("image", "")
            if ":" in img:
                version = img.split(":")[-1]
                break

        pods.append({
            "name":     name[:32],
            "ready":    ready_str,
            "status":   phase,
            "version":  version,
            "pod_hash": pod_tmpl_hash,
        })
    return pods


def _fetch_rollout_status(namespace, rollout_name, karmada_kubeconfig, karmada_context):
    env = {"KUBECONFIG": karmada_kubeconfig, "PATH": __import__("os").environ.get("PATH", "")}
    result = subprocess.run(
        ["kubectl", "--context", karmada_context,
         "-n", namespace, "get", "rollout", rollout_name, "-o", "json"],
        capture_output=True, text=True, timeout=8, env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "kubectl error")

    r      = json.loads(result.stdout)
    spec   = r.get("spec", {})
    status = r.get("status", {})
    steps  = spec.get("strategy", {}).get("canary", {}).get("steps", [])

    phase = status.get("phase", "Unknown")
    if phase == "Paused" and r.get("spec", {}).get("paused"):
        phase = "Paused (manual)"
    current_step = status.get("currentStepIndex", 0) or 0
    total_steps  = len(steps)
    replicas     = status.get("replicas", 0) or 0
    updated      = status.get("updatedReplicas", 0) or 0

    if phase == "Healthy":
        set_weight = 100
    else:
        set_weight = 0
        for i in range(min(current_step, total_steps) - 1, -1, -1):
            if "setWeight" in steps[i]:
                set_weight = steps[i]["setWeight"]
                break

    weights       = status.get("canary", {}).get("weights", {})
    canary_w      = weights.get("canary", {})
    actual_weight = canary_w.get("weight", set_weight) if canary_w else set_weight

    return phase, current_step, total_steps, set_weight, actual_weight, replicas, updated


def rollout_worker(rs, namespace, rollout_name, karmada_kubeconfig, karmada_context, stop_event):
    while not stop_event.is_set():
        try:
            phase, step, total, set_w, actual_w, replicas, updated = \
                _fetch_rollout_status(namespace, rollout_name, karmada_kubeconfig, karmada_context)
            with rs.lock:
                rs.phase            = phase
                rs.current_step     = step
                rs.total_steps      = total
                rs.set_weight       = set_w
                rs.actual_weight    = actual_w
                rs.replicas         = replicas
                rs.updated_replicas = updated
                rs.error            = None
        except Exception as exc:
            with rs.lock:
                rs.error = str(exc)[:80]
        stop_event.wait(REFRESH_ROLLOUT_S)


def replica_worker(state, namespace, kubeconfig, stop_event):
    while not stop_event.is_set():
        try:
            pods = _kubectl_get_pods(state.context, namespace, kubeconfig)
            with state.lock:
                state.pods    = pods
                state.pod_err = None
        except Exception as exc:
            with state.lock:
                state.pod_err = str(exc)[:60]
        stop_event.wait(REFRESH_REPLICAS_S)


def traffic_worker(state, host_header, stop_event):
    while not stop_event.is_set():
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{state.port}/info",
                headers={"Host": host_header},
            )
            with urllib.request.urlopen(req, timeout=TRAFFIC_TIMEOUT_S) as resp:
                body = json.loads(resp.read().decode())
            hostname      = body.get("hostname",      "")[:20]
            version       = body.get("version",       "")
            rollout_label = body.get("rollout_label", "")
            uptime        = body.get("uptime",        "")
            line = (hostname, version, rollout_label, uptime)
        except Exception as exc:
            line = (f"[err] {str(exc)[:40]}", "", "", "")

        with state.lock:
            state.traffic.append(line)

        stop_event.wait(TRAFFIC_INTERVAL_S)


def metrics_worker(state, ms, namespace, kubeconfig, stop_event):
    import os
    import re

    env = {"KUBECONFIG": kubeconfig, "PATH": os.environ.get("PATH", "")}
    pf_proc = None

    def _start_pf():
        nonlocal pf_proc
        if pf_proc and pf_proc.poll() is None:
            return
        pf_proc = subprocess.Popen(
            ["kubectl", "--context", state.context,
             "-n", "ingress-nginx",
             "port-forward", "deployment/ingress-nginx-controller",
             f"{state.metrics_port}:10254"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env,
        )

    def _scrape():
        url = f"http://127.0.0.1:{state.metrics_port}/metrics"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=METRICS_TIMEOUT_S) as resp:
            return resp.read().decode()

    def _parse(text, namespace):
        stable = 0.0
        canary = 0.0
        for line in text.splitlines():
            if not line.startswith("nginx_ingress_controller_requests"):
                continue
            if f'namespace="{namespace}"' not in line:
                continue
            try:
                val = float(line.split()[-1])
            except ValueError:
                continue
            if re.search(r'canary="[^"]+"', line):
                canary += val
            else:
                stable += val
        return stable, canary

    _start_pf()
    stop_event.wait(2)

    while not stop_event.is_set():
        try:
            _start_pf()
            text = _scrape()
            cur_stable, cur_canary = _parse(text, namespace)

            with ms.lock:
                if ms._prev_stable is not None:
                    ms.stable_delta = max(0, int(cur_stable - ms._prev_stable))
                    ms.canary_delta = max(0, int(cur_canary - ms._prev_canary))
                ms._prev_stable = cur_stable
                ms._prev_canary = cur_canary
                ms.error = None
        except Exception as exc:
            with ms.lock:
                ms.error = str(exc)[:50]

        stop_event.wait(METRICS_INTERVAL_S)

    if pf_proc and pf_proc.poll() is None:
        pf_proc.terminate()


def web_metrics_worker(wms, web_metrics_port, stop_event):
    while not stop_event.is_set():
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{web_metrics_port}/info")
            with urllib.request.urlopen(req, timeout=2) as resp:
                body = json.loads(resp.read().decode())
            with wms.lock:
                wms.readiness = body.get("readiness")
                wms.error     = None
        except Exception as exc:
            with wms.lock:
                wms.readiness = None
                wms.error     = str(exc)[:40]
        stop_event.wait(WEB_METRICS_INTERVAL_S)


def draw(stdscr, states, rs, metric_states=None, wms=None, no_rollout=False):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(500)

    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(C_HEADER, curses.COLOR_WHITE,  curses.COLOR_BLUE)
    curses.init_pair(C_STABLE, curses.COLOR_GREEN,  -1)
    curses.init_pair(C_CANARY, curses.COLOR_YELLOW, -1)
    curses.init_pair(C_ERROR,  curses.COLOR_RED,    -1)
    curses.init_pair(C_DIM,    curses.COLOR_WHITE,  -1)
    curses.init_pair(C_BORDER, curses.COLOR_CYAN,   -1)
    curses.init_pair(C_TITLE,  curses.COLOR_CYAN,   -1)

    while True:
        ch = stdscr.getch()
        if ch in (ord("q"), ord("Q"), 27):
            break

        rows, cols = stdscr.getmaxyx()
        stdscr.erase()

        col_w       = cols // len(states)
        HEADER_ROWS = 1 if no_rollout else 3
        replica_h   = (rows - HEADER_ROWS) // 2 + HEADER_ROWS
        traffic_h   = rows - replica_h - 1

        stable = _stable_version(states, key="pod_hash" if no_rollout else "version")

        if no_rollout:
            rl_counts = {}
            for s in states:
                with s.lock:
                    for _, _, rl, _ in s.traffic:
                        if rl:
                            rl_counts[rl] = rl_counts.get(rl, 0) + 1
            stable_rollout_label = max(rl_counts, key=rl_counts.get) if rl_counts else ""
        else:
            stable_rollout_label = ""

        # ── row 0: title bar ─────────────────────────────────────────────────
        title = " Karmada Multi-Cluster Dashboard  [q] quit " if no_rollout \
            else " Argo Rollouts Multi-Cluster Dashboard  [q] quit "
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, title.center(cols)[:cols])
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        if wms:
            with wms.lock:
                if wms.error:
                    badge_text = " web-metrics: ERR "
                    badge_attr = curses.color_pair(C_ERROR) | curses.A_BOLD
                elif wms.readiness is None:
                    badge_text = " web-metrics: … "
                    badge_attr = curses.color_pair(C_DIM)
                elif wms.readiness:
                    badge_text = " web-metrics: READY "
                    badge_attr = curses.color_pair(C_STABLE) | curses.A_BOLD
                else:
                    badge_text = " web-metrics: NOT READY "
                    badge_attr = curses.color_pair(C_ERROR) | curses.A_BOLD
            bx = cols - len(badge_text) - 1
            if bx > 0:
                _safe_addstr(stdscr, 0, bx, badge_text, badge_attr)

        # ── row 1: rollout status line (omitted with --no-rollout) ───────────
        if not no_rollout:
            with rs.lock:
                if rs.error:
                    status_text = f" rollout: {rs.error}"
                    status_attr = curses.color_pair(C_ERROR)
                else:
                    phase     = rs.phase or "…"
                    remaining = rs.total_steps - rs.current_step
                    status_text = (
                        f" {phase}"
                        f"  step {rs.current_step}/{rs.total_steps}"
                        f"  remaining {remaining}"
                        f"  setWeight {rs.set_weight}%"
                        f"  actualWeight {rs.actual_weight}%"
                        f"  replicas {rs.replicas}"
                        f"  updated {rs.updated_replicas} "
                    )
                    if phase == "Healthy":
                        status_attr = curses.color_pair(C_STABLE) | curses.A_BOLD
                    elif phase in ("Degraded", "Error"):
                        status_attr = curses.color_pair(C_ERROR) | curses.A_BOLD
                    elif phase in ("Paused", "Paused (manual)", "Progressing"):
                        status_attr = curses.color_pair(C_CANARY) | curses.A_BOLD
                    else:
                        status_attr = curses.color_pair(C_DIM)

            _safe_addstr(stdscr, 1, 0, status_text[:cols], status_attr)
            _safe_addstr(stdscr, 2, 0, "─" * cols, curses.color_pair(C_BORDER))

        # ── replica panels ────────────────────────────────────────────────────
        COL_HEADER_ROW = HEADER_ROWS
        COL_SEP_ROW    = HEADER_ROWS + 1
        POD_START_ROW  = HEADER_ROWS + 2

        for ci, state in enumerate(states):
            x0 = ci * col_w

            with state.lock:
                pods    = list(state.pods)
                pod_err = state.pod_err

            vkey        = "pod_hash" if no_rollout else "version"
            stable_pods = [p for p in pods if not _is_canary_version(p[vkey], stable)]
            canary_pods = [p for p in pods if     _is_canary_version(p[vkey], stable)]
            header      = f" {state.label}  stable={len(stable_pods)} canary={len(canary_pods)} "

            if ci > 0:
                for r in range(HEADER_ROWS, replica_h):
                    _safe_addstr(stdscr, r, x0, "│", curses.color_pair(C_BORDER))

            _safe_addstr(stdscr, COL_HEADER_ROW, x0 + (1 if ci > 0 else 0),
                         header[:col_w - 1],
                         curses.color_pair(C_TITLE) | curses.A_BOLD)

            sep = "─" * (col_w - (1 if ci > 0 else 0))
            _safe_addstr(stdscr, COL_SEP_ROW, x0 + (1 if ci > 0 else 0),
                         sep[:col_w - 1], curses.color_pair(C_BORDER))

            if pod_err:
                _safe_addstr(stdscr, POD_START_ROW, x0 + 1, pod_err[:col_w - 2],
                             curses.color_pair(C_ERROR))
            else:
                row = POD_START_ROW
                for pod in pods:
                    if row >= replica_h - 1:
                        break
                    is_c   = _is_canary_version(pod[vkey], stable)
                    colour = curses.color_pair(C_CANARY) if is_c else curses.color_pair(C_STABLE)
                    marker = "●" if is_c else "○"
                    line   = f"{marker} {pod['name']:<30} {pod['ready']:<5} {pod['version']}"
                    _safe_addstr(stdscr, row, x0 + (1 if ci > 0 else 0),
                                 line[:col_w - 1], colour)
                    row += 1

        # ── divider between replica and traffic panels ────────────────────────
        div_row = replica_h
        stdscr.attron(curses.color_pair(C_BORDER))
        _safe_addstr(stdscr, div_row, 0, "─" * cols)
        stdscr.attroff(curses.color_pair(C_BORDER))
        _safe_addstr(stdscr, div_row, 2, " Traffic ",
                     curses.color_pair(C_TITLE) | curses.A_BOLD)

        # ── traffic panels ────────────────────────────────────────────────────
        METRIC_ROWS = 2 if metric_states else 0
        for ci, state in enumerate(states):
            x0  = ci * col_w
            off = 1 if ci > 0 else 0

            with state.lock:
                lines = list(state.traffic)

            _safe_addstr(stdscr, div_row + 1, x0 + off,
                         f" {state.label} ", curses.color_pair(C_DIM) | curses.A_BOLD)

            if ci > 0:
                for r in range(div_row + 1, rows - 1):
                    _safe_addstr(stdscr, r, x0, "│", curses.color_pair(C_BORDER))

            log_end = rows - 1 - METRIC_ROWS
            visible = lines[-(log_end - (div_row + 2)):]
            for li, (hostname, version, rollout_label, uptime) in enumerate(visible):
                row = div_row + 2 + li
                if row >= log_end:
                    break
                is_err = hostname.startswith("[err]")
                if is_err:
                    text = hostname
                elif no_rollout:
                    text = f"{hostname}  ver={rollout_label}  tag={version}"
                else:
                    text = f"{hostname}  tag={version}  up={uptime}"
                if no_rollout:
                    is_canary = _is_canary_version(rollout_label, stable_rollout_label)
                else:
                    is_canary = _is_canary_version(version, stable)
                colour = (curses.color_pair(C_CANARY) | curses.A_BOLD
                          if is_canary else curses.color_pair(C_DIM))
                _safe_addstr(stdscr, row, x0 + off, text[:col_w - 2], colour)

            if metric_states:
                ms         = metric_states[ci]
                sep_row    = rows - 1 - METRIC_ROWS
                metric_row = rows - 1 - METRIC_ROWS + 1

                _safe_addstr(stdscr, sep_row, x0 + off,
                             "╌" * (col_w - off - 1), curses.color_pair(C_BORDER))

                with ms.lock:
                    if ms.error:
                        mtext = f" metric err: {ms.error}"
                        mattr = curses.color_pair(C_ERROR)
                    elif ms.stable_delta is None:
                        mtext = " metrics: waiting…"
                        mattr = curses.color_pair(C_DIM)
                    else:
                        mtext = (f" ○ stable +{ms.stable_delta:>4} req"
                                 f"   ● canary +{ms.canary_delta:>4} req"
                                 f"  / {METRICS_INTERVAL_S}s")
                        mattr = curses.color_pair(C_DIM)

                _safe_addstr(stdscr, metric_row, x0 + off, mtext[:col_w - 2], mattr)

        # ── bottom status bar ─────────────────────────────────────────────────
        ts = time.strftime("%H:%M:%S")
        _vkey      = "pod_hash" if no_rollout else "version"
        has_canary = any(
            _is_canary_version(p[_vkey], stable)
            for s in states for p in s.pods
        )
        legend = "  ● canary  ○ stable" if has_canary else ""
        bar    = f" {ts}{legend} "
        stdscr.attron(curses.color_pair(C_HEADER))
        _safe_addstr(stdscr, rows - 1, 0, bar.ljust(cols)[:cols])
        stdscr.attroff(curses.color_pair(C_HEADER))

        stdscr.refresh()
        time.sleep(0.5)


def _stable_version(states, key="version"):
    counts = {}
    for state in states:
        with state.lock:
            for pod in state.pods:
                v = pod.get(key, "")
                if v:
                    counts[v] = counts.get(v, 0) + 1
    if not counts:
        return ""
    return max(counts, key=counts.get)


def _is_canary_version(version, stable):
    return bool(version) and bool(stable) and version != stable


def _safe_addstr(win, y, x, text, attr=0):
    rows, cols = win.getmaxyx()
    if y < 0 or y >= rows or x < 0 or x >= cols:
        return
    max_len = cols - x - 1
    if max_len <= 0:
        return
    try:
        win.addstr(y, x, text[:max_len], attr)
    except curses.error:
        pass


def _parse_cluster(value):
    """Parse label:context:port:metrics-port into a 4-tuple."""
    parts = value.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            f"cluster must be label:context:port:metrics-port, got: {value!r}"
        )
    label, context, port, metrics_port = parts
    try:
        return (label, context, int(port), int(metrics_port))
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"port and metrics-port must be integers, got: {value!r}"
        )


def main():
    import os
    default_members_kubeconfig = os.path.join(os.path.expanduser("~"), ".kube", "members.config")
    default_karmada_kubeconfig = os.path.join(os.path.expanduser("~"), ".kube", "karmada.config")

    parser = argparse.ArgumentParser(
        description="Terminal dashboard for Karmada multi-cluster deployments.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--no-rollout", action="store_true",
                        help="Disable Argo Rollouts status panel")
    parser.add_argument("--cluster", dest="clusters", metavar="label:context:port:metrics-port",
                        type=_parse_cluster, action="append",
                        help="Cluster spec (repeatable). Default: 3 kind clusters on ports 8090-8092.")
    parser.add_argument("--namespace", default="default",
                        help="Kubernetes namespace (default: default)")
    parser.add_argument("--host-header", default="http-probe.local",
                        help="Host header for traffic requests (default: http-probe.local)")
    parser.add_argument("--members-kubeconfig", default=default_members_kubeconfig,
                        help=f"kubeconfig for member clusters (default: {default_members_kubeconfig})")
    parser.add_argument("--rollout-name", default="",
                        help="Argo Rollouts rollout name (required without --no-rollout)")
    parser.add_argument("--karmada-kubeconfig", default=default_karmada_kubeconfig,
                        help=f"kubeconfig for the Karmada API (default: {default_karmada_kubeconfig})")
    parser.add_argument("--karmada-context", default="karmada-apiserver",
                        help="kubectl context for the Karmada API (default: karmada-apiserver)")
    parser.add_argument("--scenario", default="",
                        help="Enable additional panels: s2 (ingress metrics) or s4 (web-metrics badge)")
    parser.add_argument("--web-metrics-port", type=int, default=9095,
                        help="localhost port for web-metrics readiness probe (default: 9095)")

    args = parser.parse_args()

    clusters           = args.clusters if args.clusters else DEFAULT_CLUSTERS
    namespace          = args.namespace
    host_header        = args.host_header
    members_kubeconfig = args.members_kubeconfig
    karmada_kubeconfig = args.karmada_kubeconfig
    rollout_name       = args.rollout_name
    scenario           = args.scenario

    if not args.no_rollout and not rollout_name:
        parser.error("--rollout-name is required without --no-rollout")

    states     = [ClusterState(label, ctx, port, mport) for label, ctx, port, mport in clusters]
    rs         = RolloutStatus()
    stop_event = threading.Event()

    if not args.no_rollout:
        threading.Thread(
            target=rollout_worker,
            args=(rs, namespace, rollout_name, karmada_kubeconfig,
                  args.karmada_context, stop_event),
            daemon=True,
        ).start()

    metric_states = None
    if scenario == "s2":
        metric_states = [MetricState() for _ in states]
        for state, ms in zip(states, metric_states):
            threading.Thread(target=metrics_worker,
                             args=(state, ms, namespace, members_kubeconfig, stop_event),
                             daemon=True).start()

    wms = None
    if scenario == "s4":
        wms = WebMetricsState()
        threading.Thread(target=web_metrics_worker,
                         args=(wms, args.web_metrics_port, stop_event),
                         daemon=True).start()

    for state in states:
        threading.Thread(target=replica_worker,
                         args=(state, namespace, members_kubeconfig, stop_event),
                         daemon=True).start()
        threading.Thread(target=traffic_worker,
                         args=(state, host_header, stop_event),
                         daemon=True).start()

    try:
        curses.wrapper(draw, states, rs, metric_states, wms, args.no_rollout)
    finally:
        stop_event.set()



if __name__ == "__main__":
    main()
