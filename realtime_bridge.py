import json
import heapq
import socket
import struct
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# ============================================================
# Real Time Bridge (Citra -> Browser)
# ============================================================
# Objetivo:
# - Rodar um servidor local simples que a página "Real Time" consome via SSE.
# - A página fica "escutando" eventos e atualiza o oponente automaticamente.
#
# Por que isso existe:
# - O navegador não consegue ler a memória do emulador (sandbox).
# - O Citra/Azahar expõe um RPC/UDP para leitura de memória.
# - Então usamos este bridge para ler memória e repassar o dado para o front.
#
# Como testar antes de ter o endereço de memória do ORAS:
# - Inicie o servidor e use /set?speciesId=25 ou /set?pokemon=pikachu
# - A página Real Time vai atualizar na hora.
#
# Endpoints:
# - GET /events                -> SSE stream (EventSource no browser)
# - GET /status                -> estado atual (JSON)
# - GET /set?speciesId=25      -> força um speciesId (inteiro)
# - GET /set?pokemon=pikachu   -> força um pokemon apiName (string)
#
# Notas:
# - Este arquivo foi feito para ser auto-contido (sem dependências externas).
# - Para integrar com o Citra de verdade, você precisa configurar os endereços
#   de memória corretos (ver seção ORAS abaixo).


HOST = "127.0.0.1"
HTTP_PORT = 9123
BRIDGE_VERSION = "realtime-bridge/0.3"

# ============================================================
# Integração Citra RPC (opcional)
# ============================================================
# O Azahar/Citra expõe um RPC via UDP.
# O script oficial de exemplo usa a porta 45987.
#
# Este bridge suporta "polling" de um endereço fixo (u16) em loop.
# Para ORAS, ainda precisamos descobrir o endereço do speciesId em batalha.

CITRA_RPC_HOST = "127.0.0.1"
CITRA_RPC_PORT = 45987

# ORAS: ENDEREÇO DO SPECIES ID DO OPONENTE EM BATALHA (u16)
# - Comece usando o modo manual (/set) enquanto a gente descobre este offset.
# - Quando tiver o endereço, coloque aqui e ligue ENABLE_CITRA_POLLING = True.
ORAS_OPPONENT_SPECIES_ADDR = None  # ex.: 0xXXXXXXXX

ENABLE_CITRA_POLLING = False
POLL_INTERVAL_SEC = 0.10

ORAS_DEFAULT_SCAN_START = 0x08000000
ORAS_DEFAULT_SCAN_END = 0x10000000
ORAS_PROCESS_IMAGE_START = 0x00100000
ORAS_PROCESS_IMAGE_END = 0x04000000
ORAS_LINEAR_HEAP_START = 0x14000000
ORAS_LINEAR_HEAP_END = 0x1C000000


class SharedState:
    def __init__(self):
        self._lock = threading.Lock()
        self._version = 0
        self._payload = {
            "ts": int(time.time() * 1000),
            "source": "init",
            "sourceDetail": "",
            "speciesId": None,
            "speciesIds": [],
            "pokemon": "",
            "pokemons": [],
            "addrUsed": None,
        }

    def snapshot(self):
        with self._lock:
            return self._version, dict(self._payload)

    def update(self, patch):
        now_ms = int(time.time() * 1000)
        with self._lock:
            next_payload = dict(self._payload)
            next_payload.update(patch or {})
            species_ids = next_payload.get("speciesIds")
            if not isinstance(species_ids, list):
                sid = next_payload.get("speciesId")
                species_ids = [int(sid)] if isinstance(sid, int) and sid > 0 else []
            species_ids = [int(x) for x in species_ids if isinstance(x, int) and x > 0][:2]
            next_payload["speciesIds"] = species_ids
            next_payload["speciesId"] = species_ids[0] if species_ids else None

            pokemons = next_payload.get("pokemons")
            if not isinstance(pokemons, list):
                p = str(next_payload.get("pokemon") or "").strip().lower()
                pokemons = [p] if p else []
            pokemons = [str(x or "").strip().lower() for x in pokemons if str(x or "").strip()][:2]
            next_payload["pokemons"] = pokemons
            next_payload["pokemon"] = pokemons[0] if pokemons else ""

            next_payload["ts"] = now_ms
            self._payload = next_payload
            self._version += 1
            return self._version, dict(self._payload)


STATE = SharedState()


class RuntimeConfig:
    def __init__(self):
        self._lock = threading.Lock()
        self._polling_enabled = bool(ENABLE_CITRA_POLLING)
        self._sources = {
            "wild": [ORAS_OPPONENT_SPECIES_ADDR, None],
            "trainer": [None, None],
        }
        self._source_mode = "auto"  # auto | wild | trainer

    def snapshot(self):
        with self._lock:
            def norm2(v):
                out = list(v or [])[:2]
                while len(out) < 2:
                    out.append(None)
                return out

            wild = norm2(self._sources.get("wild"))
            trainer = norm2(self._sources.get("trainer"))
            return {
                "pollingEnabled": bool(self._polling_enabled),
                "opponentSpeciesAddr": wild[0],
                "opponentSpeciesAddrs": wild,
                "sourceMode": str(self._source_mode or "auto"),
                "sources": {
                    "wild": wild,
                    "trainer": trainer,
                },
            }

    def set_polling(self, enabled, addr=None, addrs=None, sources=None):
        with self._lock:
            self._polling_enabled = bool(enabled)
            if sources is not None and isinstance(sources, dict):
                for k, v in sources.items():
                    if k not in self._sources:
                        continue
                    out = list(v or [])[:2]
                    while len(out) < 2:
                        out.append(None)
                    self._sources[k] = out
                return
            if addrs is not None:
                out = list(addrs or [])[:2]
                while len(out) < 2:
                    out.append(None)
                self._sources["wild"] = out
                return
            if addr is not None:
                out = list(self._sources.get("wild") or [])[:2]
                while len(out) < 2:
                    out.append(None)
                out[0] = addr
                self._sources["wild"] = out

    def polling_enabled(self):
        with self._lock:
            return bool(self._polling_enabled)

    def opponent_species_addr(self):
        with self._lock:
            return (self._sources.get("wild") or [None])[0]

    def opponent_species_addrs(self):
        with self._lock:
            addrs = list(self._sources.get("wild") or [])[:2]
            while len(addrs) < 2:
                addrs.append(None)
            return addrs

    def source_addrs(self, mode):
        with self._lock:
            key = str(mode or "").strip().lower()
            if key not in self._sources:
                key = "wild"
            addrs = list(self._sources.get(key) or [])[:2]
            while len(addrs) < 2:
                addrs.append(None)
            return addrs

    def set_source_addrs(self, mode, addrs):
        with self._lock:
            key = str(mode or "").strip().lower()
            if key not in self._sources:
                key = "wild"
            out = list(addrs or [])[:2]
            while len(out) < 2:
                out.append(None)
            self._sources[key] = out

    def set_source_mode(self, mode):
        with self._lock:
            m = str(mode or "").strip().lower()
            if m not in ("auto", "wild", "trainer"):
                m = "auto"
            self._source_mode = m

    def source_mode(self):
        with self._lock:
            m = str(self._source_mode or "auto").strip().lower()
            if m not in ("auto", "wild", "trainer"):
                m = "auto"
            return m


CONFIG = RuntimeConfig()


class LearnState:
    def __init__(self):
        self._lock = threading.Lock()
        self._job_id = 0
        self._status = {
            "jobId": 0,
            "state": "idle",
            "startedAt": 0,
            "endedAt": 0,
            "value": None,
            "scanStart": None,
            "scanEnd": None,
            "processed": 0,
            "total": 0,
            "found": 0,
            "error": "",
            "candidates": 0,
            "samples": 0,
        }
        self._candidate_set = None
        self._samples = []
        self._candidates_preview = []

    def snapshot(self):
        with self._lock:
            s = dict(self._status)
            s["candidatesList"] = list(self._candidates_preview) if self._candidates_preview else []
            return s

    def reset(self):
        with self._lock:
            self._job_id += 1
            self._status = {
                "jobId": self._job_id,
                "state": "idle",
                "startedAt": 0,
                "endedAt": 0,
                "value": None,
                "scanStart": None,
                "scanEnd": None,
                "processed": 0,
                "total": 0,
                "found": 0,
                "error": "",
                "candidates": 0,
                "samples": 0,
            }
            self._candidate_set = None
            self._samples = []
            self._candidates_preview = []

    def begin(self, value, scan_start, scan_end):
        with self._lock:
            self._job_id += 1
            self._status = {
                "jobId": self._job_id,
                "state": "running",
                "startedAt": int(time.time() * 1000),
                "endedAt": 0,
                "value": int(value),
                "scanStart": int(scan_start),
                "scanEnd": int(scan_end),
                "processed": 0,
                "total": int(scan_end - scan_start),
                "found": 0,
                "error": "",
                "candidates": len(self._candidate_set) if self._candidate_set else 0,
                "samples": len(self._samples),
            }
            return self._job_id

    def progress(self, job_id, processed, found):
        with self._lock:
            if int(job_id) != int(self._status.get("jobId") or 0):
                return
            if self._status.get("state") != "running":
                return
            self._status["processed"] = int(processed)
            self._status["found"] = int(found)

    def finish(self, job_id, found_addresses):
        with self._lock:
            if int(job_id) != int(self._status.get("jobId") or 0):
                return
            self._samples.append(int(self._status.get("value") or 0))
            found_set = set(int(x) for x in found_addresses)
            if self._candidate_set is None:
                self._candidate_set = found_set
            else:
                self._candidate_set = set(self._candidate_set).intersection(found_set)
            if self._candidate_set:
                self._candidates_preview = heapq.nsmallest(200, self._candidate_set)
            else:
                self._candidates_preview = []
            self._status["state"] = "done"
            self._status["endedAt"] = int(time.time() * 1000)
            self._status["candidates"] = len(self._candidate_set) if self._candidate_set else 0
            self._status["samples"] = len(self._samples)

    def fail(self, job_id, message):
        with self._lock:
            if int(job_id) != int(self._status.get("jobId") or 0):
                return
            self._status["state"] = "error"
            self._status["endedAt"] = int(time.time() * 1000)
            self._status["error"] = str(message or "unknown")

    def candidates(self):
        with self._lock:
            return list(self._candidates_preview) if self._candidates_preview else []

    def best_candidate(self):
        with self._lock:
            if self._candidates_preview:
                return int(self._candidates_preview[0])
            return None


LEARNERS = {
    "wild": LearnState(),
    "trainer": LearnState(),
}


class RequestType:
    ReadMemory = 1
    WriteMemory = 2
    ProcessList = 3
    SetGetProcess = 4


class CitraRpcClient:
    def __init__(self, address=CITRA_RPC_HOST, port=CITRA_RPC_PORT):
        self._addr = address
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(1.0)
        self._max_request_data = 1024

    def _generate_header(self, request_type, data_size):
        request_version = 1
        request_id = struct.unpack("I", struct.pack("f", time.time()))[0] ^ (int(time.time() * 1000) & 0xFFFFFFFF)
        return struct.pack("IIII", request_version, request_id, request_type, data_size), request_id

    def _read_and_validate_header(self, raw_reply, expected_id, expected_type):
        if not raw_reply or len(raw_reply) < 16:
            return None
        reply_version, reply_id, reply_type, reply_data_size = struct.unpack("IIII", raw_reply[:16])
        if reply_version != 1:
            return None
        if reply_id != expected_id:
            return None
        if reply_type != expected_type:
            return None
        if reply_data_size != len(raw_reply[16:]):
            return None
        return raw_reply[16:]

    def process_list(self):
        processes = {}
        read_processes = 0
        while True:
            request_data = struct.pack("II", read_processes, 0x7FFFFFFF)
            header, request_id = self._generate_header(RequestType.ProcessList, len(request_data))
            packet = header + request_data
            self._sock.sendto(packet, (self._addr, self._port))
            raw_reply = self._sock.recv(1024 + 0x10)
            reply_data = self._read_and_validate_header(raw_reply, request_id, RequestType.ProcessList)
            if reply_data is None:
                raise RuntimeError("RPC: resposta inválida (ProcessList). Verifique se o RPC do Citra/Azahar está habilitado e na porta correta.")
            if len(reply_data) < 4:
                raise RuntimeError("RPC: resposta curta demais (ProcessList).")
            read_count = struct.unpack("I", reply_data[0:4])[0]
            reply_data = reply_data[4:]
            if read_count == 0:
                break
            read_processes += read_count
            for i in range(read_count):
                proc_data = reply_data[i * 0x14 : (i + 1) * 0x14]
                if len(proc_data) != 0x14:
                    continue
                proc_id, title_id, proc_name = struct.unpack("<IQ8s", proc_data)
                try:
                    proc_name = proc_name.rstrip(b"\x00").decode("ascii")
                except Exception:
                    proc_name = ""
                processes[int(proc_id)] = {"titleId": int(title_id), "name": proc_name}
        return processes

    def get_process(self):
        request_data = struct.pack("II", 0, 0)
        header, request_id = self._generate_header(RequestType.SetGetProcess, len(request_data))
        packet = header + request_data
        self._sock.sendto(packet, (self._addr, self._port))
        raw_reply = self._sock.recv(1024 + 0x10)
        reply_data = self._read_and_validate_header(raw_reply, request_id, RequestType.SetGetProcess)
        if reply_data is None:
            raise RuntimeError("RPC: resposta inválida (GetProcess).")
        if len(reply_data) < 4:
            raise RuntimeError("RPC: resposta curta demais (GetProcess).")
        return struct.unpack("I", reply_data[:4])[0]

    def set_process(self, process_id):
        request_data = struct.pack("II", 1, int(process_id))
        header, request_id = self._generate_header(RequestType.SetGetProcess, len(request_data))
        packet = header + request_data
        self._sock.sendto(packet, (self._addr, self._port))
        try:
            self._sock.recv(1024 + 0x10)
        except Exception:
            pass

    def ensure_process(self):
        try:
            current = self.get_process()
            if current is not None and int(current) != 0:
                return int(current)
            procs = self.process_list()
            if not procs:
                return None
            chosen = sorted(procs.keys())[0]
            self.set_process(chosen)
            return int(chosen)
        except Exception:
            return None

    def rpc_health(self):
        try:
            current = self.get_process()
        except Exception as e:
            return {"ok": False, "error": str(e), "current": None, "processes": 0}
        try:
            procs = self.process_list()
        except Exception as e:
            return {"ok": False, "error": str(e), "current": int(current) if current is not None else None, "processes": 0}
        return {"ok": True, "error": "", "current": int(current) if current is not None else None, "processes": len(procs)}

    def rpc_health_light(self):
        try:
            current = self.get_process()
            return {"ok": True, "error": "", "current": int(current) if current is not None else None, "processes": 0}
        except Exception as e:
            return {"ok": False, "error": str(e), "current": None, "processes": 0}

    def read_memory(self, read_address, read_size):
        def run_with_limit(limit):
            max_request_data = int(limit)
            max_packet_size = max_request_data + 0x10

            result = b""
            remaining = int(read_size)
            address = int(read_address)

            while remaining > 0:
                chunk = min(remaining, max_request_data)
                request_data = struct.pack("II", address, chunk)
                header, request_id = self._generate_header(RequestType.ReadMemory, len(request_data))
                packet = header + request_data
                self._sock.sendto(packet, (self._addr, self._port))
                raw_reply = self._sock.recv(max_packet_size)
                reply_data = self._read_and_validate_header(raw_reply, request_id, RequestType.ReadMemory)
                if reply_data is None or len(reply_data) == 0:
                    return None
                result += reply_data
                remaining -= len(reply_data)
                address += len(reply_data)

            return result

        try:
            self.ensure_process()
            result = run_with_limit(self._max_request_data)
            return result
        except Exception:
            if int(self._max_request_data) > 32:
                self._max_request_data = 32
                self.ensure_process()
                return run_with_limit(self._max_request_data)
            raise

    def read_u16(self, address):
        raw = self.read_memory(address, 2)
        if len(raw) != 2:
            return None
        return struct.unpack("<H", raw)[0]


def citra_poll_loop():
    client = CitraRpcClient()
    last_seen = {"wild": [None, None], "trainer": [None, None]}
    stable_count = {"wild": 0, "trainer": 0}
    stable_value = {"wild": None, "trainer": None}
    stable_at = {"wild": 0.0, "trainer": 0.0}
    stable_since = {"wild": 0.0, "trainer": 0.0}
    selected_mode = None
    selected_at = 0.0
    last_emitted = {"mode": None, "speciesIds": []}
    last_emitted_at = 0.0
    pending = {"mode": None, "speciesIds": [], "since": 0.0}

    while True:
        try:
            if not CONFIG.polling_enabled():
                time.sleep(POLL_INTERVAL_SEC)
                continue
            wild_addrs = CONFIG.source_addrs("wild")
            trainer_addrs = CONFIG.source_addrs("trainer")
            if (not wild_addrs or wild_addrs[0] is None) and (not trainer_addrs or trainer_addrs[0] is None):
                time.sleep(POLL_INTERVAL_SEC)
                continue

            def read_mode(mode, addrs):
                values = []
                for i in range(2):
                    addr = addrs[i] if i < len(addrs) else None
                    if addr is None:
                        values.append(None)
                        continue
                    values.append(client.read_u16(addr))
                prev = last_seen.get(mode) or [None, None]
                for i in range(2):
                    if values[i] != prev[i]:
                        prev[i] = values[i]
                last_seen[mode] = prev
                primary = values[0]
                if isinstance(primary, int) and 0 < int(primary) < 2000:
                    if stable_value.get(mode) == int(primary):
                        stable_count[mode] = int(stable_count.get(mode) or 0) + 1
                    else:
                        stable_value[mode] = int(primary)
                        stable_count[mode] = 1
                        stable_since[mode] = time.time()
                    if int(stable_count[mode]) == 3:
                        stable_at[mode] = time.time()
                else:
                    stable_value[mode] = None
                    stable_count[mode] = 0
                valid = [int(v) for v in values if isinstance(v, int) and 0 < int(v) < 2000][:2]
                stable_ok = isinstance(stable_value.get(mode), int) and int(stable_count.get(mode) or 0) >= 3
                return {
                    "values": values,
                    "valid": valid,
                    "stable": [stable_value.get(mode)] if stable_ok and stable_value.get(mode) else [],
                    "stableAt": float(stable_at.get(mode) or 0.0),
                    "stableSince": float(stable_since.get(mode) or 0.0),
                }

            wild = read_mode("wild", wild_addrs)
            trainer = read_mode("trainer", trainer_addrs)

            pref = CONFIG.source_mode()
            now = time.time()

            def choose_auto():
                nonlocal selected_mode, selected_at
                candidates = []
                if trainer.get("stable"):
                    candidates.append(("trainer", trainer, trainer_addrs))
                if wild.get("stable"):
                    candidates.append(("wild", wild, wild_addrs))
                if not candidates:
                    return None
                if selected_mode:
                    still = [c for c in candidates if c[0] == selected_mode]
                    if still and (now - selected_at) < 1.5:
                        return still[0]
                if len(candidates) == 1:
                    selected_mode = candidates[0][0]
                    selected_at = now
                    return candidates[0]
                a, b = candidates[0], candidates[1]
                if a[1].get("stableAt", 0.0) != b[1].get("stableAt", 0.0):
                    chosen = a if a[1].get("stableAt", 0.0) > b[1].get("stableAt", 0.0) else b
                else:
                    chosen = a if a[0] == "trainer" else b
                if chosen[0] != selected_mode:
                    selected_mode = chosen[0]
                    selected_at = now
                return chosen

            if pref in ("wild", "trainer"):
                data = wild if pref == "wild" else trainer
                addrs_used = wild_addrs if pref == "wild" else trainer_addrs
                if data.get("stable"):
                    chosen = (pref, data, addrs_used)
                else:
                    chosen = None
            else:
                chosen = choose_auto()

            if chosen:
                mode, data, addrs_used = chosen
                species_ids = list(data.get("valid") or [])[:2]
                if species_ids:
                    is_same = last_emitted["mode"] == mode and last_emitted["speciesIds"] == species_ids
                    if not is_same:
                        if pending["mode"] != mode or pending["speciesIds"] != species_ids:
                            pending = {"mode": mode, "speciesIds": list(species_ids), "since": time.time()}
                        min_hold = 0.7
                        if (time.time() - float(last_emitted_at or 0.0)) > 2.0:
                            min_hold = 1.2
                        if (time.time() - float(pending["since"] or 0.0)) < min_hold:
                            continue

                    if not is_same:
                        last_emitted = {"mode": mode, "speciesIds": list(species_ids)}
                        last_emitted_at = time.time()
                        pending = {"mode": None, "speciesIds": [], "since": 0.0}
                        STATE.update(
                            {
                                "source": "citra",
                                "sourceDetail": mode,
                                "speciesIds": species_ids,
                                "pokemons": [],
                                "addrUsed": addrs_used[0] if addrs_used else None,
                            }
                        )
        except Exception:
            pass
        time.sleep(POLL_INTERVAL_SEC)


def _parse_int(value, default=None):
    if value is None:
        return default
    s = str(value).strip().lower()
    if not s:
        return default
    try:
        if s.startswith("0x"):
            return int(s, 16)
        return int(s)
    except Exception:
        return default


def _scan_u16_matches(client, scan_start, scan_end, u16_value, progress_cb=None):
    max_request_data = 1024
    pattern = struct.pack("<H", int(u16_value) & 0xFFFF)
    found = []

    start = int(scan_start)
    end = int(scan_end)
    total = max(0, end - start)
    processed = 0

    addr = start
    consecutive_failures = 0
    while addr < end:
        chunk = min(max_request_data, end - addr)
        block = client.read_memory(addr, chunk)
        if block is None:
            consecutive_failures += 1
            if consecutive_failures >= 128:
                break
            processed += chunk
            if progress_cb:
                progress_cb(processed, total, len(found))
            addr += chunk
            continue
        consecutive_failures = 0
        if block:
            i = 0
            while True:
                pos = block.find(pattern, i)
                if pos < 0:
                    break
                hit = addr + pos
                found.append(hit)
                i = pos + 1

        processed += chunk
        if progress_cb:
            progress_cb(processed, total, len(found))
        addr += chunk

    return found


def _scan_u32_matches(client, scan_start, scan_end, u32_value, progress_cb=None):
    max_request_data = 1024
    pattern = struct.pack("<I", int(u32_value) & 0xFFFFFFFF)
    found = []

    start = int(scan_start)
    end = int(scan_end)
    total = max(0, end - start)
    processed = 0

    addr = start
    consecutive_failures = 0
    while addr < end:
        chunk = min(max_request_data, end - addr)
        block = client.read_memory(addr, chunk)
        if block is None:
            consecutive_failures += 1
            if consecutive_failures >= 128:
                break
            processed += chunk
            if progress_cb:
                progress_cb(processed, total, len(found))
            addr += chunk
            continue
        consecutive_failures = 0
        if block:
            i = 0
            while True:
                pos = block.find(pattern, i)
                if pos < 0:
                    break
                found.append(addr + pos)
                i = pos + 1

        processed += chunk
        if progress_cb:
            progress_cb(processed, total, len(found))
        addr += chunk

    return found


def learn_scan_loop(mode, job_id, species_id, scan_start, scan_end):
    client = CitraRpcClient()
    key = str(mode or "").strip().lower()
    if key not in LEARNERS:
        key = "wild"
    learner = LEARNERS[key]

    def on_progress(processed, total, found):
        learner.progress(job_id, processed, found)

    try:
        a16 = _scan_u16_matches(client, scan_start, scan_end, species_id, progress_cb=on_progress)
        a32 = _scan_u32_matches(client, scan_start, scan_end, species_id, progress_cb=on_progress)
        addrs = list(set(a16).union(set(a32)))
        if not addrs:
            raise RuntimeError("Nenhum match encontrado. RPC pode estar sem processo selecionado ou o range não está legível.")
        learner.finish(job_id, addrs)
    except Exception as e:
        learner.fail(job_id, str(e))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def _send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        qs = parse_qs(parsed.query or "")
        qs_ci = {str(k).strip().lower(): v for (k, v) in qs.items()}

        if path == "/":
            self._send_json(
                HTTPStatus.OK,
                {
                    "version": BRIDGE_VERSION,
                    "endpoints": [
                        "/events",
                        "/status",
                        "/set?pokemon=pikachu",
                        "/set?pokemons=pikachu,zigzagoon",
                        "/set?speciesId=25",
                        "/set?speciesIds=25,263",
                        "/learn?speciesId=263",
                        "/learn/status",
                        "/learn/reset",
                        "/learn/lockbest",
                        "/probe?addr=0x08000000&size=64",
                        "/processes",
                        "/process/set?pid=123",
                    ],
                },
            )
            return

        if path == "/status":
            _, payload = STATE.snapshot()
            cfg = CONFIG.snapshot()
            learn_wild = LEARNERS["wild"].snapshot()
            learn_trainer = LEARNERS["trainer"].snapshot()
            proc_id = None
            proc_list = {}
            rpc = {"ok": False, "error": "RPC: não testado.", "current": None, "processes": 0}
            try:
                client = CitraRpcClient()
                rpc = client.rpc_health_light()
                proc_id = rpc.get("current")
            except Exception as e:
                rpc = {"ok": False, "error": str(e), "current": None, "processes": 0}
            out = dict(payload)
            out["config"] = cfg
            out["learn"] = learn_wild
            out["learners"] = {"wild": learn_wild, "trainer": learn_trainer}
            out["citra"] = {"rpc": rpc, "processId": proc_id, "processes": proc_list}
            self._send_json(HTTPStatus.OK, out)
            return

        if path == "/config":
            mode = str(qs_ci.get("sourcemode", [None])[0] or "").strip().lower()
            if mode:
                CONFIG.set_source_mode(mode)
            self._send_json(HTTPStatus.OK, CONFIG.snapshot())
            return

        if path == "/set":
            pokemon = str(qs.get("pokemon", [""])[0] or "").strip().lower()
            pokemons = str(qs.get("pokemons", [""])[0] or "").strip().lower()
            species_id = _parse_int(qs.get("speciesId", [None])[0], None)
            species_ids = str(qs.get("speciesIds", [""])[0] or "").strip().lower()
            if species_id is None:
                species_id = _parse_int(qs_ci.get("speciesid", [None])[0], None)
            if not species_ids:
                species_ids = str(qs_ci.get("speciesids", [""])[0] or "").strip().lower()
            if not pokemons:
                pokemons = str(qs_ci.get("pokemons", [""])[0] or "").strip().lower()
            if not pokemon:
                pokemon = str(qs_ci.get("pokemon", [""])[0] or "").strip().lower()

            if pokemons:
                list_ = [p.strip().lower() for p in pokemons.split(",") if p.strip()][:2]
                _, payload = STATE.update({"source": "manual", "pokemons": list_, "speciesIds": []})
                self._send_json(HTTPStatus.OK, payload)
                return

            if pokemon:
                list_ = [p.strip().lower() for p in pokemon.split(",") if p.strip()][:2]
                _, payload = STATE.update({"source": "manual", "pokemons": list_, "speciesIds": []})
                self._send_json(HTTPStatus.OK, payload)
                return

            if species_ids:
                parts = [p.strip() for p in species_ids.split(",") if p.strip()][:2]
                list_ = []
                for p in parts:
                    n = _parse_int(p, None)
                    if n and n > 0:
                        list_.append(int(n))
                if list_:
                    _, payload = STATE.update({"source": "manual", "speciesIds": list_, "pokemons": []})
                    self._send_json(HTTPStatus.OK, payload)
                    return

            if species_id and species_id > 0:
                _, payload = STATE.update({"source": "manual", "speciesIds": [int(species_id)], "pokemons": []})
                self._send_json(HTTPStatus.OK, payload)
                return

            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "Use /set?pokemon=pikachu | /set?speciesId=25 | /set?pokemons=a,b | /set?speciesIds=25,263"},
            )
            return

        if path == "/lock":
            addr = None
            if "addr" in qs and qs["addr"]:
                addr = _parse_int(qs["addr"][0], None)
            if addr is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Use /lock?addr=0xXXXXXXXX"})
                return
            CONFIG.set_polling(True, addr=addr)
            self._send_json(HTTPStatus.OK, {"pollingEnabled": True, "opponentSpeciesAddr": addr, "opponentSpeciesAddrs": CONFIG.opponent_species_addrs()})
            return

        if path == "/lock2":
            addr1 = _parse_int(qs.get("addr1", [None])[0], None)
            addr2 = _parse_int(qs.get("addr2", [None])[0], None)
            if addr1 is None and addr2 is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Use /lock2?addr1=0xXXXXXXXX&addr2=0xYYYYYYYY"})
                return
            addrs = CONFIG.opponent_species_addrs()
            if addr1 is not None:
                addrs[0] = addr1
            if addr2 is not None:
                addrs[1] = addr2
            CONFIG.set_polling(True, addrs=addrs)
            self._send_json(HTTPStatus.OK, {"pollingEnabled": True, "opponentSpeciesAddrs": CONFIG.opponent_species_addrs()})
            return

        if path == "/unlock":
            CONFIG.set_polling(False)
            self._send_json(HTTPStatus.OK, {"pollingEnabled": False, "opponentSpeciesAddrs": CONFIG.opponent_species_addrs()})
            return

        if path == "/learn/reset":
            mode = str(qs_ci.get("mode", ["wild"])[0] or "wild").strip().lower()
            if mode not in LEARNERS:
                mode = "wild"
            LEARNERS[mode].reset()
            self._send_json(HTTPStatus.OK, LEARNERS[mode].snapshot())
            return

        if path == "/learn/status":
            mode = str(qs_ci.get("mode", ["wild"])[0] or "wild").strip().lower()
            if mode not in LEARNERS:
                mode = "wild"
            self._send_json(HTTPStatus.OK, LEARNERS[mode].snapshot())
            return

        if path == "/learn":
            mode = str(qs_ci.get("mode", ["wild"])[0] or "wild").strip().lower()
            if mode not in LEARNERS:
                mode = "wild"
            species_id = None
            raw_species = None
            if "speciesId" in qs and qs["speciesId"]:
                raw_species = qs["speciesId"][0]
            elif "speciesid" in qs_ci and qs_ci["speciesid"]:
                raw_species = qs_ci["speciesid"][0]
            elif "id" in qs_ci and qs_ci["id"]:
                raw_species = qs_ci["id"][0]
            species_id = _parse_int(raw_species, None)
            if species_id is None or species_id <= 0:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Use /learn?speciesId=25 (National Dex ID)"})
                return

            scan_start = _parse_int(qs.get("start", [None])[0], ORAS_DEFAULT_SCAN_START)
            scan_end = _parse_int(qs.get("end", [None])[0], ORAS_DEFAULT_SCAN_END)
            if scan_start is None or scan_end is None or scan_end <= scan_start:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Range inválido. Ex.: /learn?speciesId=25&start=0x08000000&end=0x0A000000"})
                return

            learner = LEARNERS[mode]
            job_id = learner.begin(species_id, scan_start, scan_end)
            t = threading.Thread(target=learn_scan_loop, args=(mode, job_id, species_id, scan_start, scan_end), daemon=True)
            t.start()
            self._send_json(HTTPStatus.ACCEPTED, learner.snapshot())
            return

        if path == "/learn/lockbest":
            mode = str(qs_ci.get("mode", ["wild"])[0] or "wild").strip().lower()
            if mode not in LEARNERS:
                mode = "wild"
            best = LEARNERS[mode].best_candidate()
            snap = LEARNERS[mode].snapshot()
            total_candidates = int(snap.get("candidates") or 0)
            if best is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Sem candidatos. Rode /learn algumas vezes primeiro."})
                return
            CONFIG.set_source_addrs(mode, [best, None])
            CONFIG.set_polling(True)
            self._send_json(HTTPStatus.OK, {"locked": best, "mode": mode, "pollingEnabled": True, "candidates": total_candidates})
            return

        if path == "/probe":
            addr = _parse_int(qs.get("addr", [None])[0], None)
            size = _parse_int(qs.get("size", [None])[0], 64)
            if addr is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Use /probe?addr=0x08000000&size=64"})
                return
            if size is None or size <= 0 or size > 1024:
                size = 64
            try:
                client = CitraRpcClient()
                block = client.read_memory(addr, int(size))
                if block is None:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "ReadMemory retornou vazio/None para este endereço."})
                    return
                self._send_json(HTTPStatus.OK, {"addr": addr, "size": int(size), "hex": block.hex()})
            except Exception as e:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path == "/processes":
            try:
                client = CitraRpcClient()
                health = client.rpc_health()
                out = {"rpc": health, "current": health.get("current"), "processes": {}}
                if health.get("ok"):
                    out["processes"] = client.process_list()
                self._send_json(HTTPStatus.OK, out)
            except Exception as e:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path == "/process/set":
            pid = _parse_int(qs.get("pid", [None])[0], None)
            if pid is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Use /process/set?pid=123"})
                return
            try:
                client = CitraRpcClient()
                client.set_process(int(pid))
                self._send_json(HTTPStatus.OK, {"current": client.get_process()})
            except Exception as e:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path == "/events":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            last_version = -1
            last_ping = 0.0

            while True:
                try:
                    version, payload = STATE.snapshot()
                    if version != last_version:
                        last_version = version
                        data = json.dumps(payload, ensure_ascii=False)
                        self.wfile.write(b"event: message\n")
                        self.wfile.write(b"data: " + data.encode("utf-8") + b"\n\n")
                        self.wfile.flush()

                    now = time.time()
                    if now - last_ping >= 10.0:
                        last_ping = now
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()

                    time.sleep(0.25)
                except (BrokenPipeError, ConnectionResetError):
                    return
                except Exception:
                    time.sleep(0.5)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})


def main():
    t = threading.Thread(target=citra_poll_loop, daemon=True)
    t.start()

    server = ThreadingHTTPServer((HOST, HTTP_PORT), Handler)
    print(f"Real Time Bridge rodando em http://{HOST}:{HTTP_PORT}")
    print(f"Versão: {BRIDGE_VERSION}")
    print("SSE:   /events")
    print("Teste: /set?speciesId=25  ou  /set?pokemon=pikachu")
    print("Learn: /learn?speciesId=25 (faz scan na RAM e tenta achar o endereço)")
    print("Lock:  /learn/lockbest (usa o melhor candidato encontrado)")
    server.serve_forever()


if __name__ == "__main__":
    main()
