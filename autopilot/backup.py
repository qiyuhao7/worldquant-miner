#!/usr/bin/env python3
"""worldquant-miner 滚动备份：results.db → 本地 D 盘 + 阿里云 OSS（同 Bucket 新仓库路径）.

镜像 quant-trading-system 的 restic 双仓方案（v3 整改版，design review 后）：
- 新仓库隔离：本地 /mnt/d/quant-backup/restic-repo-wqm + OSS
  s3:https://oss-cn-shanghai.aliyuncs.com/quant-system/restic-repo-wqm
  （与 QTS 的 restic-repo 同 Bucket 并列，需各 restic init 一次）。
- 凭证复用 QTS .env（fallback），autopilot/.env 可覆盖（不进 git）：
  RESTIC_PASSWORD_WQM > RESTIC_PASSWORD（必填其一）；
  OSS_REGION_WQM > OSS_REGION、AWS_ACCESS_KEY_ID_WQM > AWS_ACCESS_KEY_ID、
  AWS_SECRET_ACCESS_KEY_WQM > AWS_SECRET_ACCESS_KEY；
  OSS 仓库地址只认 OSS_BACKUP_REPOSITORY_WQM（默认 wqm 新路径），
  绝不 fallback 到 QTS 的 OSS_BACKUP_REPOSITORY（防误写 QTS 仓库）。
- 只备单文件白名单：autopilot/backups/results.db（SQLite 热备快照），
  绝不备整个目录（防 credential.txt/.env 泄漏）。
- 滚动保留（最少保留 1 份）：forget 统一
  --keep-last 1 --keep-within-daily 7d --keep-within-weekly 30d，
  >30 天自动清空但永远留最后一份；local 每天 forget --prune + check，
  OSS 每天 forget（不带 prune）、prune 每 30 天、check 每 7 天（IA 省费）。
- 无变化跳过（RPO 上限锁死）：逻辑水位（COUNT/MAX(id)/submitted）
  + sha256 双判据，一致则跳过本次 restic backup，但 forget/check 照常；
  超 --max-skip-days（默认 7）强制备一次。
- 并发自愈：/tmp/wqm-backup.lock 非阻塞 flock，子进程 timeout=600，
  lock 冲突时 unlock 重试一次；state 文件原子写。

Usage:
    python3 autopilot/backup.py --dry-run
    python3 autopilot/backup.py                    # 每日一次（cron）
    flock -n /tmp/wqm-backup.lock python3 autopilot/backup.py >> autopilot/backup.log 2>&1
cron（与 QTS 02:30 错峰）：
    30 3 * * * cd /home/qyh/quant/worldquant-miner && flock -n /tmp/wqm-backup.lock python3 autopilot/backup.py >> autopilot/backup.log 2>&1
首跑（各 init 一次，需凭证就绪）：
    restic -r /mnt/d/quant-backup/restic-repo-wqm init
    restic -r s3:https://oss-cn-shanghai.aliyuncs.com/quant-system/restic-repo-wqm -o s3.bucket-lookup=dns -o s3.region=oss-cn-shanghai init

Stdlib only（对齐 miner_loop.py），日志轮转 5MB×3。
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import logging.handlers
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
PROJECT_ROOT = BASE.parent
DEFAULT_DB_PATH = BASE / 'results.db'
SNAPSHOT_PATH = BASE / 'backups' / 'results.db'
BACKUP_STATE_FILE = BASE / 'backups' / '.backup_state.json'
OSS_MAINTENANCE_STATE_FILE = BASE / 'backups' / '.oss_maintenance_state'
LOG_FILE = BASE / 'backup.log'
LOCK_FILE = Path('/tmp/wqm-backup.lock')
QTS_ENV_FILE = Path('/home/qyh/quant/quant-trading-system/.env')
LOCAL_ENV_FILE = BASE / '.env'

BACKUP_TAG = 'wqm-backup'
DEFAULT_LOCAL_REPO = '/mnt/d/quant-backup/restic-repo-wqm'
DEFAULT_OSS_REPO = 's3:https://oss-cn-shanghai.aliyuncs.com/quant-system/restic-repo-wqm'
RETENTION_ARGS = ['--keep-last', '1', '--keep-within-daily', '7d',
                  '--keep-within-weekly', '30d']
OSS_PRUNE_INTERVAL_DAYS = 30
OSS_CHECK_INTERVAL_DAYS = 7

logger = logging.getLogger('wqm-backup')


# ---------------- env ----------------

def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        text = path.read_text(encoding='utf-8')
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k:
            out[k] = v
    return out


def load_env() -> dict[str, str]:
    """加载凭证：已有环境变量优先，其次 autopilot/.env，最后 QTS .env。只打来源不打值。"""
    sources: dict[str, str] = {}
    for path in (QTS_ENV_FILE, LOCAL_ENV_FILE):
        for k, v in _parse_env_file(path).items():
            if k not in os.environ and k not in sources:
                sources[k] = str(path)
    for k, v in _parse_env_file(QTS_ENV_FILE).items():
        os.environ.setdefault(k, v)
    for k, v in _parse_env_file(LOCAL_ENV_FILE).items():
        os.environ.setdefault(k, v)
    # 仅当本地文件存在时覆盖 QTS 同名键（_WQM 键天然隔离，无需合并）
    if LOCAL_ENV_FILE.exists():
        for k, v in _parse_env_file(LOCAL_ENV_FILE).items():
            os.environ[k] = v
            sources[k] = str(LOCAL_ENV_FILE)
    loaded = [str(p) for p in (QTS_ENV_FILE, LOCAL_ENV_FILE) if p.exists()]
    logger.info(f'.env 加载（只记来源不记值）: {loaded or "无文件，走纯环境变量"}')
    return sources


def _get(key_wqm: str, key_base: str, default: str = '') -> str:
    return os.environ.get(key_wqm, '').strip() or os.environ.get(key_base, '').strip() or default


# ---------------- targets ----------------

@dataclass(frozen=True)
class BackupTarget:
    name: str
    repository: str
    extra_args: list[str] = field(default_factory=list)
    extra_env: dict[str, str] = field(default_factory=dict)


def build_targets() -> list[BackupTarget]:
    password = _get('RESTIC_PASSWORD_WQM', 'RESTIC_PASSWORD')
    if not password:
        raise RuntimeError('RESTIC_PASSWORD 缺失：请在 autopilot/.env 配置 RESTIC_PASSWORD_WQM（或复用 QTS .env 的 RESTIC_PASSWORD）')
    local_repo = os.environ.get('WQM_LOCAL_REPOSITORY', '').strip() or DEFAULT_LOCAL_REPO
    targets = [BackupTarget(
        name='local', repository=local_repo,
        extra_env={'RESTIC_REPOSITORY': local_repo, 'RESTIC_PASSWORD': password},
    )]
    oss_repo = os.environ.get('OSS_BACKUP_REPOSITORY_WQM', '').strip() or DEFAULT_OSS_REPO
    region = _get('OSS_REGION_WQM', 'OSS_REGION')
    ak = _get('AWS_ACCESS_KEY_ID_WQM', 'AWS_ACCESS_KEY_ID')
    sk = _get('AWS_SECRET_ACCESS_KEY_WQM', 'AWS_SECRET_ACCESS_KEY')
    missing = [k for k, v in (('OSS_REGION', region), ('AWS_ACCESS_KEY_ID', ak),
                              ('AWS_SECRET_ACCESS_KEY', sk)) if not v]
    if missing:
        logger.warning(f'云端凭证不完整，缺失 {missing}；本次跳过 oss，本地照常（默认 OSS 仓库 {oss_repo}）')
        return targets
    targets.append(BackupTarget(
        name='oss', repository=oss_repo,
        extra_args=['-o', 's3.bucket-lookup=dns', '-o', f's3.region={region}'],
        extra_env={'RESTIC_REPOSITORY': oss_repo, 'RESTIC_PASSWORD': password,
                   'AWS_ACCESS_KEY_ID': ak, 'AWS_SECRET_ACCESS_KEY': sk},
    ))
    logger.info(f'云端备份已启用: repo={oss_repo} region={region}')
    return targets


def get_target(name: str) -> BackupTarget:
    targets = {t.name: t for t in build_targets()}
    if name not in targets:
        raise RuntimeError(f"备份目标 '{name}' 不可用")
    return targets[name]


# ---------------- restic ----------------

def run_restic(args: list[str], target: BackupTarget, timeout: int = 600) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(target.extra_env)
    full_args = [*target.extra_args, *args]
    logger.info(f"[{target.name}] 执行: restic {' '.join(full_args)}")
    try:
        result = subprocess.run(['restic', *full_args], env=env,
                                capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f'[{target.name}] restic 超时 {timeout}s: {" ".join(full_args[:3])}') from exc
    for line in (result.stdout or '').strip().splitlines():
        logger.info(f'[{target.name}] [restic] {line}')
    if result.returncode != 0:
        for line in (result.stderr or '').strip().splitlines():
            logger.error(f'[{target.name}] [restic] {line}')
        # lock 冲突自愈一次
        if 'lock' in (result.stderr or '').lower() and args[0] != 'unlock':
            logger.warning(f'[{target.name}] 疑似 stale lock，尝试 unlock 后重试一次')
            try:
                subprocess.run(['restic', *target.extra_args, 'unlock', '--remove-all'],
                               env=env, capture_output=True, text=True, timeout=120)
            except Exception as exc:
                logger.warning(f'[{target.name}] unlock 失败: {exc}')
            else:
                result2 = subprocess.run(['restic', *full_args], env=env,
                                         capture_output=True, text=True, timeout=timeout)
                if result2.returncode == 0:
                    return result2
                raise subprocess.CalledProcessError(result2.returncode, ['restic', *full_args],
                                                    result2.stdout, result2.stderr)
        raise subprocess.CalledProcessError(result.returncode, ['restic', *full_args],
                                            result.stdout, result.stderr)
    return result


def human_size(num: float) -> str:
    value = float(num)
    for unit in ('B', 'KB', 'MB', 'GB'):
        if value < 1024.0:
            return f'{value:.2f} {unit}'
        value /= 1024.0
    return f'{value:.2f} TB'


# ---------------- sqlite ----------------

def sqlite_hot_backup(src: Path, dst: Path, retries: int = 3) -> int:
    """WAL 安全在线热备：备到 tmp 再原子 rename。并发写下 SQLITE_BUSY 重试。"""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + '.tmp')
    if tmp.exists():
        tmp.unlink()
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            src_conn = sqlite3.connect(str(src), timeout=60)
            try:
                src_conn.execute('PRAGMA busy_timeout=60000')
                dst_conn = sqlite3.connect(str(tmp))
                try:
                    src_conn.backup(dst_conn, pages=20000)
                finally:
                    dst_conn.close()
            finally:
                src_conn.close()
            break
        except sqlite3.OperationalError as exc:
            last_exc = exc
            logger.warning(f'SQLite 热备 attempt {attempt + 1}/{retries} BUSY: {exc}')
            time.sleep(5 * (attempt + 1))
    else:
        raise RuntimeError(f'SQLite 热备失败（BUSY 重试耗尽）: {last_exc}')
    tmp.replace(dst)
    return dst.stat().st_size


def get_watermark(db_path: Path) -> str:
    """逻辑水位：COUNT/MAX(id)/submitted 行数。文件哈希不可靠，只做第二判据。"""
    try:
        con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=60)
    except sqlite3.Error as exc:
        return f'unreadable:{exc}'
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'sims' not in tables:
            return 'empty:no-sims'
        count = con.execute('SELECT COUNT(*) FROM sims').fetchone()[0]
        max_id = con.execute('SELECT MAX(id) FROM sims').fetchone()[0]
        submitted = ''
        if 'kv' in tables:
            r = con.execute("SELECT v FROM kv WHERE k='submitted'").fetchone()
            if r:
                try:
                    submitted = f",submitted={len(json.loads(r[0]))}"
                except Exception:
                    submitted = ',submitted=?'
        return f'count={count},max_id={max_id}{submitted}'
    except sqlite3.Error as exc:
        return f'unreadable:{exc}'
    finally:
        con.close()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


# ---------------- state ----------------

def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    try:
        os.fsync(tmp.open('rb').fileno())
    except OSError:
        pass
    tmp.replace(path)


def load_backup_state() -> dict:
    if not BACKUP_STATE_FILE.exists():
        return {}
    try:
        return json.loads(BACKUP_STATE_FILE.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'备份 state 损坏，按首次运行处理: {exc}')
        return {}


def load_oss_maintenance_state() -> dict[str, date | None]:
    state: dict[str, date | None] = {'last_prune': None, 'last_check': None}
    if not OSS_MAINTENANCE_STATE_FILE.exists():
        return state
    try:
        raw = json.loads(OSS_MAINTENANCE_STATE_FILE.read_text(encoding='utf-8'))
        for key in state:
            if raw.get(key):
                state[key] = date.fromisoformat(raw[key])
    except (OSError, ValueError) as exc:
        logger.warning(f'OSS 节奏 state 损坏，按首次运行处理: {exc}')
    return state


def save_oss_maintenance_state(state: dict[str, date | None]) -> None:
    _atomic_write_json(OSS_MAINTENANCE_STATE_FILE,
                       {k: (v.isoformat() if v else None) for k, v in state.items()})


def should_run_oss_maintenance(action: str, last_run: date | None,
                               interval_days: int, today: date) -> tuple[bool, str]:
    if last_run is None:
        if action == 'prune':
            return False, '首次运行：跳过 prune（让前 30 天 pack 满 IA 最小存储期）'
        return True, '首次运行：执行一次 check 建基线'
    days = (today - last_run).days
    if days >= interval_days:
        return True, f'距上次 {days} 天 ≥ {interval_days} 天'
    return False, f'IA 低频：距上次 {days} 天 < {interval_days} 天'


# ---------------- maintenance ----------------

def run_local_maintenance(target: BackupTarget, dry_run: bool, no_prune: bool, full_check: bool) -> None:
    if dry_run:
        logger.info(f'[{target.name}] [dry-run] 跳过 forget --prune/check')
        return
    if not no_prune:
        run_restic(['forget', '--prune', '--tag', BACKUP_TAG, *RETENTION_ARGS], target)
    else:
        logger.info(f'[{target.name}] --no-prune，跳过 forget --prune')
    cmd = ['check'] + (['--read-data'] if full_check else [])
    run_restic(cmd, target)


def run_oss_maintenance(target: BackupTarget, dry_run: bool, no_prune: bool,
                        prune_interval_days: int, check_interval_days: int) -> None:
    today = date.today()
    state = load_oss_maintenance_state()
    if dry_run:
        logger.info(f'[{target.name}] [dry-run] 跳过 forget/prune/check')
        return
    if not no_prune:
        run_restic(['forget', '--tag', BACKUP_TAG, *RETENTION_ARGS], target)
    else:
        logger.info(f'[{target.name}] --no-prune，跳过 forget')
    should_prune, reason = should_run_oss_maintenance('prune', state['last_prune'], prune_interval_days, today)
    ran_prune = False
    if no_prune:
        logger.info(f'[{target.name}] --no-prune，跳过 prune')
    elif should_prune:
        logger.info(f'[{target.name}] prune 执行（{reason}）')
        run_restic(['prune'], target)
        ran_prune = True
    else:
        logger.info(f'[{target.name}] prune 跳过（{reason}）')
    should_check, reason = should_run_oss_maintenance('check', state['last_check'], check_interval_days, today)
    if should_check:
        logger.info(f'[{target.name}] check 执行（{reason}）')
        run_restic(['check'], target)
        state['last_check'] = today
    else:
        logger.info(f'[{target.name}] check 跳过（{reason}）')
    if ran_prune:
        state['last_prune'] = today
    elif state['last_prune'] is None:
        state['last_prune'] = today
    save_oss_maintenance_state(state)


def run_backup_for_target(target: BackupTarget, paths: list[Path], date_tag: str,
                          dry_run: bool, no_prune: bool, full_check: bool,
                          oss_prune_days: int, oss_check_days: int) -> None:
    logger.info(f'=== 目标 [{target.name}] 开始: {target.repository} ===')
    cmd = ['backup', '--tag', BACKUP_TAG, '--tag', date_tag]
    if dry_run:
        cmd.append('--dry-run')
    # 白名单单文件：只传快照，绝不传整个目录
    cmd += [str(p) for p in paths]
    run_restic(cmd, target)
    if target.name == 'oss':
        run_oss_maintenance(target, dry_run, no_prune, oss_prune_days, oss_check_days)
    else:
        run_local_maintenance(target, dry_run, no_prune, full_check)
    logger.info(f'=== 目标 [{target.name}] 完成 ===')


# ---------------- preflight ----------------

def preflight(db_path: Path, dry_run: bool) -> None:
    if shutil.which('restic') is None:
        raise RuntimeError('restic 未找到：sudo apt install restic')
    if not db_path.exists():
        msg = f'SQLite 活库不存在: {db_path}'
        if dry_run:
            logger.warning(msg + '（dry-run 继续）')
        else:
            raise RuntimeError(msg)


def preflight_local(target: BackupTarget) -> None:
    mount = Path('/mnt/d')
    if not mount.exists():
        raise RuntimeError(f'{mount} 未挂载，本地目标不可用（OSS 不受影响）')
    repo = Path(target.repository)
    try:
        repo.parent.mkdir(parents=True, exist_ok=True)
        probe = repo.parent / f'.wqm-write-probe-{os.getpid()}'
        probe.touch()
        probe.unlink()
    except OSError as exc:
        raise RuntimeError(f'本地仓库目录不可写 {repo.parent}: {exc}') from exc


# ---------------- main flow ----------------

def run_backup(*, db_path: Path, dry_run: bool, no_prune: bool, full_check: bool,
               max_skip_days: int, oss_prune_days: int, oss_check_days: int) -> int:
    date_tag = datetime.now().strftime('%Y%m%d')
    today = date.today()
    try:
        load_env()
        targets = build_targets()
        preflight(db_path, dry_run)

        if dry_run:
            logger.info('[dry-run] 备份清单（白名单单文件）：')
            logger.info(f'  - {SNAPSHOT_PATH}')
            logger.info(f'  tag={BACKUP_TAG}/{date_tag} retention={" ".join(RETENTION_ARGS)}')
            if not SNAPSHOT_PATH.exists():
                logger.info('[dry-run] 快照尚未生成，跳过 restic 调用（dry-run 通过）')
                return 0
        else:
            size = sqlite_hot_backup(db_path, SNAPSHOT_PATH)
            logger.info(f'热备完成: {human_size(size)} {db_path} → {SNAPSHOT_PATH}')

        # 无变化跳过（水位+sha 双判据，超 max_skip_days 强制）
        skip_backup = False
        if not dry_run:
            watermark = get_watermark(SNAPSHOT_PATH)
            sha = sha256_file(SNAPSHOT_PATH)
            st = load_backup_state()
            last_date = date.fromisoformat(st['last_backup_date']) if st.get('last_backup_date') else None
            days_since = (today - last_date).days if last_date else 999
            unchanged = (st.get('sha256') == sha and st.get('watermark') == watermark)
            if unchanged and days_since < max_skip_days:
                skip_backup = True
                logger.info(f'无变化跳过 backup（水位 {watermark}，距上次 {days_since} 天 < {max_skip_days} 天）；forget/check 照常')
            elif unchanged:
                logger.info(f'无变化但距上次 {days_since} 天 ≥ {max_skip_days} 天，强制备一次（RPO 上限）')
            else:
                logger.info(f'水位变化，执行 backup（{watermark} vs {st.get("watermark")}）')

        paths = [SNAPSHOT_PATH] if SNAPSHOT_PATH.exists() or dry_run else []
        if not dry_run and not paths:
            raise RuntimeError(f'快照不存在: {SNAPSHOT_PATH}')

        failures: dict[str, str] = {}
        backed_up: list[str] = []
        for target in targets:
            try:
                if target.name == 'local':
                    preflight_local(target)
                if skip_backup:
                    # 跳过 backup，但 forget/check 照常（keep-last 生效前提）
                    if target.name == 'oss':
                        run_oss_maintenance(target, False, no_prune, oss_prune_days, oss_check_days)
                    else:
                        run_local_maintenance(target, False, no_prune, full_check)
                    backed_up.append(target.name + '(skip-backup)')
                    continue
                run_backup_for_target(target, paths, date_tag, dry_run, no_prune,
                                      full_check, oss_prune_days, oss_check_days)
                backed_up.append(target.name)
            except Exception as exc:
                logger.exception(f'目标 [{target.name}] 失败: {exc}')
                failures[target.name] = f'{type(exc).__name__}: {exc}'

        if not dry_run and not skip_backup and not failures:
            _atomic_write_json(BACKUP_STATE_FILE, {
                'sha256': sha256_file(SNAPSHOT_PATH),
                'watermark': get_watermark(SNAPSHOT_PATH),
                'last_backup_date': today.isoformat(),
            })

        if failures:
            ok = [t.name for t in targets if t.name not in failures]
            logger.error(f'备份部分失败: 成功={ok or "无"} 失败={failures}')
            return 1
        logger.info(f'备份完成（目标={backed_up} tag={BACKUP_TAG}/{date_tag}）')
        return 0
    except Exception as exc:
        logger.exception(f'备份失败（全局）: {exc}')
        return 1


def setup_logging(verbose: bool) -> None:
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter('%(asctime)s - %(levelname)-8s - %(message)s', '%Y-%m-%d %H:%M:%S')
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    try:
        fh = logging.handlers.RotatingFileHandler(str(LOG_FILE), maxBytes=5 * 1024 * 1024,
                                                  backupCount=3, encoding='utf-8')
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except OSError as exc:
        print(f'日志文件不可写 {LOG_FILE}: {exc}', file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='wqm 滚动备份：results.db → 本地 + OSS（最少保留1份，30天滚动）')
    p.add_argument('--dry-run', action='store_true', help='预览：跳过真实快照，restic 加 --dry-run')
    p.add_argument('--no-prune', action='store_true', help='跳过 forget/prune')
    p.add_argument('--full-check', action='store_true', help='本地 check 加 --read-data（云端恒轻量）')
    p.add_argument('--db-path', default=str(DEFAULT_DB_PATH), help='活库路径')
    p.add_argument('--max-skip-days', type=int, default=7, help='无变化跳过上限天数，超期强制备一次')
    p.add_argument('--oss-prune-interval-days', type=int, default=OSS_PRUNE_INTERVAL_DAYS)
    p.add_argument('--oss-check-interval-days', type=int, default=OSS_CHECK_INTERVAL_DAYS)
    p.add_argument('--verbose', action='store_true')
    return p


def main() -> int:
    args = build_parser().parse_args()
    setup_logging(args.verbose)
    # 进程锁：cron 里已 flock，这里再加一道（非阻塞，拿不到直接退出）
    try:
        lock_fh = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        print('已有备份进程在跑，本次退出', flush=True)
        return 1
    try:
        return run_backup(db_path=Path(args.db_path), dry_run=args.dry_run,
                          no_prune=args.no_prune, full_check=args.full_check,
                          max_skip_days=args.max_skip_days,
                          oss_prune_days=args.oss_prune_interval_days,
                          oss_check_days=args.oss_check_interval_days)
    finally:
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)
            lock_fh.close()
        except Exception:
            pass


if __name__ == '__main__':
    sys.exit(main())
