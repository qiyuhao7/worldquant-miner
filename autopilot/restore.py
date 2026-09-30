#!/usr/bin/env python3
"""wqm 恢复工具：从本地/OSS 列快照、恢复到临时目录、可选回写活库.

默认只恢复到 --target（安全）；回写活库必须同时满足：
  --apply-live + --force + daemon 已停，否则拒绝执行。
回写顺序：cp live.db.bak → 拷回 → 删 -wal/-shm → PRAGMA integrity_check → 失败回滚。

Usage:
    python3 autopilot/restore.py --repo local --list
    python3 autopilot/restore.py --repo oss --list
    python3 autopilot/restore.py --repo oss --latest --target /tmp/wqm-restore
    # 回写活库（先停 daemon）：
    bash autopilot/stop.sh
    python3 autopilot/restore.py --repo local --latest --target /tmp/wqm-restore --apply-live --force
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from backup import (  # noqa: E402
    BASE,
    DEFAULT_DB_PATH,
    SNAPSHOT_PATH,
    get_target,
    human_size,
    load_env,
    run_restic,
    setup_logging,
)
from backup import logger  # noqa: E402

MAX_LIST_ENTRIES = 200


def list_restored_files(target_dir: Path) -> None:
    entries = sorted(target_dir.rglob('*'))
    logger.info(f'目标目录共 {len(entries)} 项:')
    for entry in entries[:MAX_LIST_ENTRIES]:
        rel = entry.relative_to(target_dir)
        if entry.is_dir():
            logger.info(f'  {rel}/')
        else:
            logger.info(f'  {rel} ({human_size(entry.stat().st_size)})')
    if len(entries) > MAX_LIST_ENTRIES:
        logger.info(f'  ... 其余 {len(entries) - MAX_LIST_ENTRIES} 项省略')


def daemon_running() -> bool:
    try:
        r = subprocess.run(['pgrep', '-f', 'miner_loop.py'],
                           capture_output=True, text=True, timeout=10)
        return r.returncode == 0 and bool(r.stdout.strip())
    except Exception:
        return False


def integrity_check(db_path: Path) -> bool:
    try:
        con = sqlite3.connect(str(db_path), timeout=30)
        try:
            row = con.execute('PRAGMA integrity_check').fetchone()
            return row is not None and row[0] == 'ok'
        finally:
            con.close()
    except Exception as exc:
        logger.error(f'integrity_check 异常: {exc}')
        return False


def apply_live_from_restored(target_dir: Path, live_db: Path) -> int:
    # restic 以绝对路径保存：<target>/<abs path without leading />/...
    candidates = list(target_dir.rglob('results.db'))
    if not candidates:
        logger.error(f'恢复目录无 results.db：{target_dir}（可用 ls 核对 restic 保存路径）')
        return 1
    src = candidates[0]
    logger.info(f'待回写快照: {src} ({human_size(src.stat().st_size)}) → {live_db}')
    if not integrity_check(src):
        logger.error('快照 integrity_check 失败，拒绝回写')
        return 1
    bak = live_db.with_name(f'{live_db.name}.pre-restore-{__import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")}.bak')
    try:
        shutil.copy2(live_db, bak)
        logger.info(f'活库已备份: {bak}')
    except OSError as exc:
        logger.error(f'活库备份失败，拒绝回写: {exc}')
        return 1
    try:
        shutil.copy2(src, live_db)
        for suffix in ('-wal', '-shm', '-journal'):
            p = live_db.parent / (live_db.name + suffix)
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
    except OSError as exc:
        logger.error(f'拷回失败，尝试回滚: {exc}')
        try:
            shutil.copy2(bak, live_db)
        except OSError:
            pass
        return 1
    if not integrity_check(live_db):
        logger.error('回写后 integrity_check 失败，已回滚旧库')
        try:
            shutil.copy2(bak, live_db)
        except OSError:
            pass
        return 1
    logger.info(f'回写成功（旧库留存 {bak}，确认无误后可手动删除）')
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description='wqm 恢复：列快照/恢复到临时目录/可选回写活库')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--list', action='store_true', help='列出所有快照')
    g.add_argument('--latest', action='store_true', help='恢复最新快照')
    g.add_argument('--snapshot', metavar='ID', help='恢复指定快照 ID')
    p.add_argument('--repo', choices=('local', 'oss'), default='local')
    p.add_argument('--target', help='恢复目标目录（--latest/--snapshot 必填）')
    p.add_argument('--db-path', default=str(DEFAULT_DB_PATH), help='活库路径（--apply-live 用）')
    p.add_argument('--apply-live', action='store_true', help='将恢复出的快照拷回活库（危险操作）')
    p.add_argument('--force', action='store_true', help='确认已知晓活库将被替换')
    p.add_argument('--verbose', action='store_true')
    args = p.parse_args()

    setup_logging(args.verbose)
    load_env()

    try:
        if shutil.which('restic') is None:
            raise RuntimeError('restic 未找到：sudo apt install restic')
        target = get_target(args.repo)
        logger.info(f'仓库 [{target.name}]: {target.repository}')

        if args.list:
            run_restic(['snapshots', '--tag', 'wqm-backup'], target)
            return 0

        if args.apply_live:
            if not args.force:
                logger.error('拒绝回写：需同时加 --force（确认活库将被替换）')
                return 1
            if daemon_running():
                logger.error('拒绝回写：miner_loop 仍在跑，先执行 bash autopilot/stop.sh 停 daemon')
                return 1

        if not args.target:
            p.error('--latest/--snapshot 需要 --target')
        snapshot_id = 'latest' if args.latest else args.snapshot
        target_dir = Path(args.target).expanduser().resolve()
        logger.info(f'待恢复快照: {snapshot_id} → {target_dir}')
        run_restic(['snapshots', snapshot_id], target)
        # 快照内只有单文件 backups/results.db；全量 restore 后按文件名定位
        run_restic(['restore', snapshot_id, '--target', str(target_dir),
                    '--tag', 'wqm-backup'], target)
        logger.info('恢复完成')
        list_restored_files(target_dir)

        if args.apply_live:
            return apply_live_from_restored(target_dir, Path(args.db_path))
        else:
            logger.info(f'快照在 {target_dir}（未回写活库 {args.db_path}；回写需 --apply-live --force 且先停 daemon）')
        return 0
    except Exception as exc:
        logger.exception(f'恢复失败: {exc}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
