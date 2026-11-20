"""死信重放确认明细领域服务：重放回执回填、消费确认与批次结清。

原有的死信重放在投递时就把处置状态改成「已重放」，但消息此时还没有被来源
队列真正消费，重启或并发都会让处置台账与真实消费结果脱节。本服务把
「重放」和「消费确认」拆成两步：

    待消费 --(收到消费回执)--> 等待本轮结清 --(本轮结清)--> 已确认

* 重放只生成确认明细，死信记录继续保持「待处置」；
* 消费回执按（来源队列 + 原始偏移）幂等登记，并发/重启只确认一份结果；
* 明细与原偏移、消费凭证一起落 JSON 存储，重启后仍可查询；
* 结清后才把死信记录写回「已重放」，并回写消费凭证。

跨模块关联：dlq_handler（处置台账）、mq_consumer（消费回执）、call_trace_log（追踪）。
"""
from __future__ import annotations

import hashlib
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 状态机：待消费 -> 等待本轮结清 -> 已确认；任一非终态都可以作废。
STATUS_PENDING = "待消费"
STATUS_AWAITING = "等待本轮结清"
STATUS_CONFIRMED = "已确认"
STATUS_VOID = "已作废"

ACTIVE_STATUSES = (STATUS_PENDING, STATUS_AWAITING, STATUS_CONFIRMED)

# 并发重放同一（队列, 偏移）时，只允许一个线程创建/确认回执。
_ACK_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_offset(value: Any) -> int | None:
    """把各种来源的偏移归一成整数，无法识别时返回 None。"""
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _receipt_key(source_queue: str, offset: int) -> str:
    return f"{str(source_queue).strip()}|{int(offset)}"


class DlqReplayAckService(BaseModuleService):
    MODULE = "dlq_replay_ack"
    ENTITY = "重放确认明细"
    OPERATION_TERM = "确认"
    LIST_FIELDS = [
        "明细编号", "记录编号", "原始队列", "原始偏移", "消费组名",
        "重放批次", "回执状态", "消费凭证", "回执时间",
    ]
    REQUIRED_FIELDS = ["记录编号", "原始队列", "原始偏移"]
    STATUS_ORDER = [STATUS_PENDING, STATUS_AWAITING, STATUS_CONFIRMED, STATUS_VOID]
    ACTION_RULES = {"确认回执": STATUS_CONFIRMED, "结清本轮": STATUS_CONFIRMED, "作废明细": STATUS_VOID}
    NEGATIVE_ACTIONS = []

    # ── 查询 ────────────────────────────────────────────────

    def _all_receipts(self) -> list[dict[str, Any]]:
        rows = json_store.get_all(self.MODULE)
        rows.sort(key=lambda row: int(row.get("id", 0)))
        return rows

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """确认明细直接读持久化数据，重启/换实例后仍能重新打开查看。"""
        rows = self._all_receipts()
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                needle = expected.casefold()
                rows = [
                    row for row in rows
                    if needle in str(row.get(field, "")).casefold()
                ]
        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if any(needle in str(row.get(field, "")).casefold() for field in self.LIST_FIELDS)
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return json_store.get_by_id(self.MODULE, entry_id)

    def list_details(
        self,
        *,
        status: str | None = None,
        source_queue: str | None = None,
        offset: int | str | None = None,
        batch_id: str | None = None,
        page: int = 1,
        size: int = 100,
    ) -> dict[str, Any]:
        """按状态/队列/原始偏移/批次查询确认明细。"""
        rows = self._all_receipts()
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if source_queue:
            rows = [row for row in rows if str(row.get("原始队列", "")) == str(source_queue)]
        if offset is not None:
            wanted = _as_offset(offset)
            rows = [row for row in rows if _as_offset(row.get("原始偏移")) == wanted]
        if batch_id:
            rows = [row for row in rows if str(row.get("重放批次", "")) == str(batch_id)]
        total = len(rows)
        start = (page - 1) * size
        return {
            "items": rows[start:start + size],
            "total": total,
            "page": page,
            "size": size,
            "by_status": self._count_by_status(rows),
        }

    # 方法别名，方便不同调用方按语义选取。
    get_details = list_details
    query_details = list_details

    def find_by_offset(self, source_queue: str, offset: int | str) -> dict[str, Any] | None:
        """按来源队列 + 原始偏移重查关联明细（重启后的入口）。"""
        wanted = _as_offset(offset)
        if wanted is None:
            return None
        matches = [
            row for row in self._all_receipts()
            if str(row.get("原始队列", "")) == str(source_queue)
            and _as_offset(row.get("原始偏移")) == wanted
            and row.get("status") != STATUS_VOID
        ]
        return matches[-1] if matches else None

    lookup_offset = find_by_offset

    def list_pending_for_delivery(
        self,
        *,
        batch_id: str | None = None,
        source_queue: str | None = None,
        offsets: list[int] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """返回仍可投递的明细：只包含「待消费」，已收到回执的不再投递。"""
        rows = [
            row for row in self._all_receipts()
            if row.get("status") == STATUS_PENDING
        ]
        if batch_id:
            rows = [row for row in rows if str(row.get("重放批次", "")) == str(batch_id)]
        if source_queue:
            rows = [row for row in rows if str(row.get("原始队列", "")) == str(source_queue)]
        if offsets:
            wanted = {_as_offset(item) for item in offsets}
            rows = [row for row in rows if _as_offset(row.get("原始偏移")) in wanted]
        if limit is not None:
            rows = rows[:max(0, int(limit))]
        return rows

    # ── 回填 / 登记 ─────────────────────────────────────────

    def backfill_replay_receipts(
        self,
        dlq_ids: list[int] | None = None,
        batch_id: str | None = None,
        *,
        only_pending: bool = True,
    ) -> dict[str, Any]:
        """从待处置死信数据回填重放回执，形成死信重放确认明细。

        对每条死信保留原始队列与原始偏移；已经有活动回执的（例如同一批并发
        重放）不再新建，直接返回已有明细，保证对外只有一份消费结果。
        """
        batch_id = batch_id or f"BACKFILL-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        rows = self._ledger_entries()
        if dlq_ids:
            wanted = {int(item) for item in dlq_ids}
            rows = [row for row in rows if int(row.get("id", 0)) in wanted]

        if only_pending:
            # 可回填的是还没推进到终态的死信，草稿与待处置都算。
            rows = [row for row in rows if row.get("status") in ("草稿", "待处置")]

        created = 0
        existing = 0
        failed = 0
        details: list[dict[str, Any]] = []

        for row in rows:
            result = self.register_replay_receipt(
                dlq_id=int(row.get("id", 0)),
                source_queue=str(row.get("原始队列", "unknown")),
                offset=row.get("原始偏移"),
                consumer_group=str(row.get("消费组名", "")),
                batch_id=batch_id,
                record_no=str(row.get("记录编号", "")),
                message=str(row.get("原始消息体", "")),
                entry=row,
            )
            if not result.get("ok"):
                failed += 1
                details.append(result)
                continue
            if result.get("duplicate"):
                existing += 1
            else:
                created += 1
            details.append(result)

        return {
            "ok": True,
            "batch_id": batch_id,
            "total": len(rows),
            "created": created,
            "existing": existing,
            "failed": failed,
            "details": details,
            "items": [item.get("entry") for item in details if item.get("entry")],
            "message": (
                f"回填重放回执完成: 新建 {created}, 复用 {existing}, 失败 {failed}"
            ),
        }

    # 常用别名。
    backfill_receipts = backfill_replay_receipts
    backfill_from_pending = backfill_replay_receipts

    def resolve_offset(self, entry: dict[str, Any] | None = None,
                       source_queue: str = "", offset: Any = None) -> tuple[int, str]:
        """确定原消息偏移：显式偏移 > 消息体里的 offset > 同队列已登记的最大偏移 + 1。

        返回 (偏移, 偏移来源)，来源会写进明细，便于值班员判断偏移是否回填推导。
        """
        explicit = _as_offset(offset)
        if explicit is not None:
            return explicit, "原始回执"

        if entry:
            stored = _as_offset(entry.get("原始偏移", entry.get("_offset")))
            if stored is not None:
                return stored, "死信记录"
            body = entry.get("原始消息体")
            parsed = self._offset_from_payload(body)
            if parsed is not None:
                return parsed, "消息体解析"

        queue = str(source_queue or (entry or {}).get("原始队列", ""))
        known: list[int] = []
        for row in self._all_receipts():
            if str(row.get("原始队列", "")) == queue:
                value = _as_offset(row.get("原始偏移"))
                if value is not None:
                    known.append(value)
        if queue:
            for row in self._ledger_entries():
                if str(row.get("原始队列", "")) == queue:
                    value = _as_offset(row.get("原始偏移"))
                    if value is not None:
                        known.append(value)
        return (max(known) + 1 if known else 0), "回填推导"

    @staticmethod
    def _offset_from_payload(body: Any) -> int | None:
        if not body:
            return None
        try:
            payload = json.loads(body) if isinstance(body, str) else body
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        direct = _as_offset(payload.get("offset"))
        if direct is not None:
            return direct
        data = payload.get("data")
        if isinstance(data, dict):
            marker = _as_offset(data.get("offset"))
            if marker is not None:
                return marker
            # 消费服务生成的消息 data.id = offset + 1
            data_id = _as_offset(data.get("id"))
            if data_id is not None and data_id > 0:
                return data_id - 1
        return None

    def register_replay_receipt(
        self,
        *,
        dlq_id: int = 0,
        source_queue: str,
        offset: Any = None,
        consumer_group: str = "",
        batch_id: str | None = None,
        record_no: str = "",
        message: str = "",
        entry: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """登记一条重放确认明细；同一（队列, 偏移）只保留一份活动回执。"""
        queue = str(source_queue or "unknown").strip() or "unknown"
        resolved, offset_source = self.resolve_offset(entry=entry, source_queue=queue, offset=offset)
        batch_id = batch_id or f"REPLAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        key = _receipt_key(queue, resolved)

        with _ACK_LOCK:
            existing = self._find_active_by_key(queue, resolved)
            if (existing is not None and dlq_id and offset_source == "回填推导"
                    and int(existing.get("_dlq_id", 0) or existing.get("死信记录ID", 0) or 0)
                    != int(dlq_id)):
                # 推导出来的偏移撞上了别的死信记录：顺延到下一个空闲偏移，
                # 否则两条不同消息会被合并成一份回执。
                while self._find_active_by_key(queue, resolved) is not None:
                    resolved += 1
                existing = None
                key = _receipt_key(queue, resolved)
            if existing is None and dlq_id:
                # 同一条死信重放多轮时沿用已有明细，避免每次推导偏移都新增一份。
                existing = self._find_active_by_dlq(int(dlq_id))
            if existing is not None:
                stored_offset = _as_offset(existing.get("原始偏移"))
                if stored_offset is not None:
                    resolved = stored_offset
                return {
                    "ok": True,
                    "duplicate": True,
                    "detail_id": existing.get("id"),
                    "receipt_id": existing.get("id"),
                    "batch_id": existing.get("重放批次", batch_id),
                    "offset": resolved,
                    "offset_source": existing.get("偏移来源", offset_source),
                    "status": existing.get("status", STATUS_PENDING),
                    "entry": existing,
                    "message": f"来源队列 {queue} 偏移 {resolved} 已有活动确认明细 #{existing.get('id')}，复用同一份回执",
                }

            max_id = max((int(row.get("id", 0)) for row in self._all_receipts()), default=0)
            detail_no = (
                f"ACK-{queue[:6].upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
                f"-{max_id + 1:05d}"
            )
            detail = {
                "id": max_id + 1,
                "明细编号": detail_no,
                "记录编号": record_no or f"DLQ-{dlq_id:05d}",
                "死信记录ID": str(dlq_id or ""),
                "原始队列": queue,
                "原始偏移": str(resolved),
                "消费组名": str(consumer_group or ""),
                "重放批次": batch_id,
                "回执状态": STATUS_PENDING,
                "消费凭证": "",
                "消费结论": "",
                "回执时间": "",
                "结清时间": "",
                "重试次数": "0",
                "续投次数": "0",
                "偏移来源": offset_source,
                "原始消息体": str(message or "")[:200],
                "status": STATUS_PENDING,
                "pending": True,
                "abnormal": False,
                "created_at": _now(),
                "_queue": queue,
                "_offset": resolved,
                "_dlq_id": int(dlq_id or 0),
                "_receipt_key": key,
            }
            json_store.insert(self.MODULE, detail)

            # 回执还没确认前，死信处置台账保持「待处置」，只记录关联批次。
            ledger_updates: dict[str, Any] = {
                "重放批次": batch_id,
                "确认状态": STATUS_PENDING,
                "重放回执": "",
                "最近重放时间": detail["created_at"],
                "待确认明细": str(detail["id"]),
            }
            if entry and str(entry.get("status", "")) in ("草稿", STATUS_PENDING):
                # 草稿一旦进入重放就落到待处置，等回执回来后才会推进。
                if str(entry.get("status", "")) != "待处置":
                    ledger_updates.update({
                        "status": "待处置",
                        "处置状态": "待处置",
                        "pending": True,
                    })
            self._update_dlq_ledger(int(dlq_id or 0), ledger_updates)

            return {
                "ok": True,
                "duplicate": False,
                "detail_id": detail["id"],
                "receipt_id": detail["id"],
                "batch_id": batch_id,
                "offset": resolved,
                "offset_source": offset_source,
                "status": STATUS_PENDING,
                "entry": json_store.get_by_id(self.MODULE, int(detail["id"])) or detail,
                "message": f"已登记重放确认明细 {detail_no}，待来源队列回执",
            }

    # 别名
    register_receipt = register_replay_receipt

    # ── 消费回执 ────────────────────────────────────────────

    def record_consumption_receipt(
        self,
        source_queue: str,
        offset: int | str,
        *,
        receipt: Any = None,
        consumer_group: str = "",
        consumer_id: int = 0,
        success: bool = True,
        batch_id: str | None = None,
        note: str = "",
        create_if_missing: bool = True,
    ) -> dict[str, Any]:
        """登记来源队列的实际消费回执。

        幂等键是（来源队列, 原始偏移）：并发或重启后重复登记只会返回首次的
        消费凭证，不会产生第二份处理结果。
        """
        queue = str(source_queue or "unknown").strip() or "unknown"
        resolved = _as_offset(offset)
        if resolved is None:
            return {"ok": False, "message": "缺少合法的原始偏移", "duplicate": False}

        voucher = self._voucher_from(receipt, queue, resolved, success)

        with _ACK_LOCK:
            detail = self._find_active_by_key(queue, resolved)
            if detail is None:
                if not create_if_missing:
                    return {
                        "ok": False, "duplicate": False, "offset": resolved,
                        "message": f"来源队列 {queue} 偏移 {resolved} 没有待确认的重放明细",
                    }
                registered = self.register_replay_receipt(
                    source_queue=queue,
                    offset=resolved,
                    consumer_group=consumer_group,
                    batch_id=batch_id or f"ADHOC-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                    message=note,
                )
                detail = registered.get("entry") or {}

            detail_id = int(detail.get("id", 0))
            current = str(detail.get("status", STATUS_PENDING))

            # 已经确认过或已经收到回执：直接返回同一份凭证，不再处理一次。
            if current in (STATUS_AWAITING, STATUS_CONFIRMED):
                return {
                    "ok": True,
                    "duplicate": True,
                    "reprocessed": False,
                    "detail_id": detail_id,
                    "receipt_id": detail_id,
                    "offset": resolved,
                    "offset_source": detail.get("偏移来源", ""),
                    "status": current,
                    "voucher": detail.get("消费凭证", ""),
                    "entry": detail,
                    "message": f"来源队列 {queue} 偏移 {resolved} 已收到消费回执，复用凭证 {detail.get('消费凭证', '')}",
                }

            if current == STATUS_VOID:
                return {
                    "ok": False, "duplicate": False, "offset": resolved,
                    "detail_id": detail_id,
                    "message": f"确认明细 #{detail_id} 已作废，不再接收消费回执",
                }

            # 当前是「待消费」：消费失败时保持待消费，成功的回执进入等待本轮结清。
            if not success:
                retry = int(detail.get("重试次数", "0") or "0") + 1
                json_store.update(self.MODULE, detail_id, {
                    "重试次数": str(retry),
                    "消费结论": "消费失败",
                    "最近失败原因": note or "来源队列消费失败",
                    "最近回执时间": _now(),
                    "abnormal": True,
                    "status": STATUS_PENDING,
                    "回执状态": STATUS_PENDING,
                    "pending": True,
                    "_last_receipt": self._receipt_payload(receipt),
                })
                self._update_dlq_ledger(int(detail.get("_dlq_id", 0) or detail.get("死信记录ID", 0) or 0), {
                    "确认状态": STATUS_PENDING,
                    "消费结论": "消费失败",
                    "最近回执": voucher,
                    "回执时间": _now(),
                })
                return {
                    "ok": True,
                    "duplicate": False,
                    "reprocessed": False,
                    "detail_id": detail_id,
                    "offset": resolved,
                    "status": STATUS_PENDING,
                    "voucher": voucher,
                    "entry": json_store.get_by_id(self.MODULE, detail_id),
                    "message": f"来源队列 {queue} 偏移 {resolved} 消费失败，保持待消费（retry#{retry}）",
                }

            updated = json_store.update(self.MODULE, detail_id, {
                "status": STATUS_AWAITING,
                "回执状态": STATUS_AWAITING,
                "消费凭证": voucher,
                "消费结论": "消费成功",
                "回执时间": _now(),
                "pending": True,
                "abnormal": False,
                "消费组名": consumer_group or detail.get("消费组名", ""),
                "消费端": str(consumer_id or ""),
                "_last_receipt": self._receipt_payload(receipt),
            })
            self._update_dlq_ledger(int(detail.get("_dlq_id", 0) or detail.get("死信记录ID", 0) or 0), {
                "确认状态": STATUS_AWAITING,
                "消费结论": "消费成功",
                "最近回执": voucher,
                "回执时间": _now(),
                "重放回执": voucher,
            })
            return {
                "ok": True,
                "duplicate": False,
                "reprocessed": True,
                "detail_id": detail_id,
                "receipt_id": detail_id,
                "offset": resolved,
                "offset_source": detail.get("偏移来源", ""),
                "status": STATUS_AWAITING,
                "voucher": voucher,
                "entry": updated,
                "message": f"来源队列 {queue} 偏移 {resolved} 已收到消费回执，等待本轮结清",
            }

    # 别名
    record_receipt = record_consumption_receipt
    acknowledge = record_consumption_receipt

    # ── 本轮续投与结清 ──────────────────────────────────────

    def resume_round(
        self,
        batch_id: str | None = None,
        *,
        source_queue: str | None = None,
        consumer_id: int = 0,
    ) -> dict[str, Any]:
        """服务重启后从处置入口继续处理本轮剩余消息。

        只返回仍处于「待消费」的明细；已收到实际消费回执的明细哪怕本轮还没
        结清，也不会再被投递，避免重复处理。
        """
        with _ACK_LOCK:
            pending = self.list_pending_for_delivery(
                batch_id=batch_id, source_queue=source_queue)
            waiting = [
                row for row in self._all_receipts()
                if row.get("status") == STATUS_AWAITING
                and (not batch_id or str(row.get("重放批次", "")) == str(batch_id))
                and (not source_queue or str(row.get("原始队列", "")) == str(source_queue))
            ]
            confirmed = [
                row for row in self._all_receipts()
                if row.get("status") == STATUS_CONFIRMED
                and (not batch_id or str(row.get("重放批次", "")) == str(batch_id))
                and (not source_queue or str(row.get("原始队列", "")) == str(source_queue))
            ]
            for row in pending:
                retry = int(row.get("续投次数", "0") or "0") + 1
                json_store.update(self.MODULE, int(row["id"]), {
                    "续投次数": str(retry),
                    "最近续投时间": _now(),
                })
                row["续投次数"] = str(retry)

        result = {
            "ok": True,
            "batch_id": batch_id or "",
            "resumed": len(pending),
            "awaiting_settlement": len(waiting),
            "already_confirmed": len(confirmed),
            "deliverable": [
                {
                    "detail_id": int(row.get("id", 0)),
                    "source_queue": row.get("原始队列", ""),
                    "offset": _as_offset(row.get("原始偏移")),
                    "status": row.get("status", ""),
                    "续投次数": row.get("续投次数", "0"),
                }
                for row in pending
            ],
            "pending_items": pending,
            "waiting_items": waiting,
            "message": (
                f"本轮剩余可投递 {len(pending)} 条，已有消费回执 {len(waiting)} 条不再重复投递"
            ),
        }

        if consumer_id:
            from app.services.mq_consumer import MqConsumerService
            result["consumption"] = MqConsumerService().consume_replay(
                consumer_id=consumer_id, batch_id=batch_id)
        return result

    continue_round = resume_round
    continue_replay = resume_round

    def settle_round(self, batch_id: str) -> dict[str, Any]:
        """结清本轮：把已消费的明细确认为「已确认」，并回写死信处置台账。"""
        if not batch_id:
            return {"ok": False, "message": "缺少重放批次", "confirmed": 0, "pending": 0}

        confirmed_now = 0
        already = 0
        pending = 0
        results: list[dict[str, Any]] = []

        with _ACK_LOCK:
            for row in self._all_receipts():
                if str(row.get("重放批次", "")) != str(batch_id):
                    continue
                status = row.get("status", "")
                if status == STATUS_AWAITING:
                    detail_id = int(row.get("id", 0))
                    updated = json_store.update(self.MODULE, detail_id, {
                        "status": STATUS_CONFIRMED,
                        "回执状态": STATUS_CONFIRMED,
                        "结清时间": _now(),
                        "pending": False,
                        "abnormal": False,
                    })
                    self._update_dlq_ledger(
                        int(row.get("_dlq_id", 0) or row.get("死信记录ID", 0) or 0),
                        {
                            "status": "已重放",
                            "处置状态": "已重放",
                            "确认状态": STATUS_CONFIRMED,
                            "重放批次": batch_id,
                            "重放回执": row.get("消费凭证", ""),
                            "消费结论": "消费成功",
                            "待确认明细": str(detail_id),
                            "结清时间": _now(),
                            "pending": False,
                            "abnormal": False,
                        },
                    )
                    confirmed_now += 1
                    results.append({
                        "detail_id": detail_id,
                        "offset": _as_offset(row.get("原始偏移")),
                        "status": STATUS_CONFIRMED,
                        "voucher": row.get("消费凭证", ""),
                        "entry": updated,
                    })
                elif status == STATUS_CONFIRMED:
                    already += 1
                elif status == STATUS_PENDING:
                    pending += 1

        return {
            "ok": True,
            "batch_id": batch_id,
            "confirmed": confirmed_now,
            "already_confirmed": already,
            "pending": pending,
            "settled": pending == 0,
            "results": results,
            "message": (
                f"本轮结清完成: 本次确认 {confirmed_now} 条，仍有 {pending} 条待消费"
                if pending else f"本轮已结清: 确认 {confirmed_now} 条"
            ),
        }

    settle_batch = settle_round
    close_round = settle_round

    def get_round_summary(self, batch_id: str) -> dict[str, Any]:
        rows = [row for row in self._all_receipts()
                if str(row.get("重放批次", "")) == str(batch_id)]
        by_status = self._count_by_status(rows)
        return {
            "ok": True,
            "batch_id": batch_id,
            "total": len(rows),
            "by_status": by_status,
            "pending_consumption": by_status.get(STATUS_PENDING, 0),
            "awaiting_settlement": by_status.get(STATUS_AWAITING, 0),
            "confirmed": by_status.get(STATUS_CONFIRMED, 0),
            "settled": by_status.get(STATUS_PENDING, 0) == 0
                        and by_status.get(STATUS_AWAITING, 0) == 0,
            "items": rows,
        }

    # ── 通用资源动作 ────────────────────────────────────────

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        offset = _as_offset(values.get("原始偏移"))
        if offset is None:
            return None, ["原始偏移"]

        dlq_id = _as_offset(values.get("死信记录ID")) or 0
        result = self.register_replay_receipt(
            dlq_id=dlq_id,
            source_queue=str(values.get("原始队列", "")),
            offset=offset,
            consumer_group=str(values.get("消费组名", "")),
            batch_id=str(values.get("重放批次", "") or "") or None,
            record_no=str(values.get("记录编号", "")),
            message=str(values.get("原始消息体", "")),
        )
        if not result.get("ok"):
            return None, [result.get("message", "登记确认明细失败")]
        return result.get("entry"), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "确认回执":
            result = self.record_consumption_receipt(
                source_queue=str(entry.get("原始队列", "")),
                offset=entry.get("原始偏移"),
                consumer_group=str(entry.get("消费组名", "")),
                batch_id=str(entry.get("重放批次", "") or ""),
                note="人工确认消费回执",
            )
            return result.get("entry"), result.get("message", "")

        if action == "结清本轮":
            batch = str(entry.get("重放批次", ""))
            summary = self.settle_round(batch) if batch else {"message": "明细没有关联重放批次"}
            return self.get_entry(entry_id), summary.get("message", "")

        if action == "作废明细":
            updated = json_store.update(self.MODULE, entry_id, {
                "status": STATUS_VOID,
                "回执状态": STATUS_VOID,
                "pending": False,
                "abnormal": False,
            })
            self._update_dlq_ledger(
                int(entry.get("_dlq_id", 0) or entry.get("死信记录ID", 0) or 0),
                {"确认状态": STATUS_VOID},
            )
            return updated, f"{self.ENTITY}已作废"

        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        updated = json_store.update(self.MODULE, entry_id, {
            "status": target,
            "回执状态": target,
            "pending": target != self.STATUS_ORDER[-1],
            "abnormal": False,
        })
        return updated, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        rows = self._all_receipts()
        by_status = self._count_by_status(rows)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信重放确认明细数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
            "pending_consumption": by_status.get(STATUS_PENDING, 0),
            "awaiting_settlement": by_status.get(STATUS_AWAITING, 0),
            "confirmed": by_status.get(STATUS_CONFIRMED, 0),
            "operation": self.OPERATION_TERM,
        }

    # ── 内部工具 ────────────────────────────────────────────

    def _count_by_status(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        counts = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            status = str(row.get("status", ""))
            if status in counts:
                counts[status] += 1
        return counts

    def _find_active_by_key(self, source_queue: str, offset: int) -> dict[str, Any] | None:
        key = _receipt_key(source_queue, offset)
        for row in self._all_receipts():
            if row.get("status") == STATUS_VOID:
                continue
            if row.get("_receipt_key") == key:
                return row
            if (str(row.get("原始队列", "")) == str(source_queue)
                    and _as_offset(row.get("原始偏移")) == int(offset)):
                return row
        return None

    def _find_active_by_dlq(self, dlq_id: int) -> dict[str, Any] | None:
        matches = [
            row for row in self._all_receipts()
            if row.get("status") != STATUS_VOID
            and int(row.get("_dlq_id", 0) or row.get("死信记录ID", 0) or 0) == int(dlq_id)
        ]
        return matches[-1] if matches else None

    def _update_dlq_ledger(self, dlq_id: int, updates: dict[str, Any]) -> None:
        """把确认结论回写死信处置台账。"""
        if not dlq_id:
            return
        try:
            json_store.update("dlq_handler", int(dlq_id), dict(updates))
        except Exception:
            pass

    @staticmethod
    def _ledger_entries() -> list[dict[str, Any]]:
        """读取已持久化的死信处置台账，作为回填回执的唯一来源。"""
        return list(json_store.get_all("dlq_handler"))

    @staticmethod
    def _voucher_from(receipt: Any, source_queue: str, offset: int, success: bool) -> str:
        if isinstance(receipt, str) and receipt.strip():
            return receipt.strip()
        if isinstance(receipt, dict):
            for field in ("消费凭证", "凭证", "receipt_id", "voucher", "ack_id", "receipt"):
                value = receipt.get(field)
                if value:
                    return str(value)
        prefix = "ACK" if success else "NACK"
        digest = hashlib.sha256(f"{source_queue}|{offset}".encode()).hexdigest()[:8]
        return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{digest}-{uuid.uuid4().hex[:4]}"

    @staticmethod
    def _receipt_payload(receipt: Any) -> Any:
        if isinstance(receipt, dict):
            return dict(receipt)
        if receipt is None:
            return ""
        return str(receipt)
