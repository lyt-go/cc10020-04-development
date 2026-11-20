"""死信队列管理领域服务：失败消息隔离、原因分类分析、批量重放与处置策略。

提供完整的死信处理流程：消息入队→原因归类→处置决策→批量重放→消费确认→结果验证。
跨模块关联：mq_consumer（来源队列与消费回执）、dlq_replay_ack（重放确认明细）、
call_trace_log（重放追踪）、data_quality_rule（质量校验）。

重放与确认被拆成两步：重放只登记确认明细并保持「待处置」，来源队列真正消费
回执回来后由确认明细结清，才把处置状态写成「已重放」。这样重启、并发重放都
不会在消息还没重新处理时就把处置状态结束掉。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


def _retry_count(row: dict[str, Any]) -> int:
    """示例数据里的重试次数是占位文本，按 0 兜底。"""
    try:
        return int(str(row.get("重试次数", "0") or "0").strip())
    except (TypeError, ValueError):
        return 0


class DlqHandlerService(BaseModuleService):
    MODULE = "dlq_handler"
    ENTITY = "死信记录"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["记录编号", "原始队列", "失败原因", "原始消息体", "重试次数", "入队时间", "处置状态", "重放批次"]
    REQUIRED_FIELDS = ["记录编号", "原始队列", "失败原因"]
    STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
    ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._replay_history: list[dict[str, Any]] = []

    # ── 台账读取：持久化数据优先，示例内存数据补足 ────────────

    def _ledger_rows(self) -> list[dict[str, Any]]:
        rows = [dict(row) for row in json_store.get_all(self.MODULE)]
        rows.sort(key=lambda row: int(row.get("id", 0)))
        return rows

    def list_entries(self, *, filters: dict[str, str] | None = None,
                     keyword: str | None = None, status: str | None = None,
                     page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows = self._ledger_rows()
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                needle = expected.casefold()
                rows = [row for row in rows if needle in str(row.get(field, "")).casefold()]
        if keyword:
            needle = keyword.casefold()
            rows = [row for row in rows if needle in str(row.get("记录编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        for row in self._ledger_rows():
            if int(row.get("id", 0)) == int(entry_id):
                return row
        return None

    def _update_ledger(self, dlq_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
        return json_store.update(self.MODULE, int(dlq_id), dict(updates))

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """手工登记死信，同样落持久化台账并保留原始偏移。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        now = datetime.now(timezone.utc).isoformat()
        entry = {"id": 0}
        entry.update({field: values.get(field, "") for field in self.LIST_FIELDS})
        entry.update({
            "处置状态": "草稿",
            "重放批次": "",
            "确认状态": "待回填",
            "原始偏移": str(values.get("原始偏移", "") or ""),
            "消费组名": str(values.get("消费组名", "") or ""),
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "created_at": now,
        })
        inserted = json_store.insert(self.MODULE, entry)
        return inserted, []

    @classmethod
    def ensure_seed_data(cls) -> int:
        """首次启动时把示例死信落成持久化台账，之后只读写持久化数据。"""
        if json_store.get_all(cls.MODULE):
            return 0
        from app.store import store
        rows = [dict(row) for row in store.rows(cls.MODULE)]
        if not rows:
            return 0
        for row in rows:
            json_store.insert(cls.MODULE, row)
        return len(rows)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """处置动作。重放不再直接改「已重放」，而是先登记确认明细等消费回执。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "标记重放":
            batch_id = str(entry.get("重放批次", "") or "") or None
            result = self.batch_replay([int(entry_id)], batch_id=batch_id,
                                       success_offsets=None, deliver=False)
            detail = result.get("results", [{}])[0] if result.get("results") else {}
            return self.get_entry(entry_id), (
                f"已投递到来源队列并登记确认明细 #{detail.get('detail_id', '')}，"
                f"死信保持待处置，等待消费回执"
            )

        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        updates = {
            "status": target,
            "处置状态": target,
            "pending": target != self.STATUS_ORDER[-1],
            "abnormal": action in self.NEGATIVE_ACTIONS,
        }
        if target == "待处置":
            updates["确认状态"] = "待回填"
        return self._update_ledger(entry_id, updates), f"{self.ENTITY}已{action}"

    def enqueue_dead_letter(self, source_queue: str, original_message: str,
                            failure_reason: str, consumer_id: int = 0,
                            offset: int | str | None = None,
                            consumer_group: str = "") -> dict[str, Any]:
        """将消费失败的消息加入死信队列。

        自动生成记录编号、消息指纹、入队时间戳和失败原因分类，并保留原消息在
        来源队列里的偏移；偏移是重放回执的幂等键，服务重启后仍可据此重查。
        """
        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)

        msg_fingerprint = hashlib.sha256(original_message.encode()).hexdigest()[:16]

        # 检查是否有相同指纹的已有死信（避免重复入队）
        duplicate = [e for e in existing if e.get("_fingerprint") == msg_fingerprint and e.get("status") in ("草稿", "待处置")]
        if duplicate:
            existing_dlq = duplicate[0]
            retry_count = _retry_count(existing_dlq) + 1
            json_store.update(self.MODULE, int(existing_dlq["id"]), {
                "重试次数": str(retry_count),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            return {
                "ok": True,
                "message": f"消息已在死信队列中（retry#{retry_count}），合并为已有记录 #{existing_dlq['id']}",
                "dlq_id": existing_dlq["id"],
                "is_duplicate": True,
            }

        record_no = f"DLQ-{source_queue[:4].upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{max_id + 1:05d}"

        dlq_entry = {
            "id": max_id + 1,
            "记录编号": record_no,
            "原始队列": source_queue,
            "失败原因": failure_reason,
            "原始消息体": original_message[:200],
            "重试次数": "0",
            "入队时间": datetime.now(timezone.utc).isoformat(),
            "处置状态": "草稿",
            "重放批次": "",
            "确认状态": "待回填",
            "原始偏移": "" if offset is None else str(offset),
            "消费组名": consumer_group,
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "_fingerprint": msg_fingerprint,
            "_consumer_id": consumer_id,
        }

        # 填充LIST_FIELDS中的其他字段
        for field in self.LIST_FIELDS:
            if field not in dlq_entry:
                dlq_entry[field] = dlq_entry.get("记录编号", "") if field == "记录编号" else (
                    dlq_entry.get("原始队列", "") if field == "原始队列" else
                    dlq_entry.get("失败原因", "") if field == "失败原因" else
                    dlq_entry.get("原始消息体", "") if field == "原始消息体" else
                    dlq_entry.get("重试次数", "") if field == "重试次数" else
                    dlq_entry.get("入队时间", "") if field == "入队时间" else
                    dlq_entry.get("处置状态", "") if field == "处置状态" else
                    dlq_entry.get("重放批次", "") if field == "重放批次" else ""
                )

        json_store.insert(self.MODULE, dlq_entry)
        return {"ok": True, "message": f"死信已入队，编号 {record_no}", "dlq_id": dlq_entry["id"], "is_duplicate": False}

    def analyze_failure_reasons(self) -> dict[str, Any]:
        """分析所有死信记录的失败原因分布，提供分类统计和主要问题识别。"""
        rows = json_store.get_all(self.MODULE)
        if not rows:
            return {"total": 0, "categories": {}, "top_reasons": []}

        reason_counter = Counter()
        source_counter = Counter()

        for row in rows:
            reason = str(row.get("失败原因", "未知"))
            source = str(row.get("原始队列", "未知"))
            reason_counter[reason] += 1
            source_counter[source] += 1

        # 失败原因分类
        categories = {
            "超时类": sum(1 for r, c in reason_counter.items() if "超时" in r or "timeout" in r),
            "格式类": sum(1 for r, c in reason_counter.items() if "格式" in r or "format" in r or "序列化" in r),
            "数据类": sum(1 for r, c in reason_counter.items() if "数据" in r or "data" in r or "空" in r),
            "连接类": sum(1 for r, c in reason_counter.items() if "连接" in r or "网络" in r or "connect" in r),
            "认证类": sum(1 for r, c in reason_counter.items() if "认证" in r or "auth" in r or "权限" in r),
            "其他": sum(1 for r, c in reason_counter.items() if not any(kw in r for kw in ["超时", "timeout", "格式", "format", "序列化", "数据", "data", "空", "连接", "网络", "connect", "认证", "auth", "权限"])),
        }

        return {
            "total": len(rows),
            "categories": {k: v for k, v in categories.items() if v > 0},
            "top_reasons": reason_counter.most_common(5),
            "top_sources": source_counter.most_common(5),
            "pending_count": sum(1 for r in rows if r.get("status") in ("草稿", "待处置")),
        }

    def batch_replay(self, dlq_ids: list[int], *, batch_id: str | None = None,
                     success_offsets: list[int] | None = None,
                     fail_offsets: list[int] | None = None,
                     deliver: bool = True) -> dict[str, Any]:
        """批量重放死信队列中的消息，并把结果落成重放确认明细。

        流程：
        1. 验证每条死信记录是否存在且处于可重放状态
        2. 生成重放批次ID
        3. 为每条死信登记确认明细，保留原始队列与原始偏移
        4. 投递到来源队列；只有拿到实际消费回执的明细才进入「等待本轮结清」
        5. 未拿到回执的明细保持「待消费」，死信记录继续保持「待处置」

        参数 success_offsets / fail_offsets 用于指定本轮哪些偏移会拿到消费回执；
        不指定时由来源队列模拟决定，便于复现「一条已消费、一条未消费」的现场。
        """
        if not dlq_ids:
            return {"ok": False, "message": "未指定待重放的死信记录", "replayed": 0, "failed": 0}

        batch_id = batch_id or f"REPLAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        success_offsets = None if success_offsets is None else {int(item) for item in success_offsets}
        fail_offsets = None if fail_offsets is None else {int(item) for item in fail_offsets}
        results = []
        receipts = []
        replayed = 0
        replay_failed = 0

        import random
        for dlq_id in dlq_ids:
            entry = self.get_entry(dlq_id)
            if entry is None:
                results.append({"dlq_id": dlq_id, "ok": False, "reason": "死信记录不存在"})
                replay_failed += 1
                continue

            if entry.get("status") in ("已重放", "已丢弃"):
                results.append({"dlq_id": dlq_id, "ok": False, "reason": f"记录已{entry['status']}，不可重复重放"})
                replay_failed += 1
                continue

            # 若该记录已经有活动确认明细（例如并发重放或上一轮未结清），复用它，
            # 保证同一来源队列 + 偏移只对外确认一份消费结果。
            receipt = self._ack_service().register_replay_receipt(
                dlq_id=dlq_id,
                source_queue=str(entry.get("原始队列", "unknown")),
                offset=entry.get("原始偏移"),
                consumer_group=str(entry.get("消费组名", "")),
                batch_id=batch_id,
                record_no=str(entry.get("记录编号", "")),
                message=str(entry.get("原始消息体", "")),
                entry=entry,
            )
            if not receipt.get("ok"):
                results.append({"dlq_id": dlq_id, "ok": False, "reason": receipt.get("message", "确认明细登记失败")})
                replay_failed += 1
                continue

            detail_id = int(receipt.get("detail_id", 0))
            offset = int(receipt.get("offset", 0))
            detail_status = receipt.get("status", "待消费")

            if receipt.get("duplicate") and detail_status in ("等待本轮结清", "已确认"):
                # 已有实际消费回执，绝不再投递第二次。
                replayed += 1
                results.append({
                    "dlq_id": dlq_id, "ok": True, "detail_id": detail_id, "offset": offset,
                    "status": detail_status, "duplicate": True,
                    "message": "已有消费回执，本轮不重复投递",
                })
                receipts.append(receipt)
                continue

            # 未收到过回执：登记/沿用明细后投递一次。
            self._update_ledger(dlq_id, {
                "重试次数": str(_retry_count(entry) + 1),
                "status": "待处置",
                "处置状态": "待处置",
                "确认状态": "待消费",
                "待确认明细": str(detail_id),
                "重放批次": batch_id,
                "pending": True,
                "abnormal": False,
                "replayed_at": datetime.now(timezone.utc).isoformat(),
            })

            if not deliver:
                results.append({
                    "dlq_id": dlq_id, "ok": True, "detail_id": detail_id, "offset": offset,
                    "status": "待消费", "message": "已投递，等待来源队列消费回执",
                })
                receipts.append(receipt)
                replayed += 1
                continue

            if success_offsets is not None:
                succeeds = offset in success_offsets
            elif fail_offsets is not None:
                succeeds = offset not in fail_offsets
            else:
                succeeds = random.random() < 0.7

            if succeeds:
                ack = self._ack_service().record_consumption_receipt(
                    source_queue=str(entry.get("原始队列", "unknown")),
                    offset=offset,
                    consumer_group=str(entry.get("消费组名", "")),
                    success=True,
                    batch_id=batch_id,
                    note="来源队列重放消费成功",
                )
                replayed += 1
                results.append({
                    "dlq_id": dlq_id, "ok": True, "detail_id": detail_id, "offset": offset,
                    "status": ack.get("status", "等待本轮结清"),
                    "voucher": ack.get("voucher", ""),
                    "message": "已收到消费回执，等待本轮结清",
                })
            else:
                self._ack_service().record_consumption_receipt(
                    source_queue=str(entry.get("原始队列", "unknown")),
                    offset=offset,
                    consumer_group=str(entry.get("消费组名", "")),
                    success=False,
                    batch_id=batch_id,
                    note="来源队列重放消费失败",
                )
                replay_failed += 1
                results.append({
                    "dlq_id": dlq_id, "ok": False, "detail_id": detail_id, "offset": offset,
                    "status": "待消费", "reason": "未收到消费回执，保持待处置",
                })
            receipts.append(receipt)

        # 记录重放历史
        self._replay_history.append({
            "batch_id": batch_id,
            "total": len(dlq_ids),
            "replayed": replayed,
            "failed": replay_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # 跨模块：记录重放到调用追踪
        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            tracer._record_trace(
                endpoint_id=0,
                action="死信批量重放",
                latency_ms=random.randint(100, 500),
                status="正常" if replay_failed == 0 else "异常",
                record_count=replayed,
            )
        except Exception:
            pass

        summary = self._ack_service().get_round_summary(batch_id)
        return {
            "ok": True,
            "message": (
                f"批量重放完成: 投递 {replayed}/{len(dlq_ids)}, 未拿到回执 {replay_failed}; "
                f"待消费 {summary['pending_consumption']}, 等待结清 {summary['awaiting_settlement']}"
            ),
            "batch_id": batch_id,
            "replayed": replayed,
            "failed": replay_failed,
            "total": len(dlq_ids),
            "results": results,
            "receipts": receipts,
            "pending_consumption": summary["pending_consumption"],
            "awaiting_settlement": summary["awaiting_settlement"],
            "confirmed": summary["confirmed"],
            "settled": summary["settled"],
        }

    def _ack_service(self) -> Any:
        from app.services.dlq_replay_ack import DlqReplayAckService
        return DlqReplayAckService()

    def backfill_replay_receipts(self, dlq_ids: list[int] | None = None,
                                 batch_id: str | None = None) -> dict[str, Any]:
        """从待处置死信数据回填重放回执，形成死信重放确认明细。"""
        return self._ack_service().backfill_replay_receipts(dlq_ids, batch_id)

    def get_replay_round(self, batch_id: str) -> dict[str, Any]:
        """查询一轮重放的消费确认进度，供处置入口重新打开时核对。"""
        return self._ack_service().get_round_summary(batch_id)

    def settle_replay_round(self, batch_id: str) -> dict[str, Any]:
        """本轮结清后把已消费明细确认，并回写死信处置台账。"""
        return self._ack_service().settle_round(batch_id)

    def get_dlq_stats(self) -> dict[str, Any]:
        """获取死信队列的全局统计信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {}
        by_queue = {}
        for row in rows:
            s = row.get("status", "未知")
            q = row.get("原始队列", "未知")
            by_status[s] = by_status.get(s, 0) + 1
            by_queue[q] = by_queue.get(q, 0) + 1

        return {
            "total_dlq": len(rows),
            "by_status": by_status,
            "by_queue": dict(sorted(by_queue.items(), key=lambda x: x[1], reverse=True)[:10]),
            "pending_dispatch": by_status.get("草稿", 0) + by_status.get("待处置", 0),
            "replayed_count": by_status.get("已重放", 0),
            "discarded_count": by_status.get("已丢弃", 0),
            "analysis": self.analyze_failure_reasons(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出死信队列管理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        analysis = self.analyze_failure_reasons()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信队列记录数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_replays": len(self._replay_history),
            "failure_categories": analysis.get("categories", {}),
            "operation": self.OPERATION_TERM,
        }