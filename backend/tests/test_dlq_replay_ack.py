"""死信重放确认链路测试：回执回填、偏移幂等、重启续投与本轮结清。

覆盖现场：
1. 从待处置死信回填重放回执，形成确认明细；
2. 消费队列未确认时死信保持待处置；
3. 服务重启后按原队列 + 原始偏移重查；
4. 并发重放同一来源队列 + 偏移只对外确认一份结果；
5. 同队列两条偏移一起重放，后一条已消费、前一条未消费，重启后续投；
6. 本轮结清后消费结论写回确认明细与处置台账。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import json_store
from app.services.dlq_handler import DlqHandlerService
from app.services.dlq_replay_ack import (
    STATUS_AWAITING,
    STATUS_CONFIRMED,
    STATUS_PENDING,
    DlqReplayAckService,
)
from app.services.mq_consumer import MqConsumerService


class AckTestBase(unittest.TestCase):
    def setUp(self):
        for key in ("dlq_handler", "dlq_replay_ack", "mq_consumer"):
            json_store.truncate(key)
        self.dlq = DlqHandlerService()
        self.ack = DlqReplayAckService()

    def enqueue(self, queue: str, offset: int, body: str | None = None) -> int:
        result = self.dlq.enqueue_dead_letter(
            source_queue=queue,
            original_message=body or json.dumps({"data": {"id": offset + 1}}),
            failure_reason="处理超时",
            offset=offset,
            consumer_group="order_group",
        )
        return int(result["dlq_id"])

    def ledger(self, dlq_id: int) -> dict:
        return self.dlq.get_entry(dlq_id) or {}

    def new_service_instance(self) -> DlqReplayAckService:
        """换一个服务实例，模拟进程重启后重新打开处置明细。"""
        return DlqReplayAckService()


class TestReceiptBackfill(AckTestBase):
    def test_backfill_from_pending_dead_letters(self):
        first = self.enqueue("order_queue", 10)
        second = self.enqueue("order_queue", 11)
        self.dlq.run_action(second, "标记丢弃")

        result = self.ack.backfill_replay_receipts(batch_id="R-BACKFILL")

        self.assertTrue(result["ok"])
        self.assertEqual(result["created"], 1)
        details = self.ack.list_details(batch_id="R-BACKFILL")["items"]
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["记录编号"], self.ledger(first)["记录编号"])
        self.assertEqual(details[0]["原始队列"], "order_queue")
        self.assertEqual(details[0]["原始偏移"], "10")
        self.assertEqual(details[0]["status"], STATUS_PENDING)
        self.assertEqual(details[0]["重放批次"], "R-BACKFILL")
        # 回执没回来之前，处置台账保持待处置。
        self.assertEqual(self.ledger(first)["status"], "待处置")
        self.assertEqual(self.ledger(first)["确认状态"], STATUS_PENDING)

    def test_backfill_is_idempotent_per_queue_and_offset(self):
        self.enqueue("order_queue", 10)
        first = self.ack.backfill_replay_receipts(batch_id="R-1")
        second = self.ack.backfill_replay_receipts(batch_id="R-2")

        self.assertEqual(first["created"], 1)
        self.assertEqual(second["created"], 0)
        self.assertEqual(second["existing"], 1)
        self.assertEqual(self.ack.list_details()["total"], 1)

    def test_offset_survives_when_message_body_has_no_offset(self):
        dlq_id = self.enqueue("order_queue", 0, body='{"data": {"payload": "no-offset"}}')
        result = self.ack.backfill_replay_receipts([dlq_id], "R-1")
        detail = result["details"][0]
        self.assertTrue(detail["ok"])
        self.assertEqual(self.ack.find_by_offset("order_queue", detail["offset"])["id"], detail["detail_id"])


class TestConsumptionReceipt(AckTestBase):
    def test_receipt_moves_to_awaiting_settlement(self):
        dlq_id = self.enqueue("order_queue", 10)
        self.ack.backfill_replay_receipts([dlq_id], "R-1")

        ack = self.ack.record_consumption_receipt("order_queue", 10, receipt="ACK-1", batch_id="R-1")

        self.assertTrue(ack["ok"])
        self.assertEqual(ack["status"], STATUS_AWAITING)
        self.assertEqual(ack["voucher"], "ACK-1")
        detail = self.new_service_instance().find_by_offset("order_queue", 10)
        self.assertEqual(detail["消费凭证"], "ACK-1")
        self.assertEqual(detail["原始偏移"], "10")
        self.assertEqual(self.ledger(dlq_id)["status"], "待处置")
        self.assertEqual(self.ledger(dlq_id)["确认状态"], STATUS_AWAITING)

    def test_duplicate_receipt_confirms_only_once(self):
        self.enqueue("order_queue", 10)
        self.ack.backfill_replay_receipts(None, "R-1")

        first = self.ack.record_consumption_receipt("order_queue", 10, receipt="ACK-1", batch_id="R-1")
        second = self.ack.record_consumption_receipt("order_queue", 10, receipt="ACK-2", batch_id="R-1")

        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertFalse(second["reprocessed"])
        self.assertEqual(second["voucher"], "ACK-1")
        self.assertEqual(self.ack.list_details()["total"], 1)

    def test_failed_receipt_keeps_pending(self):
        dlq_id = self.enqueue("order_queue", 10)
        self.ack.backfill_replay_receipts([dlq_id], "R-1")

        ack = self.ack.record_consumption_receipt(
            "order_queue", 10, success=False, batch_id="R-1", note="下游超时")

        self.assertEqual(ack["status"], STATUS_PENDING)
        self.assertEqual(self.ledger(dlq_id)["status"], "待处置")
        detail = self.new_service_instance().find_by_offset("order_queue", 10)
        self.assertEqual(detail["重试次数"], "1")
        self.assertEqual(detail["消费结论"], "消费失败")

    def test_concurrent_receipts_produce_single_result(self):
        self.enqueue("order_queue", 10)
        self.ack.backfill_replay_receipts(None, "R-1")

        results: list[dict] = []
        lock = threading.Lock()

        def worker(index: int) -> None:
            ack = DlqReplayAckService().record_consumption_receipt(
                "order_queue", 10, receipt=f"ACK-{index}", batch_id="R-1")
            with lock:
                results.append(ack)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 8)
        self.assertEqual(sum(1 for item in results if not item["duplicate"]), 1)
        vouchers = {item["voucher"] for item in results}
        self.assertEqual(len(vouchers), 1)
        self.assertEqual(self.ack.list_details()["total"], 1)

    def test_concurrent_replay_creates_one_detail(self):
        dlq_id = self.enqueue("order_queue", 10)
        ids: list[int] = []
        lock = threading.Lock()

        def worker() -> None:
            receipt = DlqReplayAckService().register_replay_receipt(
                dlq_id=dlq_id, source_queue="order_queue", offset=10, batch_id="R-1")
            with lock:
                ids.append(int(receipt["detail_id"]))

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(self.ack.list_details()["total"], 1)


class TestRestartRound(AckTestBase):
    def test_partial_round_after_restart_only_redelivers_pending(self):
        first = self.enqueue("order_queue", 10)
        second = self.enqueue("order_queue", 11)

        replay = self.dlq.batch_replay(
            [first, second], batch_id="R-ROUND", success_offsets=[11])
        self.assertEqual(replay["pending_consumption"], 1)
        self.assertEqual(replay["awaiting_settlement"], 1)
        self.assertEqual(self.ledger(first)["status"], "待处置")
        self.assertEqual(self.ledger(second)["status"], "待处置")

        # 值班员重启服务：新实例仍能重查原消息的关联偏移与消费凭证。
        restarted = self.new_service_instance()
        consumed = restarted.find_by_offset("order_queue", 11)
        waiting = restarted.find_by_offset("order_queue", 10)
        self.assertEqual(consumed["status"], STATUS_AWAITING)
        self.assertTrue(consumed["消费凭证"])
        self.assertEqual(waiting["status"], STATUS_PENDING)
        self.assertEqual(waiting["原始偏移"], "10")
        self.assertEqual(restarted.list_details(status=STATUS_PENDING)["total"], 1)
        self.assertEqual(restarted.list_details(status=STATUS_AWAITING)["total"], 1)

        # 从处置入口继续本轮：只有未消费的那条可投递。
        resumed = restarted.resume_round("R-ROUND")
        self.assertEqual(resumed["resumed"], 1)
        self.assertEqual([item["offset"] for item in resumed["deliverable"]], [10])

        # 再续投一次也不会把已消费的偏移重新投出去。
        again = restarted.resume_round("R-ROUND")
        self.assertEqual(again["resumed"], 1)
        self.assertEqual([item["offset"] for item in again["deliverable"]], [10])

        # 后一条已实际消费，任何重复回执都不会产生第二次处理。
        duplicate = self.dlq.batch_replay(
            [second], batch_id="R-ROUND", success_offsets=[11])
        self.assertEqual(duplicate["results"][0]["status"], STATUS_AWAITING)
        self.assertTrue(duplicate["results"][0]["duplicate"])

        # 前一条拿到回执后本轮结清，消费结论写回明细与台账。
        ack = restarted.record_consumption_receipt(
            "order_queue", 10, receipt="ACK-10", batch_id="R-ROUND")
        self.assertEqual(ack["status"], STATUS_AWAITING)
        settled = self.dlq.settle_replay_round("R-ROUND")
        self.assertTrue(settled["settled"])
        self.assertEqual(settled["confirmed"], 2)

        details = self.new_service_instance().list_details(batch_id="R-ROUND")["items"]
        self.assertTrue(all(row["status"] == STATUS_CONFIRMED for row in details))
        self.assertEqual({row["原始偏移"] for row in details}, {"10", "11"})
        self.assertTrue(all(row["消费凭证"] for row in details))
        self.assertEqual(self.ledger(first)["status"], "已重放")
        self.assertEqual(self.ledger(first)["重放回执"], "ACK-10")
        self.assertEqual(self.ledger(second)["status"], "已重放")
        self.assertTrue(self.ledger(second)["重放回执"])

    def test_settle_waits_for_unconsumed_messages(self):
        dlq_id = self.enqueue("order_queue", 10)
        self.dlq.batch_replay([dlq_id], batch_id="R-WAIT", deliver=False)

        settled = self.ack.settle_round("R-WAIT")
        self.assertFalse(settled["settled"])
        self.assertEqual(settled["pending"], 1)
        self.assertEqual(self.ledger(dlq_id)["status"], "待处置")
        self.assertEqual(self.ledger(dlq_id)["确认状态"], STATUS_PENDING)


class TestConsumerRoundTrip(AckTestBase):
    def test_consumer_receipt_writes_back_to_receipt_and_ledger(self):
        json_store.insert("mq_consumer", {
            "配置编号": "MQ-ACK-001",
            "队列名称": "order_queue",
            "消费组名": "order_group",
            "偏移策略": "earliest",
            "批量大小": "5",
            "超时时间": "5000",
            "死信队列": "order_dlq",
            "消费状态": "消费中",
            "status": "消费中",
            "pending": False,
            "abnormal": False,
        })
        consumer_id = json_store.get_all("mq_consumer")[0]["id"]

        first = self.enqueue("order_queue", 10)
        second = self.enqueue("order_queue", 11)
        self.dlq.batch_replay([first, second], batch_id="R-CONSUME", deliver=False)

        consumer = MqConsumerService()

        from unittest import mock
        real_receipt = DlqReplayAckService.record_consumption_receipt

        def always_success(service, source_queue, offset, **kwargs):
            kwargs["success"] = True
            kwargs["receipt"] = kwargs.get("note")
            return real_receipt(service, source_queue, offset, **kwargs)

        with mock.patch.object(
                DlqReplayAckService, "record_consumption_receipt", autospec=True,
                side_effect=always_success), \
             mock.patch("random.random", return_value=0.1):
            consumed = consumer.consume_replay(consumer_id, batch_id="R-CONSUME")

        self.assertEqual(consumed["consumed"], 2)
        details = self.ack.list_details(batch_id="R-CONSUME")["items"]
        self.assertTrue(all(row["status"] == STATUS_AWAITING for row in details))
        self.assertTrue(all(row["消费凭证"] for row in details))

        # 消费一次后再续投，没有剩余待消费消息。
        resumed = self.ack.resume_round("R-CONSUME")
        self.assertEqual(resumed["resumed"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
