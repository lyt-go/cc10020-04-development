"""跨模块数据流管道API：暴露完整业务链路端点。

将领域服务的能力串联为可测试的数据流管道，每条管道对应
一个跨模块数据流链路。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/pipeline", tags=["跨模块数据流"])


class WebhookPayload(BaseModel):
    event_type: str = "default_event"
    data: dict[str, Any] = {}
    headers: dict[str, str] | None = None
    signature: str | None = None


class BatchExecuteRequest(BaseModel):
    task_id: int
    max_pages: int = 5


class DlqReplayRequest(BaseModel):
    dlq_ids: list[int]
    batch_id: str | None = None
    # 本轮哪些原始偏移能拿到实际消费回执；不传则由来源队列模拟决定。
    success_offsets: list[int] | None = None
    fail_offsets: list[int] | None = None
    deliver: bool = True


class DlqReceiptBackfillRequest(BaseModel):
    dlq_ids: list[int] | None = None
    batch_id: str | None = None


class DlqConsumptionReceiptRequest(BaseModel):
    source_queue: str
    offset: int
    receipt: str | dict[str, Any] | None = None
    consumer_group: str = ""
    consumer_id: int = 0
    success: bool = True
    batch_id: str | None = None
    note: str = ""


class DlqReplayContinueRequest(BaseModel):
    batch_id: str | None = None
    source_queue: str | None = None
    consumer_id: int = 0


class DlqRoundSettleRequest(BaseModel):
    batch_id: str


class BackfillExecuteRequest(BaseModel):
    job_id: int


class EndpointTestRequest(BaseModel):
    endpoint_id: int


class TransformRequest(BaseModel):
    rule_id: int
    source_data: list[dict[str, Any]]


class ValidationRequest(BaseModel):
    records: list[dict[str, Any]]
    source_module: str = ""


@router.post("/webhook-to-lake")
def pipeline_webhook_to_lake(channel_id: int, payload: WebhookPayload) -> dict[str, Any]:
    """链路1: Webhook接收 → 事件解析 → 去重 → 分发路由 → 数据落湖。

    完整覆盖：webhook_channel → event_schema → dedup_filter → dispatch_rule → data_lake_sink。
    """
    from app.services.webhook_channel import WebhookChannelService
    svc = WebhookChannelService()
    result = svc.receive_webhook(
        channel_id=channel_id,
        payload={"type": payload.event_type, **payload.data},
        signature=payload.signature,
        headers=payload.headers,
    )
    return result


@router.post("/batch-ingest-to-lake")
def pipeline_batch_ingest_to_lake(request: BatchExecuteRequest) -> dict[str, Any]:
    """链路2: 批量拉取 → 数据转换 → 质量校验 → 数据落湖。

    完整覆盖：batch_ingest → data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.batch_ingest import BatchIngestService
    svc = BatchIngestService()
    result = svc.execute_batch(task_id=request.task_id, max_pages=request.max_pages)
    return result


@router.post("/dlq-replay")
def pipeline_dlq_replay(request: DlqReplayRequest) -> dict[str, Any]:
    """链路3: 死信队列 → 分析 → 重放 → 消费确认 → 追踪。

    完整覆盖：dlq_handler → dlq_replay_ack → mq_consumer → call_trace_log。
    重放只登记确认明细，死信记录保持待处置，等来源队列回执后再结清。
    """
    from app.services.dlq_handler import DlqHandlerService
    svc = DlqHandlerService()

    if len(request.dlq_ids) == 1 and request.dlq_ids[0] == -1:
        # 全部待处置记录重放
        analysis = svc.analyze_failure_reasons()
        if analysis["total"] == 0:
            return {"ok": False, "message": "无死信记录", "replayed": 0}
        from app.persistence import json_store
        all_dlq = json_store.get_all("dlq_handler")
        pending_ids = [int(r["id"]) for r in all_dlq if r.get("status") in ("草稿", "待处置")]
        return svc.batch_replay(
            pending_ids,
            batch_id=request.batch_id,
            success_offsets=request.success_offsets,
            fail_offsets=request.fail_offsets,
            deliver=request.deliver,
        )

    return svc.batch_replay(
        request.dlq_ids,
        batch_id=request.batch_id,
        success_offsets=request.success_offsets,
        fail_offsets=request.fail_offsets,
        deliver=request.deliver,
    )


@router.post("/dlq-replay/backfill-receipts")
def pipeline_dlq_backfill_receipts(request: DlqReceiptBackfillRequest) -> dict[str, Any]:
    """从待处置死信数据回填重放回执，形成死信重放确认明细。

    重启或换实例后也可以直接调用：明细落 JSON 存储，原偏移随明细保留。
    """
    from app.services.dlq_replay_ack import DlqReplayAckService
    svc = DlqReplayAckService()
    return svc.backfill_replay_receipts(request.dlq_ids, request.batch_id)


@router.post("/dlq-replay/consumption-receipt")
def pipeline_dlq_consumption_receipt(request: DlqConsumptionReceiptRequest) -> dict[str, Any]:
    """登记来源队列的消费回执；同一（队列, 偏移）并发/重复登记只确认一份。"""
    from app.services.dlq_replay_ack import DlqReplayAckService
    svc = DlqReplayAckService()
    return svc.record_consumption_receipt(
        source_queue=request.source_queue,
        offset=request.offset,
        receipt=request.receipt,
        consumer_group=request.consumer_group,
        consumer_id=request.consumer_id,
        success=request.success,
        batch_id=request.batch_id,
        note=request.note,
    )


@router.post("/dlq-replay/continue")
def pipeline_dlq_replay_continue(request: DlqReplayContinueRequest) -> dict[str, Any]:
    """重启后从死信处置入口继续处理本轮剩余消息。

    只续投仍为「待消费」的偏移；已收到实际消费回执的偏移不会再产生一次处理。
    """
    from app.services.dlq_replay_ack import DlqReplayAckService
    svc = DlqReplayAckService()
    return svc.resume_round(
        batch_id=request.batch_id,
        source_queue=request.source_queue,
        consumer_id=request.consumer_id,
    )


@router.post("/dlq-replay/settle")
def pipeline_dlq_replay_settle(request: DlqRoundSettleRequest) -> dict[str, Any]:
    """本轮结清：已消费明细确认，并回写死信重放确认明细与处置台账。"""
    from app.services.dlq_replay_ack import DlqReplayAckService
    svc = DlqReplayAckService()
    return svc.settle_round(request.batch_id)


@router.get("/dlq-replay/receipts")
def pipeline_dlq_receipts(
    status: str | None = None,
    source_queue: str | None = None,
    offset: int | None = None,
    batch_id: str | None = None,
    page: int = 1,
    size: int = 100,
) -> dict[str, Any]:
    """重新打开处置明细：按状态/队列/原偏移/批次查询确认明细与消费凭证。"""
    from app.services.dlq_replay_ack import DlqReplayAckService
    svc = DlqReplayAckService()
    return svc.list_details(
        status=status,
        source_queue=source_queue,
        offset=offset,
        batch_id=batch_id,
        page=page,
        size=size,
    )


@router.get("/dlq-replay/rounds/{batch_id}")
def pipeline_dlq_round(batch_id: str) -> dict[str, Any]:
    """查看一轮重放的结清进度：待消费、等待本轮结清、已确认分别有多少。"""
    from app.services.dlq_replay_ack import DlqReplayAckService
    svc = DlqReplayAckService()
    return svc.get_round_summary(batch_id)


@router.post("/backfill-execute")
def pipeline_backfill_execute(request: BackfillExecuteRequest) -> dict[str, Any]:
    """链路4: 历史回补 → 水位查询 → 批量拉取 → 数据落湖。

    完整覆盖：backfill_job → incre_cursor → batch_ingest → data_lake_sink。
    """
    from app.services.backfill_job import BackfillJobService
    svc = BackfillJobService()
    result = svc.execute_backfill(job_id=request.job_id)
    return result


@router.post("/endpoint-test")
def pipeline_endpoint_test(request: EndpointTestRequest) -> dict[str, Any]:
    """链路5: 端点连接测试 → 认证验证 → 调用追踪。

    完整覆盖：api_endpoint → api_auth_store → call_trace_log。
    """
    from app.services.api_endpoint import ApiEndpointService
    svc = ApiEndpointService()
    result = svc.test_connection(endpoint_id=request.endpoint_id)
    return result


@router.post("/transform-validate")
def pipeline_transform_validate(request: TransformRequest) -> dict[str, Any]:
    """链路6: 数据转换 → 质量校验 → 结果追踪。

    完整覆盖：data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.data_transform import DataTransformService
    transform_svc = DataTransformService()
    batch_result = transform_svc.batch_transform(rule_id=request.rule_id, source_records=request.source_data)
    return batch_result


@router.post("/batch-validate")
def pipeline_validate_batch(request: ValidationRequest) -> dict[str, Any]:
    """链路7: 质量批量校验 → 统计报告。

    完整覆盖：data_quality_rule → integration_monitor。
    """
    from app.services.data_quality_rule import DataQualityRuleService
    svc = DataQualityRuleService()
    result = svc.validate_batch(records=request.records, source_module=request.source_module)
    return result


@router.get("/health-summary")
def pipeline_health_summary() -> dict[str, Any]:
    """链路8: 全局监控指标聚合 → 健康度评分。

    完整覆盖：integration_monitor → call_trace_log → dlq_handler → data_lake_sink。
    """
    from app.services.integration_monitor import IntegrationMonitorService
    svc = IntegrationMonitorService()
    return svc.collect_global_metrics()


@router.get("/dedup-stats")
def pipeline_dedup_stats() -> dict[str, Any]:
    """链路9: 去重统计查询。"""
    from app.services.dedup_filter import DedupFilterService
    svc = DedupFilterService()
    return svc.get_dedup_stats()


@router.post("/archive-execute")
def pipeline_archive_execute(policy_id: int) -> dict[str, Any]:
    """链路10: 归档策略执行 → 压缩 → 过期清理。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    return svc.execute_archive(policy_id=policy_id)