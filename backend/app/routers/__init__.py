"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import export_batch, resource

# export_batch 使用固定前缀 /api/export-batches，必须先于通用 /{module} 路由注册。
ROUTERS = [export_batch, resource]
